using System.Text;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier;

public class BulkCopier : IBulkCopier
{
    public event SqlRowsCopiedEventHandler? SqlRowsCopied;

    private readonly SqlBulkCopy _sqlBulkCopy;

    private readonly string? _connectionString;
    private readonly SqlConnection? _connection;
    private readonly SqlTransaction? _externalTransaction;
    private readonly SqlBulkCopyOptions _copyOptions;

    public BulkCopier(
        string destinationTableName,
        IDataReaderBuilder dataReaderBuilder,
        SqlConnection connection)
        : this(destinationTableName, new SqlBulkCopy(connection), dataReaderBuilder)
    {
        _connectionString = null;
        _connection = connection;
        _externalTransaction = null;
        _copyOptions = SqlBulkCopyOptions.Default;
    }

    public BulkCopier(
        string destinationTableName,
        IDataReaderBuilder dataReaderBuilder,
        string connectionString)
        : this(destinationTableName, new SqlBulkCopy(connectionString), dataReaderBuilder)
    {
        _connectionString = connectionString;
        _connection = null;
        _externalTransaction = null;
        _copyOptions = SqlBulkCopyOptions.Default;
    }

    public BulkCopier(
        string destinationTableName,
        IDataReaderBuilder dataReaderBuilder, 
        string connectionString, 
        SqlBulkCopyOptions copyOptions)
        : this(destinationTableName, new SqlBulkCopy(connectionString, copyOptions), dataReaderBuilder)
    {
        _connectionString = connectionString;
        _connection = null;
        _externalTransaction = null;
        _copyOptions = copyOptions;
    }

    public BulkCopier(
        string destinationTableName,
        IDataReaderBuilder dataReaderBuilder, 
        SqlConnection connection, 
        SqlBulkCopyOptions copyOptions, 
        SqlTransaction externalTransaction)
        : this(destinationTableName, new SqlBulkCopy(connection, copyOptions, externalTransaction), dataReaderBuilder)
    {
        _connectionString = null;
        _connection = connection;
        _externalTransaction = externalTransaction;
        _copyOptions = copyOptions;
    }

    private BulkCopier(
        string destinationTableName,
        SqlBulkCopy sqlBulkCopy, IDataReaderBuilder dataReaderBuilder)
    {
        _sqlBulkCopy = sqlBulkCopy;
        _sqlBulkCopy.DestinationTableName = destinationTableName;
        DataReaderBuilder = dataReaderBuilder;
        DataReaderBuilder.SetupColumnMappings(_sqlBulkCopy);

        _sqlBulkCopy.SqlRowsCopied += SqlBulkCopyOnSqlRowsCopied;
    }

    public IDataReaderBuilder DataReaderBuilder { get; init; }

    public int MaxRetryCount { get; set; } = 0;
    public TimeSpan InitialDelay { get; set; } = TimeSpan.FromSeconds(2);
    public bool TruncateBeforeBulkInsert { get; set; } = false;
    public bool UseExponentialBackoff { get; set; } = true;

    public int BatchSize
    {
        get => _sqlBulkCopy.BatchSize;
        set => _sqlBulkCopy.BatchSize = value;
    }

    public string DestinationTableName => _sqlBulkCopy.DestinationTableName;

    public int NotifyAfter
    {
        get => _sqlBulkCopy.NotifyAfter;
        set => _sqlBulkCopy.NotifyAfter = value;
    }

    public int RowsCopied => _sqlBulkCopy.RowsCopied;
    public long RowsCopied64 => _sqlBulkCopy.RowsCopied64;

    public async Task WriteToServerAsync(Stream stream, Encoding encoding, TimeSpan timeout)
    {
        _sqlBulkCopy.BulkCopyTimeout = (int)timeout.TotalSeconds;

        // 外部トランザクションが設定されている場合、バルクインサート関連だけリトライしても適切な結果にならないため例外をスロー
        if (_externalTransaction is not null && 0 < MaxRetryCount)
        {
            throw new InvalidOperationException("Cannot retry with an external transaction.");
        }

        // 外部コネクションが設定されている場合、TransactionScopeと併用されている場合などに、バルクインサート関連だけリトライしても適切な結果にならないため例外をスロー
        if (_connection is not null && 0 < MaxRetryCount)
        {
            throw new InvalidOperationException("Cannot retry with an external connection.");
        }

        // リトライが設定されている場合、テーブルのトランケートが無効だと、リトライ時にデータが重複してしまうため例外をスロー
        if (0 < MaxRetryCount && !TruncateBeforeBulkInsert)
        {
            throw new InvalidOperationException("Cannot retry without truncating the table.");
        }

        var currentRetryCount = 0;
        var delay = InitialDelay;
        while (true)
        {
            try
            {
                if (TruncateBeforeBulkInsert)
                {
                    // テーブルをトランケートする処理
                    await TruncateTableAsync();
                }

                await _sqlBulkCopy.WriteToServerAsync(DataReaderBuilder.Build(stream, encoding));
                break; // 成功したらループを抜ける
            }
            catch (Exception ex)
            {
                currentRetryCount++;
                if (currentRetryCount > MaxRetryCount)
                {
                    // オプションに基づき最大回数を超えたら失敗扱い
                    throw new Exception($"BulkCopier failed after {currentRetryCount - 1} retries.", ex);
                }

                // 指数バックオフなら待機時間を増やす
                if (UseExponentialBackoff && currentRetryCount > 1)
                {
                    delay = TimeSpan.FromMilliseconds(delay.TotalMilliseconds * 2);
                }

                // リトライ前に待機
                await Task.Delay(delay);
            }
        }
    }

    /// <summary>
    /// Truncate table before bulk insert.
    /// </summary>
    /// <returns></returns>
    private async Task TruncateTableAsync()
    {
        var query = $"TRUNCATE TABLE {_sqlBulkCopy.DestinationTableName}";
        if (_externalTransaction is not null)
        {
            // 外部トランザクションが設定されている場合、そのトランザクション内で実行
            await using var command = new SqlCommand(query, _connection, _externalTransaction);
            await command.ExecuteNonQueryAsync();
        }
        else if (_connection is not null)
        {
            // 外部コネクションが設定されている場合、そのコネクション内で実行
            await using var command = new SqlCommand(query, _connection);
            await command.ExecuteNonQueryAsync();
        }
        else
        {
            // それ以外の場合、新規コネクションを作成して実行
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            await using var command = new SqlCommand(query, connection);
            await command.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// Dispose the object.
    /// </summary>
    void IDisposable.Dispose()
    {
        _sqlBulkCopy.SqlRowsCopied -= SqlBulkCopyOnSqlRowsCopied;
        ((IDisposable)_sqlBulkCopy).Dispose();
    }

    /// <summary>
    /// Notify the event when rows copied.
    /// </summary>
    /// <param name="sender"></param>
    /// <param name="e"></param>
    private void SqlBulkCopyOnSqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
        => SqlRowsCopied?.Invoke(sender, e);

}