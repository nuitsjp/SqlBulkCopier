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
    private readonly BulkCopierOptions _bulkCopierOptions;

    public BulkCopier(
        string destinationTableName,
        IDataReaderBuilder dataReaderBuilder,
        SqlConnection connection, 
        BulkCopierOptions bulkCopierOptions)
        : this(destinationTableName, new SqlBulkCopy(connection), bulkCopierOptions, dataReaderBuilder)
    {
        _connectionString = null;
        _connection = connection;
        _externalTransaction = null;
        _copyOptions = SqlBulkCopyOptions.Default;
    }

    public BulkCopier(
        string destinationTableName,
        IDataReaderBuilder dataReaderBuilder,
        string connectionString, 
        BulkCopierOptions bulkCopierOptions)
        : this(destinationTableName, new SqlBulkCopy(connectionString), bulkCopierOptions, dataReaderBuilder)
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
        SqlBulkCopyOptions copyOptions, 
        BulkCopierOptions bulkCopierOptions)
        : this(destinationTableName, new SqlBulkCopy(connectionString, copyOptions), bulkCopierOptions, dataReaderBuilder)
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
        BulkCopierOptions bulkCopierOptions, 
        SqlTransaction externalTransaction)
        : this(destinationTableName, new SqlBulkCopy(connection, copyOptions, externalTransaction), bulkCopierOptions, dataReaderBuilder)
    {
        _connectionString = null;
        _connection = connection;
        _externalTransaction = externalTransaction;
        _copyOptions = copyOptions;
    }

    private BulkCopier(
        string destinationTableName,
        SqlBulkCopy sqlBulkCopy,
        BulkCopierOptions bulkCopierOptions, 
        IDataReaderBuilder dataReaderBuilder)
    {
        _sqlBulkCopy = sqlBulkCopy;
        _sqlBulkCopy.DestinationTableName = destinationTableName;
        DataReaderBuilder = dataReaderBuilder;
        _bulkCopierOptions = bulkCopierOptions;
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
                // When truncate before bulk insert is enabled, truncate the table
                if (TruncateBeforeBulkInsert)
                {
                    await TruncateTableAsync();
                }

                await _sqlBulkCopy.WriteToServerAsync(DataReaderBuilder.Build(stream, encoding));

                // Exit the loop when the process is successful
                break;
            }
            catch (Exception ex)
            {
                currentRetryCount++;
                if (currentRetryCount > MaxRetryCount)
                {
                    // When the retry count exceeds the maximum, throw an exception
                    throw new Exception($"BulkCopier failed after {currentRetryCount - 1} retries.", ex);
                }

                // When use exponential backoff, double the delay time
                if (UseExponentialBackoff && currentRetryCount > 1)
                {
                    delay = TimeSpan.FromMilliseconds(delay.TotalMilliseconds * 2);
                }

                // Wait for the delay
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
            // External transaction is set, execute in that
#if NET8_0_OR_GREATER
            await using var command = new SqlCommand(query, _connection, _externalTransaction);
#else
            var command = new SqlCommand(query, _connection, _externalTransaction);
#endif
            await command.ExecuteNonQueryAsync();
        }
        else if (_connection is not null)
        {
            // External connection is set, execute in that
#if NET8_0_OR_GREATER
            await using var command = new SqlCommand(query, _connection);
#else
            var command = new SqlCommand(query, _connection);
#endif
            await command.ExecuteNonQueryAsync();
        }
        else
        {
            // Otherwise, create a new connection and execute
#if NET8_0_OR_GREATER
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            await using var command = new SqlCommand(query, connection);
#else
            var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            using var command = new SqlCommand(query, connection);
#endif
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