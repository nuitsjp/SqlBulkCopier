using System.Text;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier;

/// <summary>
/// Bulk copier.
/// </summary>
public class BulkCopier : IBulkCopier
{
    /// <summary>
    /// Event when rows copied.
    /// </summary>
    public event SqlRowsCopiedEventHandler? SqlRowsCopied;

    /// <summary>
    /// SQL bulk copy.
    /// </summary>
    private readonly SqlBulkCopy _sqlBulkCopy;

    /// <summary>
    /// Connection string.
    /// </summary>
    private readonly string? _connectionString;

    /// <summary>
    /// Connection.
    /// </summary>
    private readonly SqlConnection? _connection;

    /// <summary>
    /// External transaction.
    /// </summary>
    private readonly SqlTransaction? _externalTransaction;

    /// <summary>
    /// Constructor.
    /// </summary>
    public BulkCopier(
        string destinationTableName,
        IDataReaderBuilder dataReaderBuilder,
        SqlConnection connection)
        : this(destinationTableName, new SqlBulkCopy(connection), dataReaderBuilder)
    {
        _connectionString = null;
        _connection = connection;
        _externalTransaction = null;
    }

    /// <summary>
    /// Constructor.
    /// </summary>
    public BulkCopier(
        string destinationTableName,
        IDataReaderBuilder dataReaderBuilder,
        string connectionString)
        : this(destinationTableName, new SqlBulkCopy(connectionString), dataReaderBuilder)
    {
        _connectionString = connectionString;
        _connection = null;
        _externalTransaction = null;
    }

    /// <summary>
    /// Constructor.
    /// </summary>
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
    }

    /// <summary>
    /// Constructor.
    /// </summary>
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
    }

    /// <summary>
    /// Constructor.
    /// </summary>
    private BulkCopier(
        string destinationTableName,
        SqlBulkCopy sqlBulkCopy,
        IDataReaderBuilder dataReaderBuilder)
    {
        _sqlBulkCopy = sqlBulkCopy;
        _sqlBulkCopy.DestinationTableName = destinationTableName;
        DataReaderBuilder = dataReaderBuilder;
        DataReaderBuilder.SetupColumnMappings(_sqlBulkCopy);

        _sqlBulkCopy.SqlRowsCopied += SqlBulkCopyOnSqlRowsCopied;
    }

    /// <summary>
    /// IDataReaderBuilder
    /// </summary>
    public IDataReaderBuilder DataReaderBuilder { get; init; }

    /// <summary>
    /// Destination table name of bulk insert.
    /// </summary>
    public string DestinationTableName => _sqlBulkCopy.DestinationTableName;

    /// <summary>
    /// Maximum retry count.
    /// </summary>
    public int MaxRetryCount { get; set; }

    /// <summary>
    /// Truncate before bulk insert.
    /// </summary>
    public bool TruncateBeforeBulkInsert { get; set; }

    /// <summary>
    /// Use exponential backoff. Retry delay time is doubled.
    /// </summary>
    public bool UseExponentialBackoff { get; set; }

    /// <summary>
    /// Retry initial delay time.
    /// </summary>
    public TimeSpan InitialDelay { get; set; }

    /// <summary>
    /// Number of rows in each batch. Refer to SqlBulkCopy.BatchSize.
    /// </summary>
    public int BatchSize
    {
        get => _sqlBulkCopy.BatchSize;
        set => _sqlBulkCopy.BatchSize = value;
    }

    /// <summary>
    /// Number of rows in each batch. Refer to SqlBulkCopy.NotifyAfter.
    /// </summary>
    public int NotifyAfter
    {
        get => _sqlBulkCopy.NotifyAfter;
        set => _sqlBulkCopy.NotifyAfter = value;
    }

    /// <summary>
    /// Number of rows copied. Refer to SqlBulkCopy.RowsCopied.
    /// </summary>
    public int RowsCopied => _sqlBulkCopy.RowsCopied;

    /// <summary>
    /// Number of rows copied. Refer to SqlBulkCopy.RowsCopied64.
    /// </summary>
    public long RowsCopied64 => _sqlBulkCopy.RowsCopied64;

    /// <summary>
    /// Write to server asynchronously.
    /// </summary>
    /// <param name="stream"></param>
    /// <param name="encoding"></param>
    /// <param name="timeout"></param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    /// <exception cref="Exception"></exception>
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
        if (0 < MaxRetryCount && TruncateBeforeBulkInsert is false)
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
                    throw new InvalidOperationException($"BulkCopier failed after {currentRetryCount - 1} retries.", ex);
                }

                // When use exponential backoff, double the delay time
                if (UseExponentialBackoff && currentRetryCount > 1)
                {
                    delay = TimeSpan.FromMilliseconds(delay.TotalMilliseconds * 2);
                }

                // Wait for the delay
                await Task.Delay(delay);

                // Reset the stream position
                stream.Position = 0;
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