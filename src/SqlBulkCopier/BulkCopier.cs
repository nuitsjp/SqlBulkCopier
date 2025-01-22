using System.Text;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier;

/// <inheritdoc />
public class BulkCopier : IBulkCopier
{
    /// <inheritdoc />
    public event SqlRowsCopiedEventHandler? SqlRowsCopied;

    /// <summary>
    /// The underlying SqlBulkCopy instance that performs the actual bulk copy operations.
    /// </summary>
    private readonly SqlBulkCopy _sqlBulkCopy;

    /// <summary>
    /// The connection string used to connect to the database when no external connection is provided.
    /// </summary>
    private readonly string? _connectionString;

    /// <summary>
    /// The external SQL connection, if provided.
    /// </summary>
    private readonly SqlConnection? _connection;

    /// <summary>
    /// The external transaction, if provided.
    /// </summary>
    private readonly SqlTransaction? _externalTransaction;

    /// <summary>
    /// Initializes a new instance of the BulkCopier class using an existing SQL connection.
    /// </summary>
    /// <param name="destinationTableName">The name of the destination table.</param>
    /// <param name="dataReaderBuilder">The data reader builder that will construct the data reader for the bulk copy operation.</param>
    /// <param name="connection">The SQL connection to use.</param>
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
    /// Initializes a new instance of the BulkCopier class using a connection string.
    /// </summary>
    /// <param name="destinationTableName">The name of the destination table.</param>
    /// <param name="dataReaderBuilder">The data reader builder that will construct the data reader for the bulk copy operation.</param>
    /// <param name="connectionString">The connection string to use.</param>
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
    /// Initializes a new instance of the BulkCopier class using a connection string and bulk copy options.
    /// </summary>
    /// <param name="destinationTableName">The name of the destination table.</param>
    /// <param name="dataReaderBuilder">The data reader builder that will construct the data reader for the bulk copy operation.</param>
    /// <param name="connectionString">The connection string to use.</param>
    /// <param name="copyOptions">The SqlBulkCopyOptions to use.</param>
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
    /// Initializes a new instance of the BulkCopier class using an existing SQL connection, bulk copy options, and transaction.
    /// </summary>
    /// <param name="destinationTableName">The name of the destination table.</param>
    /// <param name="dataReaderBuilder">The data reader builder that will construct the data reader for the bulk copy operation.</param>
    /// <param name="connection">The SQL connection to use.</param>
    /// <param name="copyOptions">The SqlBulkCopyOptions to use.</param>
    /// <param name="externalTransaction">The transaction to use.</param>
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
    /// Private constructor that initializes the common components of the BulkCopier.
    /// </summary>
    /// <param name="destinationTableName">The name of the destination table.</param>
    /// <param name="sqlBulkCopy">The SqlBulkCopy instance to use.</param>
    /// <param name="dataReaderBuilder">The data reader builder that will construct the data reader for the bulk copy operation.</param>
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
    /// Gets the data reader builder used to construct the data reader for the bulk copy operation.
    /// </summary>
    public IDataReaderBuilder DataReaderBuilder { get; init; }

    /// <inheritdoc />
    public string DestinationTableName => _sqlBulkCopy.DestinationTableName;

    /// <inheritdoc />
    public int MaxRetryCount { get; set; }

    /// <inheritdoc />
    public bool TruncateBeforeBulkInsert { get; set; }

    /// <inheritdoc />
    public bool UseExponentialBackoff { get; set; }

    /// <inheritdoc />
    public TimeSpan InitialDelay { get; set; }

    /// <inheritdoc />
    public int BatchSize
    {
        get => _sqlBulkCopy.BatchSize;
        set => _sqlBulkCopy.BatchSize = value;
    }

    /// <inheritdoc />
    public int NotifyAfter
    {
        get => _sqlBulkCopy.NotifyAfter;
        set => _sqlBulkCopy.NotifyAfter = value;
    }

    /// <inheritdoc />
    public int RowsCopied => _sqlBulkCopy.RowsCopied;

    /// <inheritdoc />
    public long RowsCopied64 => _sqlBulkCopy.RowsCopied64;

    /// <inheritdoc />
    public async Task WriteToServerAsync(Stream stream, Encoding encoding, TimeSpan timeout)
    {
        _sqlBulkCopy.BulkCopyTimeout = (int)timeout.TotalSeconds;

        // Throw exception if retries are configured with an external transaction
        // as retrying bulk insert operations alone may not produce correct results
        if (_externalTransaction is not null && 0 < MaxRetryCount)
        {
            throw new InvalidOperationException("Cannot retry with an external transaction.");
        }

        // Throw exception if retries are configured with an external connection
        // as this might interfere with TransactionScope or other transaction management
        if (_connection is not null && 0 < MaxRetryCount)
        {
            throw new InvalidOperationException("Cannot retry with an external connection.");
        }

        // Throw exception if retries are enabled without table truncation
        // as this would result in duplicate data during retry attempts
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
                // Truncate the destination table if enabled
                if (TruncateBeforeBulkInsert)
                {
                    await TruncateTableAsync();
                }

                await _sqlBulkCopy.WriteToServerAsync(DataReaderBuilder.Build(stream, encoding));

                // Exit the retry loop on successful completion
                break;
            }
            catch (Exception ex)
            {
                currentRetryCount++;
                if (currentRetryCount > MaxRetryCount)
                {
                    throw new InvalidOperationException($"BulkCopier failed after {currentRetryCount - 1} retries.", ex);
                }

                // Apply exponential backoff if enabled and not the first retry
                if (UseExponentialBackoff && currentRetryCount > 1)
                {
                    delay = TimeSpan.FromMilliseconds(delay.TotalMilliseconds * 2);
                }

                // Wait before the next retry attempt
                await Task.Delay(delay);

                // Reset the stream position for the next attempt
                stream.Position = 0;
            }
        }
    }

    /// <summary>
    /// Truncates the destination table asynchronously.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private async Task TruncateTableAsync()
    {
        var query = $"TRUNCATE TABLE {_sqlBulkCopy.DestinationTableName}";
        if (_externalTransaction is not null)
        {
            // Execute within the provided external transaction
#if NET8_0_OR_GREATER
            await using var command = new SqlCommand(query, _connection, _externalTransaction);
#else
            var command = new SqlCommand(query, _connection, _externalTransaction);
#endif
            await command.ExecuteNonQueryAsync();
        }
        else if (_connection is not null)
        {
            // Execute using the provided external connection
#if NET8_0_OR_GREATER
            await using var command = new SqlCommand(query, _connection);
#else
            var command = new SqlCommand(query, _connection);
#endif
            await command.ExecuteNonQueryAsync();
        }
        else
        {
            // Create and use a new connection
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
    /// Releases the resources used by the BulkCopier instance.
    /// </summary>
    void IDisposable.Dispose()
    {
        _sqlBulkCopy.SqlRowsCopied -= SqlBulkCopyOnSqlRowsCopied;
        ((IDisposable)_sqlBulkCopy).Dispose();
    }

    /// <summary>
    /// Handles the SqlRowsCopied event from the underlying SqlBulkCopy instance
    /// and raises the corresponding event on this instance.
    /// </summary>
    /// <param name="sender">The source of the event.</param>
    /// <param name="e">The event arguments.</param>
    private void SqlBulkCopyOnSqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
        => SqlRowsCopied?.Invoke(sender, e);
}