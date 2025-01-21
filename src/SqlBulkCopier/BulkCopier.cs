using System.Text;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier;

/// <summary>
/// Provides functionality for bulk copying data into SQL Server tables.
/// Supports retries, transaction management, and table truncation.
/// </summary>
public class BulkCopier : IBulkCopier
{
    /// <summary>
    /// Event that is raised when a specified number of rows have been processed during the bulk copy operation.
    /// </summary>
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

    /// <summary>
    /// Gets the name of the destination table for the bulk insert operation.
    /// </summary>
    public string DestinationTableName => _sqlBulkCopy.DestinationTableName;

    /// <summary>
    /// Gets or sets the maximum number of retry attempts for the bulk copy operation.
    /// </summary>
    public int MaxRetryCount { get; set; }

    /// <summary>
    /// Gets or sets whether to truncate the destination table before performing the bulk insert.
    /// </summary>
    public bool TruncateBeforeBulkInsert { get; set; }

    /// <summary>
    /// Gets or sets whether to use exponential backoff for retry delays.
    /// When enabled, the delay time between retries doubles with each attempt.
    /// </summary>
    public bool UseExponentialBackoff { get; set; }

    /// <summary>
    /// Gets or sets the initial delay time between retry attempts.
    /// </summary>
    public TimeSpan InitialDelay { get; set; }

    /// <summary>
    /// Gets or sets the number of rows in each batch of the bulk copy operation.
    /// Maps to SqlBulkCopy.BatchSize.
    /// </summary>
    public int BatchSize
    {
        get => _sqlBulkCopy.BatchSize;
        set => _sqlBulkCopy.BatchSize = value;
    }

    /// <summary>
    /// Gets or sets the number of rows to process before raising the SqlRowsCopied event.
    /// Maps to SqlBulkCopy.NotifyAfter.
    /// </summary>
    public int NotifyAfter
    {
        get => _sqlBulkCopy.NotifyAfter;
        set => _sqlBulkCopy.NotifyAfter = value;
    }

    /// <summary>
    /// Gets the number of rows copied in the current operation.
    /// Maps to SqlBulkCopy.RowsCopied.
    /// </summary>
    public int RowsCopied => _sqlBulkCopy.RowsCopied;

    /// <summary>
    /// Gets the number of rows copied in the current operation as a 64-bit integer.
    /// Maps to SqlBulkCopy.RowsCopied64.
    /// </summary>
    public long RowsCopied64 => _sqlBulkCopy.RowsCopied64;

    /// <summary>
    /// Performs the bulk copy operation asynchronously.
    /// </summary>
    /// <param name="stream">The stream containing the data to be copied.</param>
    /// <param name="encoding">The encoding to use when reading the stream.</param>
    /// <param name="timeout">The timeout period for the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when:
    /// - Retries are enabled with an external transaction
    /// - Retries are enabled with an external connection
    /// - Retries are enabled without table truncation
    /// - The maximum retry count is exceeded
    /// </exception>
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