using System.Text;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier;

/// <summary>
/// Implements the bulk copy operations for SQL Server, providing efficient data transfer capabilities.
/// This class encapsulates SqlBulkCopy functionality with additional features such as retry logic,
/// progress tracking, and transaction support.
/// </summary>
/// <remarks>
/// This implementation supports various initialization scenarios including:
/// - Using an existing SQL connection
/// - Using a connection string
/// - Using bulk copy options
/// - Using an external transaction
/// The class manages resources properly and implements IDisposable pattern.
/// </remarks>
public class BulkCopier : IBulkCopier
{
    /// <inheritdoc />
    public event SqlRowsCopiedEventHandler? SqlRowsCopied;

    /// <summary>
    /// The underlying SqlBulkCopy instance that performs the actual bulk copy operations.
    /// This instance is managed internally and disposed when the BulkCopier is disposed.
    /// </summary>
    private readonly SqlBulkCopy _sqlBulkCopy;

    /// <summary>
    /// The connection string used to connect to the database when no external connection is provided.
    /// This is stored to manage connection lifecycle when using connection string-based initialization.
    /// </summary>
    private readonly string? _connectionString;

    /// <summary>
    /// The external SQL connection, if provided.
    /// When using an external connection, the lifecycle is managed by the caller.
    /// </summary>
    private readonly SqlConnection? _connection;

    /// <summary>
    /// The external transaction, if provided.
    /// When using an external transaction, the transaction scope is managed by the caller.
    /// </summary>
    private readonly SqlTransaction? _externalTransaction;

    /// <summary>
    /// Initializes a new instance of the BulkCopier class using an existing SQL connection.
    /// </summary>
    /// <param name="destinationTableName">The name of the destination table where the data will be copied.</param>
    /// <param name="dataReaderBuilder">The data reader builder that will construct the data reader for the bulk copy operation.</param>
    /// <param name="connection">The SQL connection to use. The connection must be opened before calling WriteToServerAsync.</param>
    /// <exception cref="ArgumentNullException">Thrown when any parameter is null.</exception>
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
    /// The builder is responsible for creating a data reader that matches the destination table schema.
    /// </summary>
    public IDataReaderBuilder DataReaderBuilder { get; init; }

    /// <inheritdoc />
    public string DestinationTableName => _sqlBulkCopy.DestinationTableName;

    /// <inheritdoc />
    public int MaxRetryCount { get; set; }

    /// <inheritdoc />
    public bool TruncateBeforeBulkInsert { get; set; }

    /// <inheritdoc />
    public TruncateMethod TruncateMethod { get; set; } = TruncateMethod.Truncate;

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

    /// <summary>
    /// Asynchronously writes data from the specified stream to the SQL Server table.
    /// Supports automatic retries, table truncation, and progress notification through events.
    /// </summary>
    /// <param name="stream">The stream containing the data to be copied. The stream must be readable and seekable.</param>
    /// <param name="encoding">The character encoding to use when reading the stream. Must match the encoding of the data in the stream.</param>
    /// <param name="timeout">The time to wait for each batch to complete before generating an error. Use TimeSpan.Zero for no timeout.</param>
    /// <returns>A task that represents the asynchronous bulk copy operation.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when:
    /// - Retries are configured with an external transaction
    /// - Retries are configured with an external connection
    /// - Retries are enabled without table truncation
    /// - The maximum retry count is exceeded during retry attempts
    /// </exception>
    /// <exception cref="SqlException">Thrown when a SQL Server error occurs during the bulk copy operation.</exception>
    /// <exception cref="ArgumentNullException">Thrown when stream or encoding is null.</exception>
    /// <remarks>
    /// This method implements a retry mechanism with optional exponential backoff.
    /// When retries are enabled:
    /// - The stream position is reset to the beginning for each retry attempt
    /// - The destination table is truncated before each attempt if TruncateBeforeBulkInsert is true
    /// - The delay between retries increases exponentially if UseExponentialBackoff is true
    /// </remarks>
    public async Task WriteToServerAsync(Stream stream, Encoding encoding, TimeSpan timeout)
    {
        var timeoutSeconds = (int)timeout.TotalSeconds;
        _sqlBulkCopy.BulkCopyTimeout = timeoutSeconds;

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
                    await TruncateTableAsync(timeoutSeconds);
                }

                await _sqlBulkCopy.WriteToServerAsync(DataReaderBuilder.Build(stream, encoding));

                // Exit the retry loop on successful completion
                break;
            }
            catch (ArgumentException)
            {
                throw;
            }
            catch (InvalidOperationException)
            {
                throw;
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
    /// This operation is performed within the same transaction context as the bulk copy operation if available.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="SqlException">Thrown when the TRUNCATE TABLE operation fails.</exception>
    private async Task TruncateTableAsync(int timeoutSeconds)
    {
        var query = TruncateMethod switch
        {
            TruncateMethod.Truncate => $"TRUNCATE TABLE {_sqlBulkCopy.DestinationTableName}",
            TruncateMethod.Delete => $"DELETE FROM {_sqlBulkCopy.DestinationTableName}",
            _ => throw new ArgumentOutOfRangeException(nameof(TruncateMethod), TruncateMethod, null)
        };
        if (_externalTransaction is not null)
        {
            // Execute within the provided external transaction
#if NET8_0_OR_GREATER
            await using var command = new SqlCommand(query, _connection, _externalTransaction);
#else
            var command = new SqlCommand(query, _connection, _externalTransaction);
#endif
            command.CommandTimeout = timeoutSeconds;
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
            command.CommandTimeout = timeoutSeconds;
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
            command.CommandTimeout = timeoutSeconds;
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
    /// Event handler for the SqlBulkCopy.SqlRowsCopied event.
    /// Propagates the event to subscribers of this class's SqlRowsCopied event.
    /// </summary>
    /// <param name="sender">The source of the event.</param>
    /// <param name="args">The event arguments containing the number of rows copied.</param>
    private void SqlBulkCopyOnSqlRowsCopied(object sender, SqlRowsCopiedEventArgs args)
    {
        SqlRowsCopied?.Invoke(this, args);
    }
}
