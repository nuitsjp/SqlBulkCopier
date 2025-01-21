using System.Text;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier;

/// <summary>
/// Defines the interface for bulk copy operations to SQL Server.
/// Provides functionality for efficiently inserting large amounts of data into SQL Server tables
/// with support for batching, retry logic, and progress notification.
/// </summary>
public interface IBulkCopier : IDisposable
{
    /// <summary>
    /// Event that fires when a specified number of rows have been copied to the server.
    /// The number of rows is determined by the NotifyAfter property.
    /// </summary>
    event SqlRowsCopiedEventHandler SqlRowsCopied;

    /// <summary>
    /// Gets or sets the number of rows in each batch.
    /// A batch is sent to the server when it reaches this size.
    /// </summary>
    int BatchSize { get; set; }

    /// <summary>
    /// Gets the name of the destination table for the bulk copy operation.
    /// </summary>
    string DestinationTableName { get; }

    /// <summary>
    /// Gets or sets the number of rows to process before generating a notification event.
    /// Set this to receive progress updates during the bulk copy operation.
    /// </summary>
    int NotifyAfter { get; set; }

    /// <summary>
    /// Gets the number of rows copied to the server in the current operation.
    /// Returns a 32-bit integer value.
    /// </summary>
    int RowsCopied { get; }

    /// <summary>
    /// Gets the number of rows copied to the server in the current operation.
    /// Returns a 64-bit integer value for handling large datasets.
    /// </summary>
    long RowsCopied64 { get; }

    /// <summary>
    /// Gets the maximum number of retry attempts for failed operations.
    /// The operation will throw an exception if this number is exceeded.
    /// </summary>
    int MaxRetryCount { get; }

    /// <summary>
    /// Gets whether the destination table should be truncated before performing the bulk insert.
    /// When true, all existing data in the destination table will be removed before the operation.
    /// </summary>
    bool TruncateBeforeBulkInsert { get; }

    /// <summary>
    /// Gets whether exponential backoff should be used for retry delays.
    /// When true, the delay between retry attempts will increase exponentially.
    /// </summary>
    bool UseExponentialBackoff { get; }

    /// <summary>
    /// Gets the initial delay duration between retry attempts.
    /// This value may be increased if UseExponentialBackoff is true.
    /// </summary>
    TimeSpan InitialDelay { get; }

    /// <summary>
    /// Asynchronously writes the data from the specified stream to the server.
    /// </summary>
    /// <param name="stream">The stream containing the data to be copied to the server.</param>
    /// <param name="encoding">The character encoding to use when reading the stream.</param>
    /// <param name="timeout">The time to wait for each batch to complete before generating an error.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when:
    /// - Retries are configured with an external transaction
    /// - Retries are configured with an external connection
    /// - Retries are enabled without table truncation
    /// - The maximum retry count is exceeded
    /// </exception>
    Task WriteToServerAsync(Stream stream, Encoding encoding, TimeSpan timeout);
}