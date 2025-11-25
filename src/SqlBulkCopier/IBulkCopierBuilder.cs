using Microsoft.Data.SqlClient;

namespace SqlBulkCopier;

/// <summary>
/// Provides builder interface for creating bulk copier instances.
/// </summary>
/// <remarks>
/// This interface defines the core functionality for building bulk copier instances
/// with different connection and configuration options. It supports creating bulk copiers
/// using various combinations of connection strings, SQL connections, and bulk copy options.
/// </remarks>
public interface IBulkCopierBuilder
{
    /// <summary>
    /// Builds column definitions for bulk copy operation.
    /// </summary>
    /// <returns>A collection of column definitions that specify how source data maps to destination columns.</returns>
    /// <remarks>
    /// The column definitions determine how source data is converted and mapped to
    /// destination table columns during the bulk copy operation.
    /// </remarks>
    IEnumerable<Column> BuildColumns();

    /// <summary>
    /// Creates a bulk copier instance using the specified SQL connection.
    /// </summary>
    /// <param name="connection">SQL connection to use for bulk copy. The connection must be opened before performing bulk copy operations.</param>
    /// <returns>A configured bulk copier instance ready for use.</returns>
    /// <exception cref="ArgumentNullException">Thrown when connection is null.</exception>
    IBulkCopier Build(SqlConnection connection);

    /// <summary>
    /// Creates a bulk copier instance using the specified connection string.
    /// </summary>
    /// <param name="connectionString">Connection string to SQL Server. Must contain all necessary information to establish a connection.</param>
    /// <returns>A configured bulk copier instance ready for use.</returns>
    /// <exception cref="ArgumentNullException">Thrown when connectionString is null or empty.</exception>
    /// <exception cref="ArgumentException">Thrown when connectionString is invalid.</exception>
    IBulkCopier Build(string connectionString);

    /// <summary>
    /// Creates a bulk copier instance using the specified connection string and copy options.
    /// </summary>
    /// <param name="connectionString">Connection string to SQL Server. Must contain all necessary information to establish a connection.</param>
    /// <param name="copyOptions">SQL bulk copy options to configure the operation behavior.</param>
    /// <returns>A configured bulk copier instance ready for use.</returns>
    /// <exception cref="ArgumentNullException">Thrown when connectionString is null or empty.</exception>
    /// <exception cref="ArgumentException">Thrown when connectionString is invalid.</exception>
    IBulkCopier Build(string connectionString, SqlBulkCopyOptions copyOptions);

    /// <summary>
    /// Creates a bulk copier instance with specified connection, options and transaction.
    /// </summary>
    /// <param name="connection">SQL connection to use for bulk copy. The connection must be opened before performing bulk copy operations.</param>
    /// <param name="copyOptions">SQL bulk copy options to configure the operation behavior.</param>
    /// <param name="externalTransaction">External transaction to use for the bulk copy operation. All bulk copy operations will be part of this transaction.</param>
    /// <returns>A configured bulk copier instance ready for use.</returns>
    /// <exception cref="ArgumentNullException">Thrown when connection or externalTransaction is null.</exception>
    IBulkCopier Build(SqlConnection connection, SqlBulkCopyOptions copyOptions, SqlTransaction externalTransaction);
}

/// <summary>
/// Generic builder interface for creating bulk copier instances with fluent configuration.
/// </summary>
/// <typeparam name="TBuilder">The type of the builder implementing this interface.</typeparam>
/// <remarks>
/// This interface extends IBulkCopierBuilder to provide a fluent API for configuring
/// bulk copier instances. It allows setting various options such as retry behavior,
/// batch size, and notification thresholds in a chainable manner.
/// </remarks>
public interface IBulkCopierBuilder<out TBuilder> : IBulkCopierBuilder
    where TBuilder : IBulkCopierBuilder<TBuilder>
{
    /// <summary>
    /// Sets the maximum number of retry attempts for failed bulk copy operations.
    /// </summary>
    /// <param name="value">The maximum number of retry attempts. Must be non-negative.</param>
    /// <returns>The builder instance for method chaining.</returns>
    /// <remarks>
    /// When retries are enabled, the bulk copier will attempt to retry failed operations
    /// up to the specified number of times before throwing an exception.
    /// </remarks>
    TBuilder SetMaxRetryCount(int value);

    /// <summary>
    /// Sets the initial delay duration between retry attempts.
    /// </summary>
    /// <param name="value">The time to wait before the first retry attempt.</param>
    /// <returns>The builder instance for method chaining.</returns>
    /// <remarks>
    /// This delay may be increased if exponential backoff is enabled through SetUseExponentialBackoff.
    /// </remarks>
    TBuilder SetInitialDelay(TimeSpan value);

    /// <summary>
    /// Sets whether to truncate the destination table before performing bulk insert.
    /// </summary>
    /// <param name="value">True to truncate the table before bulk insert; otherwise, false.</param>
    /// <returns>The builder instance for method chaining.</returns>
    /// <remarks>
    /// When enabled, all existing data in the destination table will be removed before
    /// the bulk copy operation begins. This option is required when using retries.
    /// </remarks>
    TBuilder SetTruncateBeforeBulkInsert(bool value);

    /// <summary>
    /// Sets the method used to remove existing data before performing the bulk insert.
    /// </summary>
    /// <param name="value">The truncate method to use.</param>
    /// <returns>The builder instance for method chaining.</returns>
    TBuilder SetTruncateMethod(TruncateMethod value);

    /// <summary>
    /// Sets whether to use exponential backoff for retry delays.
    /// </summary>
    /// <param name="value">True to use exponential backoff; otherwise, false.</param>
    /// <returns>The builder instance for method chaining.</returns>
    /// <remarks>
    /// When enabled, the delay between retry attempts will increase exponentially,
    /// starting from the initial delay value.
    /// </remarks>
    TBuilder SetUseExponentialBackoff(bool value);

    /// <summary>
    /// Sets the number of rows in each batch for the bulk copy operation.
    /// </summary>
    /// <param name="value">The number of rows per batch. Must be positive.</param>
    /// <returns>The builder instance for method chaining.</returns>
    /// <remarks>
    /// A batch is committed to the server when it reaches this size, allowing for
    /// better memory management and progress tracking during large operations.
    /// </remarks>
    TBuilder SetBatchSize(int value);

    /// <summary>
    /// Sets the number of rows to process before generating a notification event.
    /// </summary>
    /// <param name="value">The number of rows to process before notification. Must be positive.</param>
    /// <returns>The builder instance for method chaining.</returns>
    /// <remarks>
    /// This setting controls how frequently the SqlRowsCopied event is raised during
    /// the bulk copy operation, allowing for progress monitoring.
    /// </remarks>
    TBuilder SetNotifyAfter(int value);

    /// <summary>
    /// Sets up the default context for all columns in the bulk copy operation.
    /// </summary>
    /// <param name="c">Action that configures the default column context.</param>
    /// <returns>The builder instance for method chaining.</returns>
    /// <remarks>
    /// The default context is applied to all columns before any column-specific
    /// configuration is applied. This allows setting common options across all columns.
    /// </remarks>
    TBuilder SetDefaultColumnContext(Action<IColumnContext> c);
}
