using Microsoft.Data.SqlClient;

namespace SqlBulkCopier;

/// <summary>
/// Provides builder interface for creating bulk copier instances
/// </summary>
public interface IBulkCopierBuilder
{
    /// <summary>
    /// Builds column definitions for bulk copy operation
    /// </summary>
    /// <returns>Collection of column definitions</returns>
    IEnumerable<Column> BuildColumns();

    /// <summary>
    /// Creates a bulk copier instance using the specified SQL connection
    /// </summary>
    /// <param name="connection">SQL connection to use for bulk copy</param>
    /// <returns>Configured bulk copier instance</returns>
    IBulkCopier Build(SqlConnection connection);

    /// <summary>
    /// Creates a bulk copier instance using the specified connection string
    /// </summary>
    /// <param name="connectionString">Connection string to SQL Server</param>
    /// <returns>Configured bulk copier instance</returns>
    IBulkCopier Build(string connectionString);

    /// <summary>
    /// Creates a bulk copier instance using the specified connection string and copy options
    /// </summary>
    /// <param name="connectionString">Connection string to SQL Server</param>
    /// <param name="copyOptions">SQL bulk copy options to configure the operation</param>
    /// <returns>Configured bulk copier instance</returns>
    IBulkCopier Build(string connectionString, SqlBulkCopyOptions copyOptions);

    /// <summary>
    /// Creates a bulk copier instance with specified connection, options and transaction
    /// </summary>
    /// <param name="connection">SQL connection to use for bulk copy</param>
    /// <param name="copyOptions">SQL bulk copy options to configure the operation</param>
    /// <param name="externalTransaction">External transaction to use for the bulk copy operation</param>
    /// <returns>Configured bulk copier instance</returns>
    IBulkCopier Build(SqlConnection connection, SqlBulkCopyOptions copyOptions, SqlTransaction externalTransaction);
}

/// <summary>
/// Generic builder interface for creating bulk copier instances with fluent configuration
/// </summary>
/// <typeparam name="TBuilder">Type of the concrete builder implementing this interface</typeparam>
public interface IBulkCopierBuilder<out TBuilder> : IBulkCopierBuilder
    where TBuilder : IBulkCopierBuilder<TBuilder>
{
    /// <summary>
    /// Set max retry count
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    TBuilder SetMaxRetryCount(int value);

    /// <summary>
    /// Set retry delay
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    TBuilder SetInitialDelay(TimeSpan value);

    /// <summary>
    /// Set truncate before bulk insert
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    TBuilder SetTruncateBeforeBulkInsert(bool value);

    /// <summary>
    /// Set use exponential backoff
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    TBuilder SetUseExponentialBackoff(bool value);

    /// <summary>
    /// Set batch size
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    TBuilder SetBatchSize(int value);

    /// <summary>
    /// Set notify after
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    TBuilder SetNotifyAfter(int value);

    /// <summary>
    /// Setup default column context
    /// </summary>
    /// <param name="c"></param>
    /// <returns></returns>
    TBuilder SetDefaultColumnContext(Action<IColumnContext> c);
}