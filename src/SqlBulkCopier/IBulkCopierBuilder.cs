using Microsoft.Data.SqlClient;

namespace SqlBulkCopier;

public interface IBulkCopierBuilder
{
    IEnumerable<Column> BuildColumns();
    IBulkCopier Build(SqlConnection connection);
    IBulkCopier Build(string connectionString);
    IBulkCopier Build(string connectionString, SqlBulkCopyOptions copyOptions);
    IBulkCopier Build(SqlConnection connection, SqlBulkCopyOptions copyOptions, SqlTransaction externalTransaction);
}

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