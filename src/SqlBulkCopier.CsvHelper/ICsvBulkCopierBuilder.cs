using CsvHelper;

namespace SqlBulkCopier.CsvHelper;

/// <summary>
/// Interface for building a bulk copier with header
/// </summary>
public interface ICsvBulkCopierBuilder : IBulkCopierBuilder
{
    /// <summary>
    /// Set max retry count
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    ICsvBulkCopierBuilder SetMaxRetryCount(int value);

    /// <summary>
    /// Set retry delay
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    ICsvBulkCopierBuilder SetInitialDelay(TimeSpan value);

    /// <summary>
    /// Set truncate before bulk insert
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    ICsvBulkCopierBuilder SetTruncateBeforeBulkInsert(bool value);

    /// <summary>
    /// Set use exponential backoff
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    ICsvBulkCopierBuilder SetUseExponentialBackoff(bool value);

    /// <summary>
    /// Set batch size
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    ICsvBulkCopierBuilder SetBatchSize(int value);

    /// <summary>
    /// Set notify after
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    ICsvBulkCopierBuilder SetNotifyAfter(int value);

    /// <summary>
    /// Setup default column context
    /// </summary>
    /// <param name="c"></param>
    /// <returns></returns>
    ICsvBulkCopierBuilder SetDefaultColumnContext(Action<IColumnContext> c);

    /// <summary>
    /// Set row filter
    /// </summary>
    /// <param name="rowFilter"></param>
    /// <returns></returns>
    ICsvBulkCopierBuilder SetRowFilter(Predicate<CsvReader> rowFilter);

    /// <summary>
    /// Add column mapping
    /// </summary>
    /// <param name="columnName"></param>
    /// <returns></returns>
    ICsvBulkCopierBuilder AddColumnMapping(string columnName);

    /// <summary>
    /// Add column mapping with column context
    /// </summary>
    /// <param name="columnName"></param>
    /// <param name="c"></param>
    /// <returns></returns>
    ICsvBulkCopierBuilder AddColumnMapping(string columnName, Action<IColumnContext> c);
}