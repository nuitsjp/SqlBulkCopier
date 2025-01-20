using CsvHelper;

namespace SqlBulkCopier.CsvHelper;

/// <summary>
/// Interface for building a bulk copier with header
/// </summary>
public interface ICsvBulkCopierWithHeaderBuilder : IBulkCopierBuilder
{
    /// <summary>
    /// Set max retry count
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    ICsvBulkCopierWithHeaderBuilder SetMaxRetryCount(int value);

    /// <summary>
    /// Set retry delay
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    ICsvBulkCopierWithHeaderBuilder SetInitialDelay(TimeSpan value);

    /// <summary>
    /// Set truncate before bulk insert
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    ICsvBulkCopierWithHeaderBuilder SetTruncateBeforeBulkInsert(bool value);

    /// <summary>
    /// Set use exponential backoff
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    ICsvBulkCopierWithHeaderBuilder SetUseExponentialBackoff(bool value);

    /// <summary>
    /// Set batch size
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    ICsvBulkCopierWithHeaderBuilder SetBatchSize(int value);

    /// <summary>
    /// Set notify after
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    ICsvBulkCopierWithHeaderBuilder SetNotifyAfter(int value);

    /// <summary>
    /// Setup default column context
    /// </summary>
    /// <param name="c"></param>
    /// <returns></returns>
    ICsvBulkCopierWithHeaderBuilder SetDefaultColumnContext(Action<IColumnContext> c);

    /// <summary>
    /// Set row filter
    /// </summary>
    /// <param name="rowFilter"></param>
    /// <returns></returns>
    ICsvBulkCopierWithHeaderBuilder SetRowFilter(Predicate<CsvReader> rowFilter);

    /// <summary>
    /// Add column mapping
    /// </summary>
    /// <param name="columnName"></param>
    /// <returns></returns>
    ICsvBulkCopierWithHeaderBuilder AddColumnMapping(string columnName);

    /// <summary>
    /// Add column mapping with column context
    /// </summary>
    /// <param name="columnName"></param>
    /// <param name="c"></param>
    /// <returns></returns>
    ICsvBulkCopierWithHeaderBuilder AddColumnMapping(string columnName, Action<IColumnContext> c);
}