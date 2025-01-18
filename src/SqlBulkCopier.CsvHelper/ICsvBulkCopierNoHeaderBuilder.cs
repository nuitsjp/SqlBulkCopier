using CsvHelper;

namespace SqlBulkCopier.CsvHelper;

public interface ICsvBulkCopierNoHeaderBuilder : IBulkCopierBuilder
{
    /// <summary>
    /// Set max retry count
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    ICsvBulkCopierNoHeaderBuilder SetMaxRetryCount(int value);

    /// <summary>
    /// Set retry delay
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    ICsvBulkCopierNoHeaderBuilder SetInitialDelay(TimeSpan value);

    /// <summary>
    /// Set truncate before bulk insert
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    ICsvBulkCopierNoHeaderBuilder SetTruncateBeforeBulkInsert(bool value);

    /// <summary>
    /// Set use exponential backoff
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    ICsvBulkCopierNoHeaderBuilder SetUseExponentialBackoff(bool value);

    /// <summary>
    /// Set batch size
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    ICsvBulkCopierNoHeaderBuilder SetBatchSize(int value);

    /// <summary>
    /// Set notify after
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    ICsvBulkCopierNoHeaderBuilder SetNotifyAfter(int value);

    /// <summary>
    /// Setup default column context
    /// </summary>
    /// <param name="c"></param>
    /// <returns></returns>
    ICsvBulkCopierNoHeaderBuilder SetDefaultColumnContext(Action<IColumnContext> c);
    /// <summary>
    /// Set row filter
    /// </summary>
    /// <param name="rowFilter"></param>
    /// <returns></returns>
    ICsvBulkCopierNoHeaderBuilder SetRowFilter(Predicate<CsvReader> rowFilter);

    ICsvBulkCopierNoHeaderBuilder AddColumnMapping(string dbColumnName, int csvColumnOrdinal);
    ICsvBulkCopierNoHeaderBuilder AddColumnMapping(string dbColumnName, int csvColumnOrdinal, Action<IColumnContext> c);
}