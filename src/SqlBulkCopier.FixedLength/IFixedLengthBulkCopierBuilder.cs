using FixedLengthHelper;

namespace SqlBulkCopier.FixedLength;

public interface IFixedLengthBulkCopierBuilder : IBulkCopierBuilder
{
    /// <summary>
    /// Set max retry count
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    IFixedLengthBulkCopierBuilder SetMaxRetryCount(int value);

    /// <summary>
    /// Set retry delay
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    IFixedLengthBulkCopierBuilder SetInitialDelay(TimeSpan value);

    /// <summary>
    /// Set truncate before bulk insert
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    IFixedLengthBulkCopierBuilder SetTruncateBeforeBulkInsert(bool value);

    /// <summary>
    /// Set use exponential backoff
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    IFixedLengthBulkCopierBuilder SetUseExponentialBackoff(bool value);

    /// <summary>
    /// Set batch size
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    IFixedLengthBulkCopierBuilder SetBatchSize(int value);

    /// <summary>
    /// Set notify after
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    IFixedLengthBulkCopierBuilder SetNotifyAfter(int value);
    IFixedLengthBulkCopierBuilder SetDefaultColumnContext(Action<IColumnContext> c);
    IFixedLengthBulkCopierBuilder SetRowFilter(Predicate<IFixedLengthReader> rowFilter);
    IFixedLengthBulkCopierBuilder AddColumnMapping(string dbColumnName, int offsetBytes, int lengthBytes);
    IFixedLengthBulkCopierBuilder AddColumnMapping(string dbColumnName, int offsetBytes, int lengthBytes, Action<IColumnContext> c);
}