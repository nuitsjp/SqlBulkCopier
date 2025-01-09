using CsvHelper;

namespace SqlBulkCopier.CsvHelper
{
    /// <summary>
    /// Interface for building a bulk copier with header
    /// </summary>
    public interface ICsvBulkCopierBuilder : IBulkCopierBuilder
    {
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
}