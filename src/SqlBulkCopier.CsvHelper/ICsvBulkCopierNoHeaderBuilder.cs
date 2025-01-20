using CsvHelper;

namespace SqlBulkCopier.CsvHelper;

public interface ICsvBulkCopierNoHeaderBuilder : IBulkCopierBuilder<ICsvBulkCopierNoHeaderBuilder>
{
    /// <summary>
    /// Set row filter
    /// </summary>
    /// <param name="rowFilter"></param>
    /// <returns></returns>
    ICsvBulkCopierNoHeaderBuilder SetRowFilter(Predicate<CsvReader> rowFilter);

    ICsvBulkCopierNoHeaderBuilder AddColumnMapping(string dbColumnName, int csvColumnOrdinal);
    ICsvBulkCopierNoHeaderBuilder AddColumnMapping(string dbColumnName, int csvColumnOrdinal, Action<IColumnContext> c);
}