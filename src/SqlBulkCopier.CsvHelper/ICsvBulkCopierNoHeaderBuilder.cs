using CsvHelper;

namespace SqlBulkCopier.CsvHelper;

public interface ICsvBulkCopierBuilder<out TBuilder> : IBulkCopierBuilder<TBuilder>
    where TBuilder : ICsvBulkCopierBuilder<TBuilder>
{
    /// <summary>
    /// Set row filter
    /// </summary>
    /// <param name="rowFilter"></param>
    /// <returns></returns>
    TBuilder SetRowFilter(Predicate<CsvReader> rowFilter);
}

public interface ICsvBulkCopierNoHeaderBuilder : ICsvBulkCopierBuilder<ICsvBulkCopierNoHeaderBuilder>
{
    ICsvBulkCopierNoHeaderBuilder AddColumnMapping(string dbColumnName, int csvColumnOrdinal);
    ICsvBulkCopierNoHeaderBuilder AddColumnMapping(string dbColumnName, int csvColumnOrdinal, Action<IColumnContext> c);
}