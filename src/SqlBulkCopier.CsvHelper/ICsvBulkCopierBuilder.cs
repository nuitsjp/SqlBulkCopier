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