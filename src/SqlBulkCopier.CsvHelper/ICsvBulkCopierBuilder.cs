using CsvHelper;

namespace SqlBulkCopier.CsvHelper;

/// <summary>
/// Provides a builder interface for creating bulk copier instances that work with CSV data.
/// </summary>
/// <typeparam name="TBuilder">The type of the builder implementing this interface.</typeparam>
/// <remarks>
/// This interface extends the base IBulkCopierBuilder to provide CSV-specific functionality,
/// such as row filtering based on CSV content. It maintains the fluent API pattern
/// for configuration while adding CSV-specific features.
/// </remarks>
public interface ICsvBulkCopierBuilder<out TBuilder> : IBulkCopierBuilder<TBuilder>
    where TBuilder : ICsvBulkCopierBuilder<TBuilder>
{
    /// <summary>
    /// Sets a predicate to filter CSV rows before they are copied to the database.
    /// </summary>
    /// <param name="rowFilter">A predicate that takes a CsvReader and returns true for rows that should be included.</param>
    /// <returns>The builder instance for method chaining.</returns>
    /// <remarks>
    /// The filter is applied while reading the CSV data, allowing you to skip rows
    /// based on their content before they are processed for bulk copy.
    /// </remarks>
    TBuilder SetRowFilter(Predicate<CsvReader> rowFilter);
}