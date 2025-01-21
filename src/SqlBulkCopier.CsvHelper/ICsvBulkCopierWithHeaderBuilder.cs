namespace SqlBulkCopier.CsvHelper;

/// <summary>
/// Interface for building a bulk copier with header
/// </summary>
public interface ICsvBulkCopierWithHeaderBuilder : ICsvBulkCopierBuilder<ICsvBulkCopierWithHeaderBuilder>
{
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