namespace SqlBulkCopier.CsvHelper;

/// <summary>
/// Interface for building a bulk copier that processes CSV files with headers.
/// </summary>
/// <remarks>
/// This interface provides functionality for mapping CSV columns to database columns
/// using header names. It supports both simple mappings and mappings with custom
/// column context configuration.
/// </remarks>
public interface ICsvBulkCopierWithHeaderBuilder : ICsvBulkCopierBuilder<ICsvBulkCopierWithHeaderBuilder>
{
    /// <summary>
    /// Adds a column mapping using the CSV header name.
    /// </summary>
    /// <param name="columnName">The name of the column in the CSV header.</param>
    /// <returns>The builder instance for method chaining.</returns>
    /// <remarks>
    /// The column name from the CSV header will be used as both the source and
    /// destination column name in the bulk copy operation.
    /// </remarks>
    ICsvBulkCopierWithHeaderBuilder AddColumnMapping(string columnName);

    /// <summary>
    /// Adds a column mapping with custom configuration using the CSV header name.
    /// </summary>
    /// <param name="columnName">The name of the column in the CSV header.</param>
    /// <param name="c">An action that configures the column context for data conversion and handling.</param>
    /// <returns>The builder instance for method chaining.</returns>
    /// <remarks>
    /// This overload allows you to specify how the CSV column data should be processed,
    /// including data type conversion, formatting, and NULL handling.
    /// </remarks>
    ICsvBulkCopierWithHeaderBuilder AddColumnMapping(string columnName, Action<IColumnContext> c);

    /// <summary>
    /// Adds a column mapping using the CSV header name.
    /// </summary>
    /// <param name="dbColumnName">The name of the column in the Table of database.</param>
    /// <param name="csvColumnName">The name of the column in the CSV header.</param>
    /// <returns>The builder instance for method chaining.</returns>
    /// <remarks>
    /// The column name from the CSV header will be used as both the source and
    /// destination column name in the bulk copy operation.
    /// </remarks>
    ICsvBulkCopierWithHeaderBuilder AddColumnMapping(string dbColumnName, string csvColumnName);

    /// <summary>
    /// Adds a column mapping with custom configuration using the CSV header name.
    /// </summary>
    /// <param name="dbColumnName">The name of the column in the Table of database.</param>
    /// <param name="csvColumnName">The name of the column in the CSV header.</param>
    /// <param name="c">An action that configures the column context for data conversion and handling.</param>
    /// <returns>The builder instance for method chaining.</returns>
    /// <remarks>
    /// This overload allows you to specify how the CSV column data should be processed,
    /// including data type conversion, formatting, and NULL handling.
    /// </remarks>
    ICsvBulkCopierWithHeaderBuilder AddColumnMapping(string dbColumnName, string csvColumnName, Action<IColumnContext> c);
}