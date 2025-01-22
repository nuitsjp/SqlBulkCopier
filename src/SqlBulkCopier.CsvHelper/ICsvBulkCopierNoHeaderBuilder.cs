namespace SqlBulkCopier.CsvHelper;

/// <summary>
/// Interface for building a bulk copier that processes CSV files without headers.
/// </summary>
/// <remarks>
/// This interface provides functionality for mapping CSV columns to database columns
/// using ordinal positions. It supports both simple ordinal mappings and mappings
/// with custom column context configuration.
/// </remarks>
public interface ICsvBulkCopierNoHeaderBuilder : ICsvBulkCopierBuilder<ICsvBulkCopierNoHeaderBuilder>
{
    /// <summary>
    /// Adds a column mapping using the CSV column ordinal position.
    /// </summary>
    /// <param name="dbColumnName">The name of the column in the destination database table.</param>
    /// <param name="csvColumnOrdinal">The zero-based ordinal position of the column in the CSV file.</param>
    /// <returns>The builder instance for method chaining.</returns>
    /// <remarks>
    /// Maps a CSV column at the specified ordinal position to a database column with the given name.
    /// This is used when the CSV file does not contain header information.
    /// </remarks>
    ICsvBulkCopierNoHeaderBuilder AddColumnMapping(string dbColumnName, int csvColumnOrdinal);

    /// <summary>
    /// Adds a column mapping with custom configuration using the CSV column ordinal position.
    /// </summary>
    /// <param name="dbColumnName">The name of the column in the destination database table.</param>
    /// <param name="csvColumnOrdinal">The zero-based ordinal position of the column in the CSV file.</param>
    /// <param name="c">An action that configures the column context for data conversion and handling.</param>
    /// <returns>The builder instance for method chaining.</returns>
    /// <remarks>
    /// This overload allows you to specify how the CSV column data should be processed,
    /// including data type conversion, formatting, and NULL handling, while mapping
    /// by ordinal position instead of header names.
    /// </remarks>
    ICsvBulkCopierNoHeaderBuilder AddColumnMapping(string dbColumnName, int csvColumnOrdinal, Action<IColumnContext> c);
}