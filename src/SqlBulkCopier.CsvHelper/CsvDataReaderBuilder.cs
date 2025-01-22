using System.Data;
using System.Globalization;
using System.Text;
using CsvHelper;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier.CsvHelper;

/// <summary>
/// Implements IDataReaderBuilder for creating CSV data readers.
/// </summary>
/// <remarks>
/// This builder creates data readers that can process CSV files with or without headers,
/// supporting column mapping and row filtering capabilities.
/// </remarks>
/// <param name="hasHeader">True if the CSV file has a header row; otherwise, false.</param>
/// <param name="columns">The collection of column definitions for mapping and conversion.</param>
/// <param name="rowFilter">Predicate to filter rows during reading.</param>
public class CsvDataReaderBuilder(bool hasHeader, IEnumerable<Column> columns, Predicate<CsvReader> rowFilter) : IDataReaderBuilder
{
    /// <summary>
    /// Gets whether the CSV file has a header row.
    /// </summary>
    public bool HasHeader => hasHeader;

    /// <summary>
    /// Sets up the column mappings for the SqlBulkCopy operation.
    /// </summary>
    /// <param name="sqlBulkCopy">The SqlBulkCopy instance to configure.</param>
    /// <remarks>
    /// For files with headers, maps columns by name.
    /// For files without headers, maps columns by ordinal position.
    /// </remarks>
    public void SetupColumnMappings(SqlBulkCopy sqlBulkCopy)
    {
        foreach (var column in columns)
        {
            if (hasHeader)
            {
                // When the CSV file has a header, we can use the column name.
                sqlBulkCopy.ColumnMappings.Add(column.Name, column.Name);
            }
            else
            {
                // When the CSV file does not have a header, we use the column ordinal.
                sqlBulkCopy.ColumnMappings.Add(column.Ordinal, column.Name);
            }
        }
    }

    /// <summary>
    /// Creates a new CsvDataReader instance for reading CSV data.
    /// </summary>
    /// <param name="stream">The stream containing the CSV data.</param>
    /// <param name="encoding">The character encoding to use when reading the stream.</param>
    /// <returns>A configured CsvDataReader instance.</returns>
    /// <remarks>
    /// The created reader will:
    /// - Use the specified encoding for reading the CSV data
    /// - Apply the configured column mappings and data conversions
    /// - Filter rows based on the provided predicate
    /// - Handle headers according to the hasHeader setting
    /// </remarks>
    public IDataReader Build(Stream stream, Encoding encoding)
    {
        var csvReader = new CsvReader(new StreamReader(stream, encoding), CultureInfo.CurrentCulture);
        return new CsvDataReader(csvReader, columns, hasHeader, rowFilter);
    }
}