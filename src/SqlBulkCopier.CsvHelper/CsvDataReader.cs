using System.Data;
using CsvHelper;

namespace SqlBulkCopier.CsvHelper;

/// <summary>
/// Implements IDataReader for reading CSV data using CsvHelper.
/// This class provides a bridge between CSV data and SQL Server bulk copy operations.
/// </summary>
/// <remarks>
/// This implementation:
/// - Supports CSV files with or without headers
/// - Allows filtering of rows during reading
/// - Provides column mapping and data type conversion
/// - Manages resource cleanup through IDisposable
/// </remarks>
public class CsvDataReader : IDataReader
{
    private readonly Dictionary<int, Column> _columns;

    private readonly CsvReader _csvReader;
    private readonly Predicate<CsvReader> _rowFilter;
    private bool _isDisposed;

    /// <summary>
    /// Initializes a new instance of the CsvDataReader class.
    /// </summary>
    /// <param name="csvReader">The CsvHelper reader instance to use for reading CSV data.</param>
    /// <param name="columns">The collection of column definitions for mapping and conversion.</param>
    /// <param name="hasHeader">True if the CSV file has a header row; otherwise, false.</param>
    /// <param name="rowFilter">Optional predicate to filter rows during reading.</param>
    /// <remarks>
    /// When hasHeader is true, the constructor will:
    /// 1. Read the header row
    /// 2. Map column ordinals based on header names
    /// 3. Configure column mappings for the bulk copy operation
    /// </remarks>
    public CsvDataReader(CsvReader csvReader, IEnumerable<Column> columns, bool hasHeader = true, Predicate<CsvReader>? rowFilter = null)
    {
        _csvReader = csvReader;
        _rowFilter = rowFilter ?? (_ => true);
        if (hasHeader)
        {
            // When the CSV file has a header, we need to read header first.
            while (csvReader.Read())
            {
                if (_rowFilter(csvReader))
                {
                    break;
                }
            }
            csvReader.ReadHeader();
        }

        _columns = (hasHeader
                // When the CSV file has a header, get the column ordinal from the header.
                ? columns.Select(c => c with { Ordinal = csvReader.GetFieldIndex(c.Name) }).ToArray()
                : columns)
            .ToDictionary(x => x.Ordinal, x => x);
    }

    /// <summary>
    /// Gets the ordinal position of the named column.
    /// </summary>
    /// <param name="name">The name of the column to find.</param>
    /// <returns>The zero-based column ordinal.</returns>
    /// <exception cref="ArgumentException">The name specified is not a valid column name.</exception>
    public int GetOrdinal(string name)
    {
        return _csvReader.GetFieldIndex(name);
    }

    /// <summary>
    /// Gets the value of the specified column as an object.
    /// </summary>
    /// <param name="i">The zero-based column ordinal.</param>
    /// <returns>The value of the column.</returns>
    /// <exception cref="IndexOutOfRangeException">The column index is out of range.</exception>
    /// <remarks>
    /// The value is converted according to the column's configuration,
    /// including any type conversion, formatting, and NULL handling settings.
    /// </remarks>
    public object GetValue(int i)
    {
        if (_columns.TryGetValue(i, out var column))
        {
            return column.Convert(_csvReader.GetField(i)!);
        }

        throw new IndexOutOfRangeException($"Field with ordinal '{i}' was not found.");
    }

    /// <summary>
    /// Advances the reader to the next record.
    /// </summary>
    /// <returns>true if there are more rows; otherwise, false.</returns>
    /// <remarks>
    /// This method applies any configured row filter before returning.
    /// Filtered rows are skipped automatically.
    /// </remarks>
    public bool Read()
    {
        while (_csvReader.Read())
        {
            if (_rowFilter(_csvReader))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Closes the reader, releasing any resources.
    /// </summary>
    public void Close()
    {
        Dispose(true);
    }

    /// <summary>
    /// Disposes of the reader, releasing any resources.
    /// </summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Releases the unmanaged resources used by the reader and optionally releases the managed resources.
    /// </summary>
    /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
    private void Dispose(bool disposing)
    {
        if (!_isDisposed)
        {
            if (disposing)
            {
                _csvReader.Dispose();
            }

            _isDisposed = true;
        }
    }

    #region NotSupportedException
    /// <inheritdoc />
    public int Depth => throw new NotSupportedException();

    /// <inheritdoc />
    public bool IsClosed => throw new NotSupportedException();

    /// <inheritdoc />
    public int RecordsAffected => throw new NotSupportedException();

    /// <inheritdoc />
    public bool GetBoolean(int i) => throw new NotSupportedException();

    /// <inheritdoc />
    public byte GetByte(int i) => throw new NotSupportedException();

    /// <inheritdoc />
    public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) => throw new NotSupportedException();

    /// <inheritdoc />
    public char GetChar(int i) => throw new NotSupportedException();

    /// <inheritdoc />
    public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) => throw new NotSupportedException();

    /// <inheritdoc />
    public IDataReader GetData(int i) => throw new NotSupportedException();

    /// <inheritdoc />
    public string GetDataTypeName(int i) => throw new NotSupportedException();

    /// <inheritdoc />
    public DateTime GetDateTime(int i) => throw new NotSupportedException();

    /// <inheritdoc />
    public decimal GetDecimal(int i) => throw new NotSupportedException();

    /// <inheritdoc />
    public double GetDouble(int i) => throw new NotSupportedException();

    /// <inheritdoc />
    public Type GetFieldType(int i) => throw new NotSupportedException();

    /// <inheritdoc />
    public float GetFloat(int i) => throw new NotSupportedException();

    /// <inheritdoc />
    public Guid GetGuid(int i) => throw new NotSupportedException();

    /// <inheritdoc />
    public short GetInt16(int i) => throw new NotSupportedException();

    /// <inheritdoc />
    public int GetInt32(int i) => throw new NotSupportedException();

    /// <inheritdoc />
    public long GetInt64(int i) => throw new NotSupportedException();

    /// <inheritdoc />
    public string GetName(int i) => throw new NotSupportedException();

    /// <inheritdoc />
    public string GetString(int i) => throw new NotSupportedException();

    /// <inheritdoc />
    public int GetValues(object[] values) => throw new NotSupportedException();

    /// <inheritdoc />
    public bool IsDBNull(int i) => throw new NotSupportedException();

    /// <summary>
    /// Gets the number of columns in the current row.
    /// </summary>
    public int FieldCount => _columns.Count;

    public object this[int i] => throw new NotSupportedException();

    public object this[string name] => throw new NotSupportedException();

    /// <inheritdoc />
    public DataTable GetSchemaTable() => throw new NotSupportedException();

    /// <inheritdoc />
    public bool NextResult() => throw new NotSupportedException();

    #endregion
}