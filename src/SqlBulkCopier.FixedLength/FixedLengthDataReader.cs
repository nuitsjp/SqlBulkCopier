using System.Data;
using FixedLengthHelper;

namespace SqlBulkCopier.FixedLength;

/// <summary>
/// Provides an IDataReader implementation for reading data from fixed-length files.
/// </summary>
public class FixedLengthDataReader : IDataReader
{
    /// <summary>
    /// The IFixedLengthReader instance used to read the fixed-length file.
    /// </summary>
    private readonly IFixedLengthReader _fixedLengthReader;

    /// <summary>
    /// A dictionary mapping column names to their ordinal positions.
    /// </summary>
    private readonly IReadOnlyDictionary<string, int> _columnOrdinals;

    /// <summary>
    /// A list of columns in the fixed-length file.
    /// </summary>
    private readonly IReadOnlyList<FixedLengthColumn> _columns;

    /// <summary>
    /// A predicate used to filter rows.
    /// </summary>
    private readonly Predicate<IFixedLengthReader> _rowFilter;

    /// <summary>
    /// A flag indicating whether the reader has been disposed.
    /// </summary>
    private bool _isDisposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="FixedLengthDataReader"/> class.
    /// </summary>
    /// <param name="fixedLengthReader">The fixed-length reader instance.</param>
    /// <param name="columns">The list of columns in the fixed-length file.</param>
    /// <param name="rowFilter">The predicate used to filter rows.</param>
    public FixedLengthDataReader(
        IFixedLengthReader fixedLengthReader,
        IReadOnlyList<FixedLengthColumn> columns,
        Predicate<IFixedLengthReader> rowFilter)
    {
        _fixedLengthReader = fixedLengthReader;
        _columnOrdinals = columns
            .ToDictionary(x => x.Name, column => column.Ordinal);
        _columns = columns.ToArray();
        _rowFilter = rowFilter;
    }

    /// <inheritdoc />
    public int FieldCount => _columns.Count;

    /// <inheritdoc />
    public int Depth => 0;

    /// <inheritdoc />
    public bool IsClosed => _fixedLengthReader.IsClosed;

    /// <inheritdoc />
    public int RecordsAffected => 0;

    /// <inheritdoc />
    public object this[int i] => GetValue(i);

    /// <inheritdoc />
    public object this[string name] => throw new NotSupportedException();

    /// <inheritdoc />
    public int GetOrdinal(string name)
    {
        if (_columnOrdinals.TryGetValue(name, out var ordinal))
        {
            return ordinal;
        }
        throw new IndexOutOfRangeException($"Field with name '{name}' was not found.");
    }

    /// <inheritdoc />
    public object GetValue(int i)
    {
        if (_columns.Count <= i) throw new IndexOutOfRangeException($"Field with ordinal '{i}' was not found.");

        var column = _columns[i];
        return column.Convert(_fixedLengthReader.GetField(column.OffsetBytes, column.LengthBytes));
    }

    /// <inheritdoc />
    public void Close()
    {
        Dispose();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_isDisposed)
        {
            return;
        }
        _isDisposed = true;
        Dispose(true);
    }

    /// <summary>
    /// Disposes the reader and releases any resources.
    /// </summary>
    /// <param name="disposing">True if called from Dispose; false if called from a finalizer.</param>
    private void Dispose(bool disposing)
    {
        if (disposing)
        {
            _fixedLengthReader.Dispose();
        }
    }

    #region NotSupported
    /// <inheritdoc />
    public string GetName(int i)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public string GetDataTypeName(int i)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public Type GetFieldType(int i) => typeof(string);

    /// <inheritdoc />
    public int GetValues(object[] values)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public bool GetBoolean(int i)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public byte GetByte(int i)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public char GetChar(int i)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public Guid GetGuid(int i)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public short GetInt16(int i)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public int GetInt32(int i)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public long GetInt64(int i)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public float GetFloat(int i)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public double GetDouble(int i)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public string GetString(int i)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public decimal GetDecimal(int i)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public DateTime GetDateTime(int i)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public IDataReader GetData(int i)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public bool IsDBNull(int i)
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public DataTable GetSchemaTable()
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public bool NextResult()
    {
        throw new NotSupportedException();
    }

    /// <inheritdoc />
    public bool Read()
    {
        while (_fixedLengthReader.Read())
        {
            if (_rowFilter(_fixedLengthReader))
            {
                return true;
            }
        }

        return false;
    }

    #endregion
}
