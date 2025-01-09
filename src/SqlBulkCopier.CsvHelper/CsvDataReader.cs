using System.Data;
using CsvHelper;

namespace SqlBulkCopier.CsvHelper;

public class CsvDataReader : IDataReader
{
    private readonly Dictionary<int, Column> _columns;

    private readonly CsvReader _csvReader;
    private readonly Predicate<CsvReader> _rowFilter;
    private bool _isDisposed;

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

    public int GetOrdinal(string name)
    {
        return _csvReader.GetFieldIndex(name);
    }

    public object GetValue(int i)
    {
        if (_columns.TryGetValue(i, out var column))
        {
            return column.Convert(_csvReader.GetField(i)!);
        }

        throw new IndexOutOfRangeException($"Field with ordinal '{i}' was not found.");
    }

    public bool Read()
    {
        while (_csvReader.Read())
        {
            if(_rowFilter(_csvReader))
            {
                return true;
            }
        }

        return false;
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

    private void Dispose(bool disposing)
    {
        if (!disposing) return;
        
        _csvReader.Dispose();
    }

    #region NotSupportedException
    public int Depth => throw new NotSupportedException();
    public bool IsClosed => throw new NotSupportedException();
    public int RecordsAffected => throw new NotSupportedException();

    public bool GetBoolean(int i)
    {
        throw new NotSupportedException();
    }

    public byte GetByte(int i)
    {
        throw new NotSupportedException();
    }

    public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
    {
        throw new NotSupportedException();
    }

    public char GetChar(int i)
    {
        throw new NotSupportedException();
    }

    public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
    {
        throw new NotSupportedException();
    }

    public IDataReader GetData(int i)
    {
        throw new NotSupportedException();
    }

    public string GetDataTypeName(int i)
    {
        throw new NotSupportedException();
    }

    public DateTime GetDateTime(int i)
    {
        throw new NotSupportedException();
    }

    public decimal GetDecimal(int i)
    {
        throw new NotSupportedException();
    }

    public double GetDouble(int i)
    {
        throw new NotSupportedException();
    }

    public Type GetFieldType(int i)
    {
        throw new NotSupportedException();
    }

    public float GetFloat(int i)
    {
        throw new NotSupportedException();
    }

    public Guid GetGuid(int i)
    {
        throw new NotSupportedException();
    }

    public short GetInt16(int i)
    {
        throw new NotSupportedException();
    }

    public int GetInt32(int i)
    {
        throw new NotSupportedException();
    }

    public long GetInt64(int i)
    {
        throw new NotSupportedException();
    }

    public string GetName(int i)
    {
        throw new NotSupportedException();
    }

    public string GetString(int i)
    {
        throw new NotSupportedException();
    }

    public int GetValues(object[] values)
    {
        throw new NotSupportedException();
    }

    public bool IsDBNull(int i)
    {
        throw new NotSupportedException();
    }

    public int FieldCount => _columns.Count;

    public object this[int i] => throw new NotSupportedException();

    public object this[string name] => throw new NotSupportedException();

    public DataTable GetSchemaTable()
    {
        throw new NotSupportedException();
    }

    public bool NextResult()
    {
        throw new NotSupportedException();
    }

    #endregion
}