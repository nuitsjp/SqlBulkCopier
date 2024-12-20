using FixedLengthHelper;

namespace SqlBulkCopier.FixedLength;

public class FixedLengthBulkCopierBuilder(
    string destinationTableName)
{
    public static FixedLengthBulkCopierBuilder CreateBuilder(string destinationTableName) => new(destinationTableName);

    private readonly FixedLengthDataReaderBuilder _fixedLengthDataReaderBuilder = new();
    
    private readonly Dictionary<string, Column> _columns = new();

    public FixedLengthBulkCopierBuilder AddColumnMapping(string dbColumnName, int offsetBytes, int lengthBytes)
        => AddColumnMapping(dbColumnName, offsetBytes, lengthBytes, TrimMode.None);
    public FixedLengthBulkCopierBuilder AddColumnMapping(string dbColumnName, int offsetBytes, int lengthBytes, Func<string, bool> isDbNull)
        => AddColumnMapping(dbColumnName, offsetBytes, lengthBytes, TrimMode.None, null, isDbNull);
    public FixedLengthBulkCopierBuilder AddColumnMapping(string dbColumnName, int offsetBytes, int lengthBytes, TrimMode trimMode, char[]? trimChars = null, bool isEmptyNull = false)
        => AddColumnMapping(dbColumnName, offsetBytes, lengthBytes, trimMode, trimChars, s => isEmptyNull && string.IsNullOrEmpty(s));

    public FixedLengthBulkCopierBuilder AddColumnMapping(string dbColumnName, int offsetBytes, int lengthBytes, TrimMode trimMode, char[]? trimChars, Func<string, bool> isDbNull)
    {
        _fixedLengthDataReaderBuilder.AddColumn(dbColumnName, offsetBytes, lengthBytes, trimMode, trimChars, isDbNull);
        var latestColumn = _fixedLengthDataReaderBuilder.Columns[^1];
        _columns[dbColumnName] = latestColumn;
        return this;
    }

    public ISqlBulkCopier Build()
    {
        return new FixedLengthBulkCopier(destinationTableName, _fixedLengthDataReaderBuilder, _columns);
    }

}