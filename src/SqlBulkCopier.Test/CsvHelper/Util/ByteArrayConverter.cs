using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.TypeConversion;

namespace SqlBulkCopier.Test.CsvHelper.Util;

// ReSharper disable once ClassNeverInstantiated.Global
public class ByteArrayConverter : ITypeConverter
{
    public object ConvertFromString(string? text, IReaderRow row, MemberMapData memberMapData)
    {
        return Convert.FromBase64String(text!);
    }

    public string ConvertToString(object? value, IWriterRow row, MemberMapData memberMapData)
    {
        return Convert.ToBase64String((byte[])value!);
    }
}