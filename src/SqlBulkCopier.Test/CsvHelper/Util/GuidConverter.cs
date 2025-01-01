using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.TypeConversion;

namespace SqlBulkCopier.Test.CsvHelper.Util
{
    // ReSharper disable once ClassNeverInstantiated.Global
    public class GuidConverter : ITypeConverter
    {
        public object ConvertFromString(string? text, IReaderRow row, MemberMapData memberMapData)
        {
            return Guid.Parse(text!);
        }
        public string ConvertToString(object? value, IWriterRow row, MemberMapData memberMapData)
        {
#if NET8_0_OR_GREATER
            return value!.ToString()!;
#else
        return value!.ToString();
#endif
        }
    }
}