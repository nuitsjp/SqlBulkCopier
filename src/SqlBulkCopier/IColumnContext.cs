using System.Globalization;

namespace SqlBulkCopier
{
    public interface IColumnContext
    {
        IColumnContext AsBigInt(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null);
        IColumnContext AsBit();
        IColumnContext AsUniqueIdentifier();
        IColumnContext AsDate(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles dateTimeStyles = DateTimeStyles.None);
        IColumnContext AsDateTime(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles dateTimeStyles = DateTimeStyles.None);
        IColumnContext AsDecimal(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null);
        IColumnContext AsFloat(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null);
        IColumnContext AsInt(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null);
        IColumnContext AsMoney(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null);
        IColumnContext AsReal(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null);
        IColumnContext AsSmallDateTime(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles dateTimeStyles = DateTimeStyles.None);
        IColumnContext AsSmallInt(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null);
        IColumnContext AsSmallMoney(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null);
        IColumnContext AsTimestamp(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles style = DateTimeStyles.None);
        IColumnContext AsTinyInt(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null);
        IColumnContext AsDateTime2(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles dateTimeStyles = DateTimeStyles.None);
        IColumnContext AsTime(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles dateTimeStyles = DateTimeStyles.None);
        IColumnContext AsDateTimeOffset(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles dateTimeStyles = DateTimeStyles.None);
        IColumnContext AsBinary();
        IColumnContext AsVarBinary();
        IColumnContext AsImage();
        IColumnContext Trim(char[]? trimChars = null);
        IColumnContext TrimStart(char[]? trimChars = null);
        IColumnContext TrimEnd(char[]? trimChars = null);
        IColumnContext TreatEmptyStringAsNull();
        IColumnContext Convert(Func<string, object> convert);
        Column Build();
    }
}