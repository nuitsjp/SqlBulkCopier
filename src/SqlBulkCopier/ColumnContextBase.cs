using System.Data;
using System.Globalization;

namespace SqlBulkCopier;

public abstract class ColumnContextBase : IColumnContext
{
    protected SqlDbType? SqlDbType;
    protected NumberStyles NumberStyles = NumberStyles.None;
    protected string? Format;
    protected DateTimeStyles DateTimeStyles = DateTimeStyles.None;
    protected CultureInfo? CultureInfo;
    protected TrimMode TrimMode = TrimMode.None;
    protected char[]? TrimChars;
    protected bool IsTreatEmptyStringAsNull;
    protected Func<string, object>? Converter;

    public IColumnContext AsBigInt(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null)
    {
        SqlDbType = System.Data.SqlDbType.BigInt;
        NumberStyles = numberStyles;
        CultureInfo = cultureInfo;
        return this;
    }

    public IColumnContext AsBit()
    {
        SqlDbType = System.Data.SqlDbType.Bit;
        return this;
    }

    public IColumnContext AsUniqueIdentifier()
    {
        SqlDbType = System.Data.SqlDbType.UniqueIdentifier;
        return this;
    }

    public IColumnContext AsDate(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles dateTimeStyles = DateTimeStyles.None)
    {
        SqlDbType = System.Data.SqlDbType.Date;
        Format = format;
        CultureInfo = cultureInfo;
        DateTimeStyles = dateTimeStyles;
        return this;
    }

    public IColumnContext AsDateTime(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles dateTimeStyles = DateTimeStyles.None)
    {
        SqlDbType = System.Data.SqlDbType.DateTime;
        Format = format;
        CultureInfo = cultureInfo;
        DateTimeStyles = dateTimeStyles;
        return this;
    }

    public IColumnContext AsDecimal(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null)
    {
        SqlDbType = System.Data.SqlDbType.Decimal;
        NumberStyles = numberStyles;
        CultureInfo = cultureInfo;
        return this;
    }

    public IColumnContext AsFloat(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null)
    {
        SqlDbType = System.Data.SqlDbType.Float;
        NumberStyles = numberStyles;
        CultureInfo = cultureInfo;
        return this;
    }

    public IColumnContext AsInt(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null)
    {
        SqlDbType = System.Data.SqlDbType.Int;
        NumberStyles = numberStyles;
        CultureInfo = cultureInfo;
        return this;
    }

    public IColumnContext AsMoney(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null)
    {
        SqlDbType = System.Data.SqlDbType.Money;
        NumberStyles = numberStyles;
        CultureInfo = cultureInfo;
        return this;
    }

    public IColumnContext AsReal(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null)
    {
        SqlDbType = System.Data.SqlDbType.Real;
        NumberStyles = numberStyles;
        CultureInfo = cultureInfo;
        return this;
    }

    public IColumnContext AsSmallDateTime(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles dateTimeStyles = DateTimeStyles.None)
    {
        SqlDbType = System.Data.SqlDbType.SmallDateTime;
        Format = format;
        CultureInfo = cultureInfo;
        DateTimeStyles = dateTimeStyles;
        return this;
    }

    public IColumnContext AsSmallInt(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null)
    {
        SqlDbType = System.Data.SqlDbType.SmallInt;
        NumberStyles = numberStyles;
        CultureInfo = cultureInfo;
        return this;
    }

    public IColumnContext AsSmallMoney(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null)
    {
        SqlDbType = System.Data.SqlDbType.SmallMoney;
        NumberStyles = numberStyles;
        CultureInfo = cultureInfo;
        return this;
    }

    public IColumnContext AsTimestamp(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles style = DateTimeStyles.None)
    {
        SqlDbType = System.Data.SqlDbType.Timestamp;
        Format = format;
        CultureInfo = cultureInfo;
        DateTimeStyles = style;
        return this;
    }

    public IColumnContext AsTinyInt(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null)
    {
        SqlDbType = System.Data.SqlDbType.TinyInt;
        NumberStyles = numberStyles;
        CultureInfo = cultureInfo;
        return this;
    }

    public IColumnContext AsDateTime2(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles dateTimeStyles = DateTimeStyles.None)
    {
        SqlDbType = System.Data.SqlDbType.DateTime2;
        Format = format;
        CultureInfo = cultureInfo;
        DateTimeStyles = dateTimeStyles;
        return this;
    }

    public IColumnContext AsTime(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles dateTimeStyles = DateTimeStyles.None)
    {
        SqlDbType = System.Data.SqlDbType.Time;
        Format = format;
        CultureInfo = cultureInfo;
        DateTimeStyles = dateTimeStyles;
        return this;
    }

    public IColumnContext AsDateTimeOffset(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles dateTimeStyles = DateTimeStyles.None)
    {
        SqlDbType = System.Data.SqlDbType.DateTimeOffset;
        Format = format;
        CultureInfo = cultureInfo;
        DateTimeStyles = dateTimeStyles;
        return this;
    }

    public IColumnContext AsBinary()
    {
        SqlDbType = System.Data.SqlDbType.Binary;
        return this;
    }

    public IColumnContext AsVarBinary()
    {
        SqlDbType = System.Data.SqlDbType.VarBinary;
        return this;
    }

    public IColumnContext AsImage()
    {
        SqlDbType = System.Data.SqlDbType.Image;
        return this;
    }

    public IColumnContext Trim(char[]? trimChars = null)
    {
        TrimMode = TrimMode.Trim;
        TrimChars = trimChars;
        return this;
    }

    public IColumnContext TrimStart(char[]? trimChars = null)
    {
        TrimMode = TrimMode.TrimStart;
        TrimChars = trimChars;
        return this;
    }

    public IColumnContext TrimEnd(char[]? trimChars = null)
    {
        TrimMode = TrimMode.TrimEnd;
        TrimChars = trimChars;
        return this;
    }

    public IColumnContext TreatEmptyStringAsNull()
    {
        IsTreatEmptyStringAsNull = true;
        return this;
    }

    public IColumnContext Convert(Func<string, object> convert)
    {
        Converter = convert;
        return this;
    }

    public abstract Column Build();
}