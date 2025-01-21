using System.Data;
using System.Globalization;

namespace SqlBulkCopier;

/// <summary>
/// Provides a base implementation for configuring column contexts with a fluent interface.
/// Supports configuration of data type conversion, formatting, and string handling options.
/// </summary>
/// <param name="ordinal">The zero-based position of the column in the data source.</param>
/// <param name="name">The name of the column.</param>
public abstract class ColumnContextBase(int ordinal, string name) : IColumnContext
{
    /// <summary>
    /// Gets the zero-based position of the column in the data source.
    /// </summary>
    public int Ordinal { get; } = ordinal;

    /// <summary>
    /// Gets or sets the SQL Server data type of the column.
    /// </summary>
    public SqlDbType? SqlDbType { get; private set; }

    /// <summary>
    /// Gets or sets the format string used for parsing date, time, or custom formatted values.
    /// </summary>
    public string? Format { get; private set; }

    /// <summary>
    /// Gets or sets the style elements that can be present in numeric string values.
    /// </summary>
    protected NumberStyles NumberStyles = NumberStyles.None;

    /// <summary>
    /// Gets or sets the formatting options for parsing date and time strings.
    /// </summary>
    protected DateTimeStyles DateTimeStyles = DateTimeStyles.None;

    /// <summary>
    /// Gets or sets the culture-specific formatting information to use during parsing.
    /// </summary>
    protected CultureInfo? CultureInfo;

    /// <summary>
    /// Gets or sets how whitespace should be trimmed from input strings.
    /// </summary>
    protected TrimMode TrimMode = TrimMode.None;

    /// <summary>
    /// Gets or sets the set of characters to remove when trimming.
    /// </summary>
    protected char[]? TrimChars;

    /// <summary>
    /// Gets or sets whether empty strings should be converted to NULL values.
    /// </summary>
    protected bool IsTreatEmptyStringAsNull;

    /// <summary>
    /// Gets or sets the custom conversion function for string values.
    /// </summary>
    protected Func<string, object>? Converter;

    /// <summary>
    /// Gets the name of the column.
    /// </summary>
    public string Name { get; } = name;

    #region Numeric Type Configuration Methods

    /// <summary>
    /// Configures the column as a SQL BIGINT type.
    /// </summary>
    /// <param name="numberStyles">Defines the style elements that can be present in the string.</param>
    /// <param name="cultureInfo">The culture-specific formatting information.</param>
    public IColumnContext AsBigInt(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null)
    {
        SqlDbType = System.Data.SqlDbType.BigInt;
        NumberStyles = numberStyles;
        CultureInfo = cultureInfo;
        return this;
    }

    /// <summary>
    /// Configures the column as a SQL BIT type.
    /// Accepts "1"/"0", "True"/"False" (case insensitive) as valid values.
    /// </summary>
    public IColumnContext AsBit()
    {
        SqlDbType = System.Data.SqlDbType.Bit;
        return this;
    }

    /// <summary>
    /// Configures the column as a SQL UNIQUEIDENTIFIER type.
    /// </summary>
    public IColumnContext AsUniqueIdentifier()
    {
        SqlDbType = System.Data.SqlDbType.UniqueIdentifier;
        return this;
    }

    #endregion

    #region DateTime Type Configuration Methods

    /// <summary>
    /// Configures the column as a SQL DATE type.
    /// </summary>
    /// <param name="format">The format string for parsing date strings.</param>
    /// <param name="cultureInfo">The culture-specific formatting information.</param>
    /// <param name="dateTimeStyles">The formatting options for parsing date strings.</param>
    public IColumnContext AsDate(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles dateTimeStyles = DateTimeStyles.None)
    {
        SqlDbType = System.Data.SqlDbType.Date;
        Format = format;
        CultureInfo = cultureInfo;
        DateTimeStyles = dateTimeStyles;
        return this;
    }

    /// <summary>
    /// Configures the column as a SQL DATETIME type.
    /// </summary>
    /// <param name="format">The format string for parsing datetime strings.</param>
    /// <param name="cultureInfo">The culture-specific formatting information.</param>
    /// <param name="dateTimeStyles">The formatting options for parsing datetime strings.</param>
    public IColumnContext AsDateTime(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles dateTimeStyles = DateTimeStyles.None)
    {
        SqlDbType = System.Data.SqlDbType.DateTime;
        Format = format;
        CultureInfo = cultureInfo;
        DateTimeStyles = dateTimeStyles;
        return this;
    }

    #endregion

    #region Decimal Type Configuration Methods

    /// <summary>
    /// Configures the column as a SQL DECIMAL type.
    /// </summary>
    /// <param name="numberStyles">Defines the style elements that can be present in the string.</param>
    /// <param name="cultureInfo">The culture-specific formatting information.</param>
    public IColumnContext AsDecimal(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null)
    {
        SqlDbType = System.Data.SqlDbType.Decimal;
        NumberStyles = numberStyles;
        CultureInfo = cultureInfo;
        return this;
    }

    /// <summary>
    /// Configures the column as a SQL FLOAT type.
    /// </summary>
    /// <param name="numberStyles">Defines the style elements that can be present in the string.</param>
    /// <param name="cultureInfo">The culture-specific formatting information.</param>
    public IColumnContext AsFloat(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null)
    {
        SqlDbType = System.Data.SqlDbType.Float;
        NumberStyles = numberStyles;
        CultureInfo = cultureInfo;
        return this;
    }

    /// <summary>
    /// Configures the column as a SQL INT type.
    /// </summary>
    /// <param name="numberStyles">Defines the style elements that can be present in the string.</param>
    /// <param name="cultureInfo">The culture-specific formatting information.</param>
    public IColumnContext AsInt(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null)
    {
        SqlDbType = System.Data.SqlDbType.Int;
        NumberStyles = numberStyles;
        CultureInfo = cultureInfo;
        return this;
    }

    #endregion

    #region Money Type Configuration Methods

    /// <summary>
    /// Configures the column as a SQL MONEY type.
    /// </summary>
    /// <param name="numberStyles">Defines the style elements that can be present in the string.</param>
    /// <param name="cultureInfo">The culture-specific formatting information.</param>
    public IColumnContext AsMoney(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null)
    {
        SqlDbType = System.Data.SqlDbType.Money;
        NumberStyles = numberStyles;
        CultureInfo = cultureInfo;
        return this;
    }

    /// <summary>
    /// Configures the column as a SQL REAL type.
    /// </summary>
    /// <param name="numberStyles">Defines the style elements that can be present in the string.</param>
    /// <param name="cultureInfo">The culture-specific formatting information.</param>
    public IColumnContext AsReal(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null)
    {
        SqlDbType = System.Data.SqlDbType.Real;
        NumberStyles = numberStyles;
        CultureInfo = cultureInfo;
        return this;
    }

    #endregion

    #region Small Type Configuration Methods

    /// <summary>
    /// Configures the column as a SQL SMALLDATETIME type.
    /// </summary>
    /// <param name="format">The format string for parsing datetime strings.</param>
    /// <param name="cultureInfo">The culture-specific formatting information.</param>
    /// <param name="dateTimeStyles">The formatting options for parsing datetime strings.</param>
    public IColumnContext AsSmallDateTime(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles dateTimeStyles = DateTimeStyles.None)
    {
        SqlDbType = System.Data.SqlDbType.SmallDateTime;
        Format = format;
        CultureInfo = cultureInfo;
        DateTimeStyles = dateTimeStyles;
        return this;
    }

    /// <summary>
    /// Configures the column as a SQL SMALLINT type.
    /// </summary>
    /// <param name="numberStyles">Defines the style elements that can be present in the string.</param>
    /// <param name="cultureInfo">The culture-specific formatting information.</param>
    public IColumnContext AsSmallInt(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null)
    {
        SqlDbType = System.Data.SqlDbType.SmallInt;
        NumberStyles = numberStyles;
        CultureInfo = cultureInfo;
        return this;
    }

    /// <summary>
    /// Configures the column as a SQL SMALLMONEY type.
    /// </summary>
    /// <param name="numberStyles">Defines the style elements that can be present in the string.</param>
    /// <param name="cultureInfo">The culture-specific formatting information.</param>
    public IColumnContext AsSmallMoney(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null)
    {
        SqlDbType = System.Data.SqlDbType.SmallMoney;
        NumberStyles = numberStyles;
        CultureInfo = cultureInfo;
        return this;
    }

    #endregion

    #region Time Type Configuration Methods

    /// <summary>
    /// Configures the column as a SQL TIMESTAMP type.
    /// </summary>
    /// <param name="format">The format string for parsing timestamp strings.</param>
    /// <param name="cultureInfo">The culture-specific formatting information.</param>
    /// <param name="style">The formatting options for parsing timestamp strings.</param>
    public IColumnContext AsTimestamp(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles style = DateTimeStyles.None)
    {
        SqlDbType = System.Data.SqlDbType.Timestamp;
        Format = format;
        CultureInfo = cultureInfo;
        DateTimeStyles = style;
        return this;
    }

    /// <summary>
    /// Configures the column as a SQL TINYINT type.
    /// </summary>
    /// <param name="numberStyles">Defines the style elements that can be present in the string.</param>
    /// <param name="cultureInfo">The culture-specific formatting information.</param>
    public IColumnContext AsTinyInt(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null)
    {
        SqlDbType = System.Data.SqlDbType.TinyInt;
        NumberStyles = numberStyles;
        CultureInfo = cultureInfo;
        return this;
    }

    /// <summary>
    /// Configures the column as a SQL DATETIME2 type.
    /// </summary>
    /// <param name="format">The format string for parsing datetime strings.</param>
    /// <param name="cultureInfo">The culture-specific formatting information.</param>
    /// <param name="dateTimeStyles">The formatting options for parsing datetime strings.</param>
    public IColumnContext AsDateTime2(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles dateTimeStyles = DateTimeStyles.None)
    {
        SqlDbType = System.Data.SqlDbType.DateTime2;
        Format = format;
        CultureInfo = cultureInfo;
        DateTimeStyles = dateTimeStyles;
        return this;
    }

    /// <summary>
    /// Configures the column as a SQL TIME type.
    /// </summary>
    /// <param name="format">The format string for parsing time strings.</param>
    /// <param name="cultureInfo">The culture-specific formatting information.</param>
    /// <param name="dateTimeStyles">The formatting options for parsing time strings.</param>
    public IColumnContext AsTime(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles dateTimeStyles = DateTimeStyles.None)
    {
        SqlDbType = System.Data.SqlDbType.Time;
        Format = format;
        CultureInfo = cultureInfo;
        DateTimeStyles = dateTimeStyles;
        return this;
    }

    /// <summary>
    /// Configures the column as a SQL DATETIMEOFFSET type.
    /// </summary>
    /// <param name="format">The format string for parsing datetime offset strings.</param>
    /// <param name="cultureInfo">The culture-specific formatting information.</param>
    /// <param name="dateTimeStyles">The formatting options for parsing datetime offset strings.</param>
    public IColumnContext AsDateTimeOffset(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles dateTimeStyles = DateTimeStyles.None)
    {
        SqlDbType = System.Data.SqlDbType.DateTimeOffset;
        Format = format;
        CultureInfo = cultureInfo;
        DateTimeStyles = dateTimeStyles;
        return this;
    }

    #endregion

    #region Binary Type Configuration Methods

    /// <summary>
    /// Configures the column as a SQL BINARY type.
    /// </summary>
    public IColumnContext AsBinary()
    {
        SqlDbType = System.Data.SqlDbType.Binary;
        return this;
    }

    /// <summary>
    /// Configures the column as a SQL VARBINARY type.
    /// </summary>
    public IColumnContext AsVarBinary()
    {
        SqlDbType = System.Data.SqlDbType.VarBinary;
        return this;
    }

    /// <summary>
    /// Configures the column as a SQL IMAGE type.
    /// </summary>
    public IColumnContext AsImage()
    {
        SqlDbType = System.Data.SqlDbType.Image;
        return this;
    }

    #endregion

    #region String Handling Configuration Methods

    /// <summary>
    /// Configures the column to trim whitespace or specified characters from both ends of the input string.
    /// </summary>
    /// <param name="trimChars">The characters to trim. If null, trims whitespace.</param>
    public IColumnContext Trim(char[]? trimChars = null)
    {
        TrimMode = TrimMode.Trim;
        TrimChars = trimChars;
        return this;
    }

    /// <summary>
    /// Configures the column to trim whitespace or specified characters from the start of the input string.
    /// </summary>
    /// <param name="trimChars">The characters to trim. If null, trims whitespace.</param>
    public IColumnContext TrimStart(char[]? trimChars = null)
    {
        TrimMode = TrimMode.TrimStart;
        TrimChars = trimChars;
        return this;
    }

    /// <summary>
    /// Configures the column to trim whitespace or specified characters from the end of the input string.
    /// </summary>
    /// <param name="trimChars">The characters to trim. If null, trims whitespace.</param>
    public IColumnContext TrimEnd(char[]? trimChars = null)
    {
        TrimMode = TrimMode.TrimEnd;
        TrimChars = trimChars;
        return this;
    }

    /// <summary>
    /// Configures the column to treat empty strings as NULL values in the database.
    /// </summary>
    /// <returns>The current column context instance.</returns>
    public IColumnContext TreatEmptyStringAsNull()
    {
        IsTreatEmptyStringAsNull = true;
        return this;
    }

    #endregion

    /// <summary>
    /// Sets a custom conversion function for the column's values.
    /// </summary>
    /// <param name="convert">A function that takes a string input and returns the converted object.</param>
    /// <returns>The current column context instance.</returns>
    public IColumnContext Convert(Func<string, object> convert)
    {
        Converter = convert;
        return this;
    }

    /// <summary>
    /// Builds a Column instance with the configured settings.
    /// </summary>
    /// <param name="setDefaultContext">Action to apply default context settings before building.</param>
    /// <returns>A new Column instance with the specified configuration.</returns>
    public abstract Column Build(Action<IColumnContext> setDefaultContext);
}