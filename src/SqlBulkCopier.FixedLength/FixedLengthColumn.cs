using System.Data;
using System.Globalization;

namespace SqlBulkCopier.FixedLength;

/// <summary>
/// Represents a column definition for fixed-length file processing.
/// Inherits from the Column class and adds properties specific to fixed-length columns.
/// </summary>
/// <param name="Ordinal">The zero-based ordinal position of the column.</param>
/// <param name="Name">The name of the column.</param>
/// <param name="OffsetBytes">The zero-based offset in bytes of the column in the fixed-length file.</param>
/// <param name="LengthBytes">The length in bytes of the column in the fixed-length file.</param>
/// <param name="SqlDbType">The SQL data type of the column. Optional.</param>
/// <param name="NumberStyles">The number styles to use for parsing numeric values. Optional.</param>
/// <param name="DateTimeStyle">The date and time styles to use for parsing date and time values. Optional.</param>
/// <param name="Format">The format string to use for parsing and formatting values. Optional.</param>
/// <param name="CultureInfo">The culture info to use for parsing and formatting values. Optional.</param>
/// <param name="TrimMode">The trim mode to apply to string values. Optional.</param>
/// <param name="TrimChars">The characters to trim from string values. Optional.</param>
/// <param name="TreatEmptyStringAsNull">Indicates whether empty strings should be treated as null. Optional.</param>
/// <param name="ConvertValue">A custom conversion function for the column's values. Optional.</param>
public record FixedLengthColumn(
    int Ordinal,
    string Name,
    int OffsetBytes,
    int LengthBytes,
    SqlDbType? SqlDbType = null,
    NumberStyles NumberStyles = NumberStyles.None,
    DateTimeStyles DateTimeStyle = DateTimeStyles.None,
    string? Format = null,
    CultureInfo? CultureInfo = null,
    TrimMode TrimMode = TrimMode.None,
    char[]? TrimChars = null,
    bool TreatEmptyStringAsNull = false,
    Func<string, object>? ConvertValue = null)
    : Column(
        Ordinal,
        Name,
        SqlDbType,
        NumberStyles,
        DateTimeStyle,
        Format,
        CultureInfo,
        TrimMode,
        TrimChars,
        TreatEmptyStringAsNull,
        ConvertValue);
