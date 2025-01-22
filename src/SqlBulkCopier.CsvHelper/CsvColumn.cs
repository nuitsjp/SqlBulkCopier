using System.Data;
using System.Globalization;

namespace SqlBulkCopier.CsvHelper;

/// <summary>
/// Represents a column in a CSV file with data conversion and formatting capabilities.
/// </summary>
/// <remarks>
/// This record extends the base Column class to provide CSV-specific column handling,
/// inheriting all the data type conversion and formatting functionality while
/// maintaining the same configuration options.
/// </remarks>
/// <param name="Ordinal">The zero-based position of the column in the CSV file.</param>
/// <param name="Name">The name of the column, used for header-based mapping.</param>
/// <param name="SqlDbType">The SQL Server data type for the destination column.</param>
/// <param name="NumberStyles">The number styles to use when parsing numeric values.</param>
/// <param name="DateTimeStyle">The date/time styles to use when parsing date/time values.</param>
/// <param name="Format">The format string for parsing formatted values.</param>
/// <param name="CultureInfo">The culture-specific formatting information.</param>
/// <param name="TrimMode">The type of trimming to apply to string values.</param>
/// <param name="TrimChars">The specific characters to trim, if any.</param>
/// <param name="TreatEmptyStringAsNull">Whether to convert empty strings to NULL.</param>
/// <param name="ConvertValue">Custom conversion function for special cases.</param>
public record CsvColumn(
    int Ordinal,
    string Name,
    SqlDbType? SqlDbType,
    NumberStyles NumberStyles,
    DateTimeStyles DateTimeStyle,
    string? Format,
    CultureInfo? CultureInfo,
    TrimMode TrimMode,
    char[]? TrimChars,
    bool TreatEmptyStringAsNull,
    Func<string, object>? ConvertValue)
    : Column(Ordinal, Name, SqlDbType, NumberStyles, DateTimeStyle, Format, CultureInfo, TrimMode, TrimChars, TreatEmptyStringAsNull, ConvertValue);