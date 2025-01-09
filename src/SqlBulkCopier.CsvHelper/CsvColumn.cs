using System.Data;
using System.Globalization;

namespace SqlBulkCopier.CsvHelper;

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