using System.Data;
using System.Globalization;

namespace SqlBulkCopier.FixedLength
{
    /// <summary>
    /// Column definition.
    /// </summary>
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
}