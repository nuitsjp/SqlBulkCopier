using System.Data;
using System.Globalization;

namespace SqlBulkCopier;

/// <summary>
/// Defines a column structure for SQL bulk copy operations with conversion capabilities.
/// </summary>
/// <param name="Ordinal">The zero-based position of the column in the data source.</param>
/// <param name="Name">The name of the column.</param>
/// <param name="SqlDbType">The SQL Server data type of the column. If null, treats data as string.</param>
/// <param name="NumberStyles">Defines the style elements that can be present in a numeric string.</param>
/// <param name="DateTimeStyle">Defines the formatting options for parsing date and time strings.</param>
/// <param name="Format">The format string for parsing date, time, or custom formatted values.</param>
/// <param name="CultureInfo">The culture-specific formatting information to use during parsing.</param>
/// <param name="TrimMode">Specifies how whitespace should be trimmed from the input string.</param>
/// <param name="TrimChars">The set of characters to remove when trimming. If null, removes whitespace.</param>
/// <param name="TreatEmptyStringAsNull">If true, converts empty strings to DBNull.Value.</param>
/// <param name="ConvertValue">Custom function for converting string values to the target type.</param>
public abstract record Column(
    int Ordinal,
    string Name,
    SqlDbType? SqlDbType = null,
    NumberStyles NumberStyles = NumberStyles.None,
    DateTimeStyles DateTimeStyle = DateTimeStyles.None,
    string? Format = null,
    CultureInfo? CultureInfo = null,
    TrimMode TrimMode = TrimMode.None,
    char[]? TrimChars = null,
    bool TreatEmptyStringAsNull = false,
    Func<string, object>? ConvertValue = null)
{
    /// <summary>
    /// Converts a string value to the appropriate type based on the column's configuration.
    /// </summary>
    /// <param name="s">The string value to convert.</param>
    /// <returns>The converted value as an object, or DBNull.Value for null/empty strings when appropriate.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when TrimMode has an invalid value.</exception>
    /// <exception cref="InvalidCastException">Thrown when a string cannot be converted to the specified type.</exception>
    /// <exception cref="InvalidOperationException">Thrown when conversion is attempted for an unsupported SqlDbType.</exception>
    public object Convert(string s)
    {
        // Apply the specified trimming operation to the input string
        var trimValue = TrimMode switch
        {
            TrimMode.None => s,
            TrimMode.Trim => s.Trim(TrimChars),
            TrimMode.TrimStart => s.TrimStart(TrimChars),
            TrimMode.TrimEnd => s.TrimEnd(TrimChars),
#pragma warning disable CA2208
            _ => throw new ArgumentOutOfRangeException()
#pragma warning restore CA2208
        };

        // Use custom conversion function if provided
        if (ConvertValue is not null)
        {
            return ConvertValue(trimValue);
        }

        // Use SqlDbType-based conversion if specified
        if (SqlDbType is not null)
        {
            return ConvertSqlDbType(trimValue);
        }

        // Handle empty strings based on configuration
        if (TreatEmptyStringAsNull && string.IsNullOrEmpty(trimValue))
        {
            return DBNull.Value;
        }

        // Return the trimmed string as-is if no other conversion applies
        return trimValue;
    }

    /// <summary>
    /// Converts a string value to the specified SQL Server data type.
    /// </summary>
    /// <param name="trimValue">The pre-trimmed string value to convert.</param>
    /// <returns>The converted value as an object, or DBNull.Value for null/empty strings.</returns>
    /// <exception cref="InvalidCastException">Thrown when the string cannot be converted to the specified type.</exception>
    /// <exception cref="InvalidOperationException">Thrown when conversion is attempted for an unsupported SqlDbType.</exception>
    private object ConvertSqlDbType(string trimValue)
    {
        // Return DBNull for empty strings to maintain SQL NULL semantics
        if (string.IsNullOrEmpty(trimValue))
        {
            return DBNull.Value;
        }

        return SqlDbType switch
        {
            // Integer types
            System.Data.SqlDbType.BigInt => NumberStyles is NumberStyles.None
                ? long.Parse(trimValue, CultureInfo)
                : long.Parse(trimValue, NumberStyles, CultureInfo),

            // Binary types
            System.Data.SqlDbType.Binary
                or System.Data.SqlDbType.VarBinary
                or System.Data.SqlDbType.Image => System.Convert.FromBase64String(trimValue),

            // Boolean type with various string representations
            System.Data.SqlDbType.Bit => trimValue switch
            {
                "1" => true,
                "0" => false,
                "True" => true,
                "False" => false,
                "true" => true,
                "false" => false,
                "TRUE" => true,
                "FALSE" => false,
                _ => throw new InvalidCastException($"Cannot convert '{trimValue}' to Bit.")
            },

            // Date and time types
            System.Data.SqlDbType.Date
                or System.Data.SqlDbType.DateTime
                or System.Data.SqlDbType.SmallDateTime
                or System.Data.SqlDbType.Timestamp
                or System.Data.SqlDbType.DateTime2 => Format is null
                    ? DateTime.Parse(trimValue, CultureInfo, DateTimeStyle)
                    : DateTime.ParseExact(trimValue, Format, CultureInfo, DateTimeStyle),

            // Decimal types
            System.Data.SqlDbType.Decimal
                or System.Data.SqlDbType.Money
                or System.Data.SqlDbType.SmallMoney => NumberStyles is NumberStyles.None
                    ? decimal.Parse(trimValue, CultureInfo)
                    : decimal.Parse(trimValue, NumberStyles, CultureInfo),

            // Floating-point types
            System.Data.SqlDbType.Float => NumberStyles is NumberStyles.None
                ? float.Parse(trimValue, CultureInfo)
                : float.Parse(trimValue, NumberStyles, CultureInfo),

            System.Data.SqlDbType.Int => NumberStyles is NumberStyles.None
                ? int.Parse(trimValue, CultureInfo)
                : int.Parse(trimValue, NumberStyles, CultureInfo),

            System.Data.SqlDbType.Real => NumberStyles is NumberStyles.None
                ? double.Parse(trimValue, CultureInfo)
                : double.Parse(trimValue, NumberStyles, CultureInfo),

            System.Data.SqlDbType.SmallInt => NumberStyles is NumberStyles.None
                ? short.Parse(trimValue, CultureInfo)
                : short.Parse(trimValue, NumberStyles, CultureInfo),

            System.Data.SqlDbType.TinyInt => NumberStyles is NumberStyles.None
                ? byte.Parse(trimValue, CultureInfo)
                : byte.Parse(trimValue, NumberStyles, CultureInfo),

            // Time-related types
            System.Data.SqlDbType.Time => Format is null
                ? TimeSpan.Parse(trimValue, CultureInfo)
                : TimeSpan.ParseExact(trimValue, Format, CultureInfo),

            System.Data.SqlDbType.DateTimeOffset => Format is null
                ? DateTimeOffset.Parse(trimValue, CultureInfo)
                : DateTimeOffset.ParseExact(trimValue, Format, CultureInfo),

            // GUID type
            System.Data.SqlDbType.UniqueIdentifier => Guid.Parse(trimValue),

            _ => throw new InvalidOperationException($"Unsupported SqlDbType '{SqlDbType}'.")
        };
    }
}