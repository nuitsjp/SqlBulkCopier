using System.Data;
using System.Globalization;

namespace SqlBulkCopier
{
    /// <summary>
    /// Column definition.
    /// </summary>
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
        public object Convert(string s)
        {
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

            if (ConvertValue is not null)
            {
                return ConvertValue(trimValue);
            }

            if (SqlDbType is not null)
            {
                return ConvertSqlDbType(trimValue);
            }

            if (TreatEmptyStringAsNull && string.IsNullOrEmpty(trimValue))
            {
                return DBNull.Value;
            }

            return trimValue;
        }

        private object ConvertSqlDbType(string trimValue)
        {
            if (string.IsNullOrEmpty(trimValue))
            {
                return DBNull.Value;
            }

            return SqlDbType switch
            {
                System.Data.SqlDbType.BigInt => NumberStyles is NumberStyles.None
                    ? long.Parse(trimValue, CultureInfo)
                    : long.Parse(trimValue, NumberStyles, CultureInfo),
                System.Data.SqlDbType.Binary
                    or System.Data.SqlDbType.VarBinary
                    or System.Data.SqlDbType.Image => System.Convert.FromBase64String(trimValue),
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
                System.Data.SqlDbType.Date
                    or System.Data.SqlDbType.DateTime
                    or System.Data.SqlDbType.SmallDateTime
                    or System.Data.SqlDbType.Timestamp
                    or System.Data.SqlDbType.DateTime2 => Format is null
                        ? DateTime.Parse(trimValue, CultureInfo, DateTimeStyle)
                        : DateTime.ParseExact(trimValue, Format, CultureInfo, DateTimeStyle),
                System.Data.SqlDbType.Decimal
                    or System.Data.SqlDbType.Money
                    or System.Data.SqlDbType.SmallMoney => NumberStyles is NumberStyles.None
                        ? decimal.Parse(trimValue, CultureInfo)
                        : decimal.Parse(trimValue, NumberStyles, CultureInfo),
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
                System.Data.SqlDbType.Time => Format is null
                    ? TimeSpan.Parse(trimValue, CultureInfo)
                    : TimeSpan.ParseExact(trimValue, Format, CultureInfo),
                System.Data.SqlDbType.DateTimeOffset => Format is null
                    ? DateTimeOffset.Parse(trimValue, CultureInfo)
                    : DateTimeOffset.ParseExact(trimValue, Format, CultureInfo),
                System.Data.SqlDbType.UniqueIdentifier => Guid.Parse(trimValue),
                _ => throw new InvalidOperationException($"Unsupported SqlDbType '{SqlDbType}'.")
            };
        }
    }
}