using System.Data;
using System.Globalization;
using System.Runtime.CompilerServices;
using FixedLengthHelper;
using Microsoft.Extensions.Configuration;
using SqlBulkCopier.Hosting;

namespace SqlBulkCopier.FixedLength.Hosting
{
    public static class FixedLengthBulkCopierParser
    {
        public const string DefaultSectionName = "SqlBulkCopier";

        public static IFixedLengthBulkCopierBuilder Parse(IConfiguration configuration, string sectionName = DefaultSectionName)
        {
            return BuildBuilder(configuration.GetRequiredSection(sectionName));
        }

        internal static IFixedLengthBulkCopierBuilder BuildBuilder(IConfigurationSection sqlBulkCopier)
        {
            var tableName = sqlBulkCopier.GetValue<string>("DestinationTableName");
            if (tableName is null)
            {
                throw new InvalidOperationException("DestinationTableName is not found or empty.");
            }

            var builder = FixedLengthBulkCopierBuilder.Create(tableName);
            builder.SetDefaultColumnContext(
                SetupContext(sqlBulkCopier.GetSection("DefaultColumnSettings")));

            builder.SetRowFilter(GetRowFilter(sqlBulkCopier));

            var columns = sqlBulkCopier.GetSection("Columns");
            foreach (var column in columns.GetChildren())
            {
                var offset = column.GetValue<int>("Offset");
                var length = column.GetValue<int>("Length");
                builder.AddColumnMapping(column.Key, offset, length, SetupContext(column));
            }

            return builder;
        }

        private static Action<IColumnContext> SetupContext(IConfigurationSection column)
        {
            var trimAction = TrimAction(column);
            var treatEmptyStringAsNullAction = TreatEmptyStringAsNullAction(column);
            var sqlDbTypeAction = SqlDbTypeAction(column);

            return c =>
            {
                trimAction(c);
                treatEmptyStringAsNullAction(c);
                sqlDbTypeAction(c);
            };
        }

        private static Predicate<IFixedLengthReader> GetRowFilter(IConfigurationSection sqlBulkCopier)
        {
            var rowFilter = sqlBulkCopier.GetSection("RowFilter");
            PredicateContext context = new(
                rowFilter.GetSection("StartsWith").Get<string[]>(),
                rowFilter.GetSection("Equals").Get<string[]>(),
                rowFilter.GetSection("EndsWith").Get<string[]>());
            return context.Predicate;
        }

        private class PredicateContext(
            string[]? startsWithStrings,
            string[]? equalsStrings,
            string[]? endsWithStrings)
        {
            private bool _isFirst = true;
            private readonly string[] _startsWithStrings = startsWithStrings ?? [];
            private readonly string[] _equalsStrings = equalsStrings ?? [];
            private readonly string[] _endsWithStrings = endsWithStrings ?? [];
            private byte[][] _startsWithByteArrays = [];
            private byte[][] _equalsByteArrays = [];
            private byte[][] _endsWithByteArrays = [];


            public bool Predicate(IFixedLengthReader reader)
            {
                if (_isFirst)
                {
                    var encoding = reader.Encoding;
                    _isFirst = false;
                    _startsWithByteArrays = _startsWithStrings.Select(s => encoding.GetBytes(s)).ToArray();
                    _equalsByteArrays = _equalsStrings.Select(s => encoding.GetBytes(s)).ToArray();
                    _endsWithByteArrays = _endsWithStrings.Select(s => encoding.GetBytes(s)).ToArray();
                }

                var currentRow = reader.CurrentRow;
                foreach (var startsWithByteArray in _startsWithByteArrays)
                {
                    if (currentRow.StartsWith(startsWithByteArray))
                    {
                        return false;
                    }
                }

                foreach (var equalsByteArray in _equalsByteArrays)
                {
                    if (currentRow.SequenceEqualsOptimized(equalsByteArray))
                    {
                        return false;
                    }
                }

                foreach (var endsWithByteArray in _endsWithByteArrays)
                {
                    if (currentRow.EndsWith(endsWithByteArray))
                    {
                        return false;
                    }
                }


                return true;
            }
        }

        private static Action<IColumnContext> TreatEmptyStringAsNullAction(IConfigurationSection column)
        {
            Action<IColumnContext> treatEmptyStringAsNullAction =
                column.GetValue<bool>("TreatEmptyStringAsNull")
                    ? c => c.TreatEmptyStringAsNull()
                    : _ => { };
            return treatEmptyStringAsNullAction;
        }

        private static Action<IColumnContext> TrimAction(IConfigurationSection column)
        {
            var trimMode = column.GetEnum("TrimMode", TrimMode.None);
            var trimChars = column.GetValue<string>("TrimChars")?.ToCharArray();
            Action<IColumnContext> trimAction = trimMode switch
            {
                TrimMode.None => _ => { }
                ,
                TrimMode.Trim => c => c.Trim(trimChars),
                TrimMode.TrimStart => c => c.TrimStart(trimChars),
                TrimMode.TrimEnd => c => c.TrimEnd(trimChars),
                _ => throw new ArgumentOutOfRangeException()
            };
            return trimAction;
        }

        private static Action<IColumnContext> SqlDbTypeAction(IConfigurationSection column)
        {
            var sqlDbType = column.GetEnum<SqlDbType>("SqlDbType");
            var numberStyles = column.GetEnum("NumberStyles", NumberStyles.None);
            var format = column.GetValue<string>("Format");
            var dateTimeStyles = column.GetEnum("DateTimeStyles", DateTimeStyles.None);
            var cultureInfo = column.GetCultureInfo("CultureInfo");
            Action<IColumnContext> sqlDbTypeAction = sqlDbType switch
            {
                null
                    or SqlDbType.Char
                    or SqlDbType.NChar
                    or SqlDbType.VarChar
                    or SqlDbType.NVarChar
                    or SqlDbType.Text
                    or SqlDbType.NText
                    or SqlDbType.Xml
                    => _ => { }
                ,
                SqlDbType.BigInt => c => c.AsBigInt(numberStyles, cultureInfo),
                SqlDbType.Binary => c => c.AsBinary(),
                SqlDbType.Bit => c => c.AsBit(),
                SqlDbType.Date => c => c.AsDate(format, cultureInfo, dateTimeStyles),
                SqlDbType.DateTime => c => c.AsDateTime(format, cultureInfo, dateTimeStyles),
                SqlDbType.DateTime2 => c => c.AsDateTime2(format, cultureInfo, dateTimeStyles),
                SqlDbType.DateTimeOffset => c => c.AsDateTimeOffset(format, cultureInfo, dateTimeStyles),
                SqlDbType.Decimal => c => c.AsDecimal(numberStyles, cultureInfo),
                SqlDbType.Float => c => c.AsFloat(numberStyles, cultureInfo),
                SqlDbType.Image => c => c.AsImage(),
                SqlDbType.Int => c => c.AsInt(numberStyles, cultureInfo),
                SqlDbType.Money => c => c.AsMoney(numberStyles, cultureInfo),
                SqlDbType.Real => c => c.AsReal(numberStyles, cultureInfo),
                SqlDbType.SmallDateTime => c => c.AsSmallDateTime(format, cultureInfo, dateTimeStyles),
                SqlDbType.SmallInt => c => c.AsSmallInt(numberStyles, cultureInfo),
                SqlDbType.SmallMoney => c => c.AsSmallMoney(numberStyles, cultureInfo),
                SqlDbType.Time => c => c.AsTime(format, cultureInfo, dateTimeStyles),
                SqlDbType.Timestamp => c => c.AsTimestamp(format, cultureInfo, dateTimeStyles),
                SqlDbType.TinyInt => c => c.AsTinyInt(numberStyles, cultureInfo),
                SqlDbType.UniqueIdentifier => c => c.AsUniqueIdentifier(),
                SqlDbType.VarBinary => c => c.AsVarBinary(),
                _ => throw new ArgumentOutOfRangeException()
            };
            return sqlDbTypeAction;
        }
    }
}