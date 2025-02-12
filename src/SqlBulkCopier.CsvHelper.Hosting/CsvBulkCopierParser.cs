using System.Data;
using System.Globalization;
using Microsoft.Extensions.Configuration;

namespace SqlBulkCopier.CsvHelper.Hosting;

public static class CsvBulkCopierParser
{
    public const string DefaultSectionName = "SqlBulkCopier";

    public static IBulkCopierBuilder Parse(IConfiguration configuration, string sectionName = DefaultSectionName)
    {
        var sqlBulkCopier = configuration.GetRequiredSection(sectionName);
        var hasHeader = sqlBulkCopier.GetValue<bool>("HasHeader");
        return hasHeader
            ? ParseHasHeaderBuilder(sqlBulkCopier)
            : ParseNoHeaderBuilder(sqlBulkCopier);
    }

    internal static ICsvBulkCopierWithHeaderBuilder ParseHasHeaderBuilder(IConfigurationSection sqlBulkCopier)
    {
        var tableName = sqlBulkCopier.GetValue<string>("DestinationTableName");
        if (tableName is null)
        {
            throw new InvalidOperationException("DestinationTableName is not found or empty.");
        }

        var builder = CsvBulkCopierBuilder.CreateWithHeader(tableName);
        builder.SetDefaultColumnContext(
            SetupContext(sqlBulkCopier.GetSection("DefaultColumnSettings")));

        var columns = sqlBulkCopier.GetSection("Columns");
        foreach (var column in columns.GetChildren())
        {
            builder.AddColumnMapping(column.Key, SetupContext(column));
        }

        var truncateBeforeBulkInsert = sqlBulkCopier.GetValue<bool?>("TruncateBeforeBulkInsert") ?? false;
        builder.SetTruncateBeforeBulkInsert(truncateBeforeBulkInsert);

        var maxRetryCount = sqlBulkCopier.GetValue<int?>("MaxRetryCount") ?? 0;
        builder.SetMaxRetryCount(maxRetryCount);

        var initialDelay = sqlBulkCopier.GetValue<TimeSpan?>("InitialDelay") ?? TimeSpan.Zero;
        builder.SetInitialDelay(initialDelay);

        var useExponentialBackoff = sqlBulkCopier.GetValue<bool?>("UseExponentialBackoff") ?? false;
        builder.SetUseExponentialBackoff(useExponentialBackoff);

        var batchSize = sqlBulkCopier.GetValue<int?>("BatchSize") ?? 0;
        builder.SetBatchSize(batchSize);

        var notifyAfter = sqlBulkCopier.GetValue<int?>("NotifyAfter") ?? 0;
        builder.SetNotifyAfter(notifyAfter);

        var rowFilterSection = sqlBulkCopier.GetSection("RowFilter");
        if (rowFilterSection.Exists() && rowFilterSection.GetChildren().Any())
        {
            var startsWith = rowFilterSection.GetSection("StartsWith").Get<string[]>() ?? [];
            var equals = rowFilterSection.GetSection("Equals").Get<string[]>() ?? [];
            var endsWith = rowFilterSection.GetSection("EndsWith").Get<string[]>() ?? [];
            builder.SetRowFilter(row =>
            {
                var rowRecord = row.Parser.RawRecord;
                // If rowRecord ends with a newline code, remove the newline code.
                if (rowRecord.EndsWith("\r\n"))
                {
                    rowRecord = rowRecord.Substring(0, rowRecord.Length - 2);
                }
                if (rowRecord.EndsWith("\n"))
                {
                    rowRecord = rowRecord.Substring(0, rowRecord.Length - 1);
                }

                foreach (var prefix in startsWith)
                {
                    if (rowRecord.StartsWith(prefix))
                    {
                        return false;
                    }
                }
                foreach (var equal in equals)
                {
                    if (rowRecord == equal)
                    {
                        return false;
                    }
                }
                foreach (var suffix in endsWith)
                {
                    if (rowRecord.EndsWith(suffix))
                    {
                        return false;
                    }
                }
                return true;
            });
        }

        return builder;
    }

    internal static ICsvBulkCopierNoHeaderBuilder ParseNoHeaderBuilder(IConfigurationSection sqlBulkCopier)
    {
        var tableName = sqlBulkCopier.GetValue<string>("DestinationTableName");
        if (tableName is null)
        {
            throw new InvalidOperationException("DestinationTableName is not found or empty.");
        }

        var builder = CsvBulkCopierBuilder.CreateNoHeader(tableName);
        builder.SetDefaultColumnContext(
            SetupContext(sqlBulkCopier.GetSection("DefaultColumnSettings")));

        var columns = sqlBulkCopier.GetSection("Columns");
        foreach (var column in columns.GetChildren())
        {
            var ordinal = column.GetValue<int?>("Ordinal");
            if (ordinal is null)
            {
                throw new InvalidOperationException($"Column {column.Key} does not have Ordinal.");
            }
            builder.AddColumnMapping(column.Key, ordinal.Value, SetupContext(column));
        }

        var truncateBeforeBulkInsert = sqlBulkCopier.GetValue<bool?>("TruncateBeforeBulkInsert") ?? false;
        builder.SetTruncateBeforeBulkInsert(truncateBeforeBulkInsert);

        var maxRetryCount = sqlBulkCopier.GetValue<int?>("MaxRetryCount") ?? 0;
        builder.SetMaxRetryCount(maxRetryCount);

        var initialDelay = sqlBulkCopier.GetValue<TimeSpan?>("InitialDelay") ?? TimeSpan.Zero;
        builder.SetInitialDelay(initialDelay);

        var useExponentialBackoff = sqlBulkCopier.GetValue<bool?>("UseExponentialBackoff") ?? false;
        builder.SetUseExponentialBackoff(useExponentialBackoff);

        var batchSize = sqlBulkCopier.GetValue<int?>("BatchSize") ?? 0;
        builder.SetBatchSize(batchSize);

        var notifyAfter = sqlBulkCopier.GetValue<int?>("NotifyAfter") ?? 0;
        builder.SetNotifyAfter(notifyAfter);

        var rowFilterSection = sqlBulkCopier.GetSection("RowFilter");
        if (rowFilterSection.Exists() && rowFilterSection.GetChildren().Any())
        {
            var startsWith = rowFilterSection.GetSection("StartsWith").Get<string[]>() ?? [];
            var endsWith = rowFilterSection.GetSection("EndsWith").Get<string[]>() ?? [];
            builder.SetRowFilter(row =>
            {
                foreach (var prefix in startsWith)
                {
                    if (row.Parser.RawRecord.StartsWith("Header"))
                    {
                        return false;
                    }
                }
                foreach (var suffix in endsWith)
                {
                    if (row.Parser.RawRecord.StartsWith("Footer"))
                    {
                        return false;
                    }
                }
                return true;
            });
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