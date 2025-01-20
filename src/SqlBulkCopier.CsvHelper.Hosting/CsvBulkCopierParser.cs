using System.Data;
using System.Globalization;
using Microsoft.Extensions.Configuration;
using SqlBulkCopier.Hosting;

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