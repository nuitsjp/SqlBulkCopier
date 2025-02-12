using CsvHelper.Configuration;
using CsvHelper;
using SqlBulkCopier.CsvHelper;
using System.Globalization;
using System.Text;
using SqlBulkCopier.Test.CsvHelper.Util;

// ReSharper disable UseAwaitUsing

namespace SqlBulkCopier.Test.CsvHelper;

// ReSharper disable once UnusedMember.Global
public class CsvBulkCopierWithHeaderBuilderTest() : WriteToServerAsync<ICsvBulkCopierWithHeaderBuilder>
{
    public override WithRetryDataReaderBuilder CreateWithRetryDataReaderBuilder(ICsvBulkCopierWithHeaderBuilder builder, int retryCount) 
        => new(new CsvDataReaderBuilder(true, builder.BuildColumns(), _ => true), retryCount);

    /// <summary>
    /// 生成したデータを固定長でファイル出力(バイト数でパディング)する
    /// </summary>
    // ReSharper disable once UnusedMember.Local
    protected override async Task<Stream> CreateBulkInsertStreamAsync(List<BulkInsertTestTarget> dataList, bool withHeaderAndFooter = false)
    {
        var encoding = new UTF8Encoding(false);
        using var stream = new MemoryStream();
        // ReSharper disable once UseAwaitUsing
        using var writer = new StreamWriter(stream, encoding);
        var csvWriter = new CsvWriter(
            writer,
            new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true
            });
        csvWriter.Context.RegisterClassMap<BulkInsertTestTargetMap>();
        await csvWriter.WriteRecordsAsync(Targets, CancellationToken.None);
        await csvWriter.FlushAsync();

        if (withHeaderAndFooter == false)
        {
            // csvWriterなどを閉じると、streamも閉じられるため作り直す
            return new MemoryStream(stream.ToArray());
        }

        return new MemoryStream(
            encoding.GetBytes(
                $"""
                 Header
                 {encoding.GetString(stream.ToArray())}
                 Footer
                 """));
    }

    protected override ICsvBulkCopierWithHeaderBuilder ProvideBuilder(bool withRowFilter = false)
    {
        var builder = CsvBulkCopierBuilder
            .CreateWithHeader("[dbo].[BulkInsertTestTarget]")
            .SetDefaultColumnContext(c => c.TrimEnd().TreatEmptyStringAsNull())

            // GUID
            .AddColumnMapping("UniqueIdValue", c => c.AsUniqueIdentifier())

            // XML → そのまま文字列で受け取れる
            .AddColumnMapping("XmlValue")

            // ■ int?, decimal?, float?, string? などは自動変換が期待できるので、変換指定なし
            .AddColumnMapping("Id")
            .AddColumnMapping("TinyInt")
            .AddColumnMapping("SmallInt")
            .AddColumnMapping("IntValue")
            .AddColumnMapping("BigInt")

            // bit列は "0" or "1" → bool に明示変換が必要
            .AddColumnMapping("BitValue", c => c.AsBit())

            // decimal や float なども標準的な数値文字列であれば自動変換が可能
            .AddColumnMapping("DecimalValue")
            .AddColumnMapping("NumericValue")
            .AddColumnMapping("MoneyValue")
            .AddColumnMapping("SmallMoneyValue")
            .AddColumnMapping("FloatValue")
            .AddColumnMapping("RealValue")

            // 日付系：yyyyMMdd, yyyyMMddHHmmss などは SQLServer が自動認識しない場合が多い
            // よって、パーサーを指定
            .AddColumnMapping("DateValue", c => c.AsDate("yyyyMMdd"))
            .AddColumnMapping("DateTimeValue", c => c.AsDateTime("yyyyMMddHHmmss"))
            .AddColumnMapping("SmallDateTimeValue", c => c.AsSmallDateTime("yyyyMMddHHmmss"))
            .AddColumnMapping("DateTime2Value", c => c.AsDateTime2("yyyyMMddHHmmss"))

            // time: "HHmmss" として保存しているなら要手動パース
            .AddColumnMapping("TimeValue", c => c.AsTime(@"hh\:mm\:ss"))

            // datetimeoffset: "yyyyMMddHHmmss+09:00" など → 要パーサー
            .AddColumnMapping("DateTimeOffsetValue", c => c.AsDateTimeOffset("yyyyMMddHHmmK"))

            // 文字列系は何も指定しなければそのまま文字列としてマッピングされる（必要に応じて TrimEnd）
            .AddColumnMapping("CharValue")
            .AddColumnMapping("VarCharValue")
            .AddColumnMapping("NCharValue")
            .AddColumnMapping("NVarCharValue")

            // バイナリを Base64 で書き出しているなら、Convert.FromBase64String が必要
            // もし ASCII 文字列そのままなら変換不要
            .AddColumnMapping("BinaryValue", c => c.AsBinary())
            .AddColumnMapping("VarBinaryValue", c => c.AsVarBinary());
        if (withRowFilter)
        {
            builder.SetRowFilter(reader =>
            {
                if (reader.Parser.RawRecord.StartsWith("Header"))
                {
                    return false;
                }

                if (reader.Parser.RawRecord.StartsWith("Footer"))
                {
                    return false;
                }

                return true;
            });
        }
        return builder;
    }
}