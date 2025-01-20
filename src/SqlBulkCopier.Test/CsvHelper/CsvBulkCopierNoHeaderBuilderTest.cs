using System.Globalization;
using System.Text;
using CsvHelper.Configuration;
using CsvHelper;
using Shouldly;
using SqlBulkCopier.CsvHelper;
using SqlBulkCopier.FixedLength;
using SqlBulkCopier.Test.CsvHelper.Util;

// ReSharper disable UseAwaitUsing

namespace SqlBulkCopier.Test.CsvHelper;

public class CsvBulkCopierNoHeaderBuilderTest : WriteToServerAsync<ICsvBulkCopierNoHeaderBuilder>
{
    public override void SetDefaultColumnContext()
    {
        // Arrange
        const decimal expected = 1234567.89m;
        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierBuilder
            .Create("[dbo].[BulkInsertTestTarget]")
            .SetDefaultColumnContext(
                c => c
                    .TrimEnd(['x', 'y'])
                    .TreatEmptyStringAsNull()
                    .AsDecimal(NumberStyles.AllowThousands | NumberStyles.AllowDecimalPoint, CultureInfo.GetCultureInfo("de-DE")))
            .AddColumnMapping("First", 0, 1)
            .AddColumnMapping("Second", 1, 2, c => c.AsDecimal());
        var first = builder.Columns.First();
        var second = builder.Columns.Last();

        // Act & Assert
        first.Convert("1.234.567,89xy").ShouldBe(expected);
        second.Convert("1,234,567.89xy").ShouldBe(expected);
    }

    protected override void SetRowFilter(ICsvBulkCopierNoHeaderBuilder builder)
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

    protected override ICsvBulkCopierNoHeaderBuilder ProvideBuilder()
        => CsvBulkCopierBuilder
            .CreateNoHeader("[dbo].[BulkInsertTestTarget]")
            .SetDefaultColumnContext(c => c.TrimEnd().TreatEmptyStringAsNull())

            // GUID
            .AddColumnMapping("UniqueIdValue", 24, c => c.AsUniqueIdentifier())

            // XML → そのまま文字列で受け取れる
            .AddColumnMapping("XmlValue", 25)

            // ■ int?, decimal?, float?, string? などは自動変換が期待できるので、変換指定なし
            .AddColumnMapping("Id", 0)
            .AddColumnMapping("TinyInt", 1)
            .AddColumnMapping("SmallInt", 2)
            .AddColumnMapping("IntValue", 3)
            .AddColumnMapping("BigInt", 4)

            // bit列は "0" or "1" → bool に明示変換が必要
            .AddColumnMapping("BitValue", 5, c => c.AsBit())

            // decimal や float なども標準的な数値文字列であれば自動変換が可能
            .AddColumnMapping("DecimalValue", 6)
            .AddColumnMapping("NumericValue", 7)
            .AddColumnMapping("MoneyValue", 8)
            .AddColumnMapping("SmallMoneyValue", 9)
            .AddColumnMapping("FloatValue", 10)
            .AddColumnMapping("RealValue", 11)

            // 日付系：yyyyMMdd, yyyyMMddHHmmss などは SQLServer が自動認識しない場合が多い
            // よって、パーサーを指定
            .AddColumnMapping("DateValue", 12, c => c.AsDate("yyyyMMdd"))
            .AddColumnMapping("DateTimeValue", 13, c => c.AsDateTime("yyyyMMddHHmmss"))
            .AddColumnMapping("SmallDateTimeValue", 14, c => c.AsSmallDateTime("yyyyMMddHHmmss"))
            .AddColumnMapping("DateTime2Value", 15, c => c.AsDateTime2("yyyyMMddHHmmss"))

            // time: "HHmmss" として保存しているなら要手動パース
            .AddColumnMapping("TimeValue", 16, c => c.AsTime(@"hh\:mm\:ss"))

            // datetimeoffset: "yyyyMMddHHmmss+09:00" など → 要パーサー
            .AddColumnMapping("DateTimeOffsetValue", 17, c => c.AsDateTimeOffset("yyyyMMddHHmmK"))

            // 文字列系は何も指定しなければそのまま文字列としてマッピングされる（必要に応じて TrimEnd）
            .AddColumnMapping("CharValue", 18)
            .AddColumnMapping("VarCharValue", 19)
            .AddColumnMapping("NCharValue", 20)
            .AddColumnMapping("NVarCharValue", 21)

            // バイナリを Base64 で書き出しているなら、Convert.FromBase64String が必要
            // もし ASCII 文字列そのままなら変換不要
            .AddColumnMapping("BinaryValue", 22, c => c.AsBinary())
            .AddColumnMapping("VarBinaryValue", 23, c => c.AsVarBinary());

    /// <summary>
    /// 生成したデータを固定長でファイル出力(バイト数でパディング)する
    /// </summary>
    // ReSharper disable once UnusedMember.Local
    protected override async Task<Stream> CreateBulkInsertStreamAsync(List<BulkInsertTestTarget> dataList, bool withHeaderAndFooter = false)
    {
        var encoding = new UTF8Encoding(false);
        using var stream = new MemoryStream();
        using var writer = new StreamWriter(stream, encoding);
        var csvWriter = new CsvWriter(
            writer,
            new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = false // ヘッダー行を無効化
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
}