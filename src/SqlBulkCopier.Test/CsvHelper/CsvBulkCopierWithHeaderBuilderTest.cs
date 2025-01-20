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

public class CsvBulkCopierWithHeaderBuilderTest() : CsvBulkCopierBuilderTest<ICsvBulkCopierWithHeaderBuilder>(true)
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

    protected override void SetRowFilter(ICsvBulkCopierWithHeaderBuilder builder)
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

    protected override ICsvBulkCopierWithHeaderBuilder ProvideBuilder()
        => CsvBulkCopierBuilder
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
}