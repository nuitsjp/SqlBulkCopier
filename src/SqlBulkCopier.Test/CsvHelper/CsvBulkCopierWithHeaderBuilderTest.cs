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