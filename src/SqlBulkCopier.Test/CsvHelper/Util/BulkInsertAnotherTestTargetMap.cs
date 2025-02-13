using System.Globalization;
using CsvHelper.Configuration;

namespace SqlBulkCopier.Test.CsvHelper.Util;

// ReSharper disable once ClassNeverInstantiated.Global
public sealed class BulkInsertAnotherTestTargetMap : ClassMap<BulkInsertTestTarget>
{
    public BulkInsertAnotherTestTargetMap()
    {
        // PK (IDENTITY)
        Map(m => m.Id).Name("VId");

        // 数値型
        Map(m => m.TinyInt).Name("VTinyInt");
        Map(m => m.SmallInt).Name("VSmallInt");
        Map(m => m.IntValue).Name("VIntValue");
        Map(m => m.BigInt).Name("VBigInt");

        // ブール型
        Map(m => m.BitValue).Name("VBitValue");

        // 小数点数型
        Map(m => m.DecimalValue).Name("VDecimalValue");
        Map(m => m.NumericValue).Name("VNumericValue");
        Map(m => m.MoneyValue).Name("VMoneyValue");
        Map(m => m.SmallMoneyValue).Name("VSmallMoneyValue");
        Map(m => m.FloatValue).Name("VFloatValue");
        Map(m => m.RealValue).Name("VRealValue");

        // 日付・時刻型
        Map(m => m.DateValue).Name("VDateValue")
            .TypeConverterOption.CultureInfo(CultureInfo.CurrentCulture)
            .TypeConverterOption.Format("yyyyMMdd");
        Map(m => m.DateTimeValue).Name("VDateTimeValue")
            .TypeConverterOption.CultureInfo(CultureInfo.CurrentCulture)
            .TypeConverterOption.Format("yyyyMMddHHmmss");
        Map(m => m.SmallDateTimeValue).Name("VSmallDateTimeValue")
            .TypeConverterOption.CultureInfo(CultureInfo.CurrentCulture)
            .TypeConverterOption.Format("yyyyMMddHHmmss");
        Map(m => m.DateTime2Value).Name("VDateTime2Value")
            .TypeConverterOption.CultureInfo(CultureInfo.CurrentCulture)
            .TypeConverterOption.Format("yyyyMMddHHmmss");
        Map(m => m.TimeValue).Name("VTimeValue")
            .TypeConverterOption.Format(@"hh\:mm\:ss")
            .TypeConverterOption.CultureInfo(CultureInfo.CurrentCulture);
        Map(m => m.DateTimeOffsetValue).Name("VDateTimeOffsetValue")
            .TypeConverterOption.CultureInfo(CultureInfo.CurrentCulture)
            .TypeConverterOption.Format("yyyyMMddHHmmK");

        // 文字列型
        Map(m => m.CharValue).Name("VCharValue");
        Map(m => m.VarCharValue).Name("VVarCharValue");
        Map(m => m.NCharValue).Name("VNCharValue");
        Map(m => m.NVarCharValue).Name("VNVarCharValue");

        // バイナリ型
        Map(m => m.BinaryValue).Name("VBinaryValue").TypeConverter<ByteArrayConverter>();
        Map(m => m.VarBinaryValue).Name("VVarBinaryValue").TypeConverter<ByteArrayConverter>();

        // GUID
        Map(m => m.UniqueIdValue).Name("VUniqueIdValue").TypeConverter<GuidConverter>();

        // XML
        Map(m => m.XmlValue).Name("VXmlValue");
    }
}