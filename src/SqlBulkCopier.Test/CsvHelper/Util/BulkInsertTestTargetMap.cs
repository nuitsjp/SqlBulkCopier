using System.Globalization;
using CsvHelper.Configuration;

namespace SqlBulkCopier.Test.CsvHelper.Util
{
    // ReSharper disable once ClassNeverInstantiated.Global
    public sealed class BulkInsertTestTargetMap : ClassMap<BulkInsertTestTarget>
    {
        public BulkInsertTestTargetMap()
        {
            // PK (IDENTITY)
            Map(m => m.Id).Name("Id");

            // 数値型
            Map(m => m.TinyInt).Name("TinyInt");
            Map(m => m.SmallInt).Name("SmallInt");
            Map(m => m.IntValue).Name("IntValue");
            Map(m => m.BigInt).Name("BigInt");

            // ブール型
            Map(m => m.BitValue).Name("BitValue");

            // 小数点数型
            Map(m => m.DecimalValue).Name("DecimalValue");
            Map(m => m.NumericValue).Name("NumericValue");
            Map(m => m.MoneyValue).Name("MoneyValue");
            Map(m => m.SmallMoneyValue).Name("SmallMoneyValue");
            Map(m => m.FloatValue).Name("FloatValue");
            Map(m => m.RealValue).Name("RealValue");

            // 日付・時刻型
            Map(m => m.DateValue).Name("DateValue")
                .TypeConverterOption.CultureInfo(CultureInfo.CurrentCulture)
                .TypeConverterOption.Format("yyyyMMdd");
            Map(m => m.DateTimeValue).Name("DateTimeValue")
                .TypeConverterOption.CultureInfo(CultureInfo.CurrentCulture)
                .TypeConverterOption.Format("yyyyMMddHHmmss");
            Map(m => m.SmallDateTimeValue).Name("SmallDateTimeValue")
                .TypeConverterOption.CultureInfo(CultureInfo.CurrentCulture)
                .TypeConverterOption.Format("yyyyMMddHHmmss");
            Map(m => m.DateTime2Value).Name("DateTime2Value")
                .TypeConverterOption.CultureInfo(CultureInfo.CurrentCulture)
                .TypeConverterOption.Format("yyyyMMddHHmmss");
            Map(m => m.TimeValue).Name("TimeValue")
                .TypeConverterOption.Format(@"hh\:mm\:ss")
                .TypeConverterOption.CultureInfo(CultureInfo.CurrentCulture);
            Map(m => m.DateTimeOffsetValue).Name("DateTimeOffsetValue")
                .TypeConverterOption.CultureInfo(CultureInfo.CurrentCulture)
                .TypeConverterOption.Format("yyyyMMddHHmmK");

            // 文字列型
            Map(m => m.CharValue).Name("CharValue");
            Map(m => m.VarCharValue).Name("VarCharValue");
            Map(m => m.NCharValue).Name("NCharValue");
            Map(m => m.NVarCharValue).Name("NVarCharValue");

            // バイナリ型
            Map(m => m.BinaryValue).Name("BinaryValue").TypeConverter<ByteArrayConverter>();
            Map(m => m.VarBinaryValue).Name("VarBinaryValue").TypeConverter<ByteArrayConverter>();

            // GUID
            Map(m => m.UniqueIdValue).Name("UniqueIdValue").TypeConverter<GuidConverter>();

            // XML
            Map(m => m.XmlValue).Name("XmlValue");
        }
    }
}