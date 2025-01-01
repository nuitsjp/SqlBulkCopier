using System.Globalization;
using System.Text;
using Dapper;
using FluentAssertions;
using Microsoft.Data.SqlClient;
using SqlBulkCopier.FixedLength;

// ReSharper disable UseAwaitUsing

namespace SqlBulkCopier.Test.FixedLength
{
    public class FixedLengthBulkCopierBuilderTest() : BulkCopierBuilderTestBase(DatabaseName)
    {
        private const string DatabaseName = "FixedLengthBulkCopierBuilderTest";

        [Fact]
        public void SetDefaultColumnContext()
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
            first.Convert("1.234.567,89xy").Should().Be(expected);
            second.Convert("1,234,567.89xy").Should().Be(expected);
        }

        [Fact]
        public async Task WriteToServerAsync()
        {
            // Arrange
            const int count = 100;
            var targets = GenerateBulkInsertTestTargetData(count);
            using var stream = await CreateFixedLengthAsync(targets);
            using var sqlConnection = new SqlConnection(SqlBulkCopierConnectionString);
            await sqlConnection.OpenAsync(CancellationToken.None);

            var sqlBulkCopier = FixedLengthBulkCopierBuilder
                .Create("[dbo].[BulkInsertTestTarget]")
                .SetDefaultColumnContext(c => c.TrimEnd().TreatEmptyStringAsNull())

                .AddColumnMapping("Id", 0, 10)
                .AddColumnMapping("TinyInt", 10, 3)
                .AddColumnMapping("SmallInt", 13, 6)
                .AddColumnMapping("IntValue", 19, 11)
                .AddColumnMapping("BigInt", 30, 20)

                // bit列は "0" or "1" → bool に明示変換が必要
                .AddColumnMapping("BitValue", 50, 1, c => c.AsBit())

                // decimal や float なども標準的な数値文字列であれば自動変換が可能
                .AddColumnMapping("DecimalValue", 51, 21)
                .AddColumnMapping("NumericValue", 72, 21)
                .AddColumnMapping("MoneyValue", 93, 21)
                .AddColumnMapping("SmallMoneyValue", 114, 21)
                .AddColumnMapping("FloatValue", 135, 15)
                .AddColumnMapping("RealValue", 150, 12)

                // 日付系：yyyyMMdd, yyyyMMddHHmmss などは SQLServer が自動認識しない場合が多い
                // よって、パーサーを指定
                .AddColumnMapping("DateValue", 162, 8, c => c.AsDate("yyyyMMdd"))
                .AddColumnMapping("DateTimeValue", 170, 14, c => c.AsDateTime("yyyyMMddHHmmss"))
                .AddColumnMapping("SmallDateTimeValue", 184, 14, c => c.AsSmallDateTime("yyyyMMddHHmmss"))
                .AddColumnMapping("DateTime2Value", 198, 14, c => c.AsDateTime2("yyyyMMddHHmmss"))

                // time: "HHmmss" として保存しているなら要手動パース
                .AddColumnMapping("TimeValue", 212, 6, c => c.AsTime("hhmmss"))

                // datetimeoffset: "yyyyMMddHHmmss+09:00" など → 要パーサー
                .AddColumnMapping("DateTimeOffsetValue", 218, 18, c => c.AsDateTimeOffset("yyyyMMddHHmmK"))

                // 文字列系は何も指定しなければそのまま文字列としてマッピングされる（必要に応じて TrimEnd）
                .AddColumnMapping("CharValue", 236, 10)
                .AddColumnMapping("VarCharValue", 246, 50)
                .AddColumnMapping("NCharValue", 296, 10)
                .AddColumnMapping("NVarCharValue", 306, 50)

                // バイナリを Base64 で書き出しているなら、Convert.FromBase64String が必要
                // もし ASCII 文字列そのままなら変換不要
                .AddColumnMapping("BinaryValue", 356, 20, c => c.AsBinary())
                .AddColumnMapping("VarBinaryValue", 376, 100, c => c.AsVarBinary())

                // GUID
                .AddColumnMapping("UniqueIdValue", 476, 36, c => c.AsUniqueIdentifier())

                // XML → そのまま文字列で受け取れる
                .AddColumnMapping("XmlValue", 512, 100)

                // ビルド
                .Build();

            // ファイルを開いて実行
            await sqlBulkCopier.WriteToServerAsync(sqlConnection, stream, Encoding.UTF8);

            // Assert
            var insertedRows = (await sqlConnection.QueryAsync<BulkInsertTestTarget>(
                "SELECT * FROM [dbo].[BulkInsertTestTarget] order by Id")).ToArray();

            insertedRows.Should().NotBeEmpty("書き出したデータが読み込まれるはず");
            insertedRows.Length.Should().Be(count);

            // 先頭行などを必要に応じて検証
            var expected = targets.First();
            var actual = insertedRows.First();
            ShouldBe(expected, actual);
        }

        [Fact]
        public async Task WriteToServer_WithHeaderAndFooterAsync()
        {
            // Arrange
            const int count = 1;
            var targets = GenerateBulkInsertTestTargetData(count);
            using var stream = await CreateFixedLengthAsync(targets, true);
            using var sqlConnection = new SqlConnection(SqlBulkCopierConnectionString);
            await sqlConnection.OpenAsync(CancellationToken.None);

            Encoding encoding = new UTF8Encoding(false);

            var sqlBulkCopier = FixedLengthBulkCopierBuilder
                .Create("[dbo].[BulkInsertTestTarget]")
                .SetDefaultColumnContext(c => c.TrimEnd().TreatEmptyStringAsNull())
                .SetRowFilter(reader =>
                {
                    if (reader.CurrentRow.Length == 0)
                    {
                        return false;
                    }
                    if(reader.GetField(0, "Header".Length) == "Header")
                    {
                        return false;
                    }
                    if(reader.GetField(0, "Footer".Length) == "Footer")
                    {
                        return false;
                    }
                    return true;
                })

                .AddColumnMapping("Id", 0, 10)
                .AddColumnMapping("TinyInt", 10, 3)
                .AddColumnMapping("SmallInt", 13, 6)
                .AddColumnMapping("IntValue", 19, 11)
                .AddColumnMapping("BigInt", 30, 20)

                // bit列は "0" or "1" → bool に明示変換が必要
                .AddColumnMapping("BitValue", 50, 1, c => c.AsBit())

                // decimal や float なども標準的な数値文字列であれば自動変換が可能
                .AddColumnMapping("DecimalValue", 51, 21)
                .AddColumnMapping("NumericValue", 72, 21)
                .AddColumnMapping("MoneyValue", 93, 21)
                .AddColumnMapping("SmallMoneyValue", 114, 21)
                .AddColumnMapping("FloatValue", 135, 15)
                .AddColumnMapping("RealValue", 150, 12)

                // 日付系：yyyyMMdd, yyyyMMddHHmmss などは SQLServer が自動認識しない場合が多い
                // よって、パーサーを指定
                .AddColumnMapping("DateValue", 162, 8, c => c.AsDate("yyyyMMdd"))
                .AddColumnMapping("DateTimeValue", 170, 14, c => c.AsDateTime("yyyyMMddHHmmss"))
                .AddColumnMapping("SmallDateTimeValue", 184, 14, c => c.AsSmallDateTime("yyyyMMddHHmmss"))
                .AddColumnMapping("DateTime2Value", 198, 14, c => c.AsDateTime2("yyyyMMddHHmmss"))

                // time: "HHmmss" として保存しているなら要手動パース
                .AddColumnMapping("TimeValue", 212, 6, c => c.AsTime("hhmmss"))

                // datetimeoffset: "yyyyMMddHHmmss+09:00" など → 要パーサー
                .AddColumnMapping("DateTimeOffsetValue", 218, 18, c => c.AsDateTimeOffset("yyyyMMddHHmmK"))

                // 文字列系は何も指定しなければそのまま文字列としてマッピングされる（必要に応じて TrimEnd）
                .AddColumnMapping("CharValue", 236, 10)
                .AddColumnMapping("VarCharValue", 246, 50)
                .AddColumnMapping("NCharValue", 296, 10)
                .AddColumnMapping("NVarCharValue", 306, 50)

                // バイナリを Base64 で書き出しているなら、Convert.FromBase64String が必要
                // もし ASCII 文字列そのままなら変換不要
                .AddColumnMapping("BinaryValue", 356, 20, c => c.AsBinary())
                .AddColumnMapping("VarBinaryValue", 376, 100, c => c.AsVarBinary())

                // GUID
                .AddColumnMapping("UniqueIdValue", 476, 36, c => c.AsUniqueIdentifier())

                // XML → そのまま文字列で受け取れる
                .AddColumnMapping("XmlValue", 512, 100)

                // ビルド
                .Build();

            // ファイルを開いて実行
            await sqlBulkCopier.WriteToServerAsync(sqlConnection, stream, encoding);

            // Assert
            var insertedRows = (await sqlConnection.QueryAsync<BulkInsertTestTarget>(
                "SELECT * FROM [dbo].[BulkInsertTestTarget] order by Id")).ToArray();

            insertedRows.Should().NotBeEmpty("書き出したデータが読み込まれるはず");
            insertedRows.Length.Should().Be(count);

            // 先頭行などを必要に応じて検証
            var expected = targets.First();
            var actual = insertedRows.First();
            ShouldBe(expected, actual);
        }

        /// <summary>
        /// 生成したデータを固定長でファイル出力(バイト数でパディング)する
        /// </summary>
        // ReSharper disable once UnusedMember.Local
        private static async Task<Stream>  CreateFixedLengthAsync(List<BulkInsertTestTarget> dataList, bool withHeaderAndFooter = false)
        {
            // 文字エンコーディングはUTF-8
            Encoding encoding = new UTF8Encoding(false);

            using var stream = new MemoryStream();
            using var writer = new StreamWriter(stream, encoding);
            foreach (var item in dataList)
            {
                // バイト数の幅はサンプルとして決めています。要件に合わせて変更してください。
                await writer.WriteAsync(PadRightBytes(item.Id?.ToString() ?? "", 10, encoding));
                await writer.WriteAsync(PadRightBytes(item.TinyInt?.ToString() ?? "", 3, encoding));
                await writer.WriteAsync(PadRightBytes(item.SmallInt?.ToString() ?? "", 6, encoding));
                await writer.WriteAsync(PadRightBytes(item.IntValue?.ToString() ?? "", 11, encoding));
                await writer.WriteAsync(PadRightBytes(item.BigInt?.ToString() ?? "", 20, encoding));
                await writer.WriteAsync(PadRightBytes(item.BitValue == true ? "1" : "0", 1, encoding));
                // 51bytes

                // decimal系(10,2) を想定 → 例: 最大 9999999999.99(13文字) + 余裕
                await writer.WriteAsync(PadRightBytes(item.DecimalValue?.ToString("0.00") ?? "", 21, encoding));
                await writer.WriteAsync(PadRightBytes(item.NumericValue?.ToString("0.00") ?? "", 21, encoding));
                await writer.WriteAsync(PadRightBytes(item.MoneyValue?.ToString("0.00") ?? "", 21, encoding));
                await writer.WriteAsync(PadRightBytes(item.SmallMoneyValue?.ToString("0.00") ?? "", 21, encoding));
                // 84bytes

                // double? floatValue, float? realValue
                await writer.WriteAsync(PadRightBytes(item.FloatValue?.ToString("G") ?? "", 15, encoding));
                await writer.WriteAsync(PadRightBytes(item.RealValue?.ToString("G") ?? "", 12, encoding));
                // 27bytes

                // 日付時刻系
                // DATE / DATETIME / SMALLDATETIME / DATETIME2 → yyyyMMddHHmmss (一旦14桁統一)
                await writer.WriteAsync(PadRightBytes(item.DateValue?.ToString("yyyyMMdd") ?? "", 8, encoding));
                await writer.WriteAsync(PadRightBytes(item.DateTimeValue?.ToString("yyyyMMddHHmmss") ?? "", 14, encoding));
                await writer.WriteAsync(PadRightBytes(item.SmallDateTimeValue?.ToString("yyyyMMddHHmmss") ?? "", 14, encoding));
                await writer.WriteAsync(PadRightBytes(item.DateTime2Value?.ToString("yyyyMMddHHmmss") ?? "", 14, encoding));

                // TIME → HHmmss (6桁) or fffまで含める場合は9桁など
                await writer.WriteAsync(PadRightBytes(item.TimeValue?.ToString("hhmmss") ?? "", 6, encoding));

                // DATETIMEOFFSET → yyyyMMddHHmmK (18桁程度) ※フォーマットは要調整
                await writer.WriteAsync(PadRightBytes(item.DateTimeOffsetValue?.ToString("yyyyMMddHHmmK") ?? "", 18, encoding));

                // 文字列系
                await writer.WriteAsync(PadRightBytes(item.CharValue ?? "", 10, encoding));
                await writer.WriteAsync(PadRightBytes(item.VarCharValue ?? "", 50, encoding));
                await writer.WriteAsync(PadRightBytes(item.NCharValue ?? "", 10, encoding));
                await writer.WriteAsync(PadRightBytes(item.NVarCharValue ?? "", 50, encoding));

                // バイナリ → Base64などで文字列化
                await writer.WriteAsync(PadRightBytes(item.BinaryValue is null ? ""
                    : Convert.ToBase64String(item.BinaryValue), 20, encoding));
                await writer.WriteAsync(PadRightBytes(item.VarBinaryValue is null ? ""
                    : Convert.ToBase64String(item.VarBinaryValue), 100, encoding));

                // GUID
                await writer.WriteAsync(PadRightBytes(item.UniqueIdValue?.ToString() ?? "", 36, encoding));

                // XML → 適当に100バイト程度
                await writer.WriteAsync(PadRightBytes(item.XmlValue ?? "", 100, encoding));

                await writer.WriteLineAsync(); // 行区切り
            }

            await writer.FlushAsync();

            if (!withHeaderAndFooter)
            {
                return new MemoryStream(stream.ToArray());
            }

            var fixedLength =
                $"""
                 Header
                 {encoding.GetString(stream.ToArray())}
                 Footer
                 """;
            return new MemoryStream(encoding.GetBytes(fixedLength));
        }

        /// <summary>
        /// 文字列を指定されたバイト数になるように右側をスペースでパディング
        /// </summary>
        private static string PadRightBytes(string str, int totalBytes, Encoding encoding)
        {
            var stringBytes = encoding.GetBytes(str);
            if (stringBytes.Length == totalBytes)
            {
                // バイト数が一致する場合はそのまま返す
                return str;
            }
            if (totalBytes < stringBytes.Length)
            {
                // バイト数が超過する場合は切り詰める
                return encoding.GetString(stringBytes, 0, totalBytes);
            }

            // 不足分をスペースで埋める
            var paddingBytes = totalBytes - stringBytes.Length;
            return str + new string(' ', paddingBytes);
        }
    }
}