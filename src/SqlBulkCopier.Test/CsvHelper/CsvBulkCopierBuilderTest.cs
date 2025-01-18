using System.Globalization;
using System.Text;
using CsvHelper;
using CsvHelper.Configuration;
using Dapper;
using Microsoft.Data.SqlClient;
using Shouldly;
using SqlBulkCopier.CsvHelper;
using SqlBulkCopier.Test.CsvHelper.Util;

// ReSharper disable UseAwaitUsing

namespace SqlBulkCopier.Test.CsvHelper;

public class CsvBulkCopierBuilderTest
{
    [Fact]
    public void SetDefaultColumnContext()
    {
        // Arrange
        const decimal expected = 1234567.89m;
        var builder = (CsvBulkCopierBuilder)CsvBulkCopierBuilder
            .Create("[dbo].[BulkInsertTestTarget]")
            .SetDefaultColumnContext(
                c => c
                    .TrimEnd(['x', 'y'])
                    .TreatEmptyStringAsNull()
                    .AsDecimal(NumberStyles.AllowThousands | NumberStyles.AllowDecimalPoint, CultureInfo.GetCultureInfo("de-DE")))
            .AddColumnMapping("First")
            .AddColumnMapping("Second", c => c.AsDecimal());
        var first = builder.Columns.First();
        var second = builder.Columns.Last();

        // Act & Assert
        first.Convert("1.234.567,89xy").ShouldBe(expected);
        second.Convert("1,234,567.89xy").ShouldBe(expected);
    }

    public class WriteToServerAsync() : BulkCopierBuilderTestBase(DatabaseName)
    {
        private const string DatabaseName = "CsvBulkCopierBuilderTest";

        const int Count = 100;

        private List<BulkInsertTestTarget> Targets { get; } = GenerateBulkInsertTestTargetData(Count);

        [Fact]
        public async Task ByConnection()
        {
            // Arrange
            var sqlBulkCopier = ProvideBuilder()
                .Build(await OpenConnectionAsync());

            // Act
            await sqlBulkCopier.WriteToServerAsync(
                await CreateCsvAsync(Targets), 
                Encoding.UTF8, 
                TimeSpan.FromMinutes(30));

            // Assert
            await AssertAsync();
        }

        [Fact]
        public async Task ByConnectionString()
        {
            // Arrange
            var sqlBulkCopier = ProvideBuilder()
                .Build(SqlBulkCopierConnectionString);

            await sqlBulkCopier.WriteToServerAsync(
                await CreateCsvAsync(Targets),
                Encoding.UTF8,
                TimeSpan.FromMinutes(30));

            // Assert
            await AssertAsync();
        }

        [Fact]
        public async Task ByConnectionStringAndOptions()
        {
            // Arrange
            var sqlBulkCopier = ProvideBuilder()
                .Build(SqlBulkCopierConnectionString, SqlBulkCopyOptions.Default);

            await sqlBulkCopier.WriteToServerAsync(
                await CreateCsvAsync(Targets),
                Encoding.UTF8,
                TimeSpan.FromMinutes(30));

            // Assert
            await AssertAsync();
        }

        [Fact]
        public async Task WithTransaction()
        {
            // Arrange
            using var connection = await OpenConnectionAsync();
            using var transaction = connection.BeginTransaction();
            var sqlBulkCopier = ProvideBuilder()
                .Build(connection, SqlBulkCopyOptions.Default, transaction);

            await sqlBulkCopier.WriteToServerAsync(
                await CreateCsvAsync(Targets),
                Encoding.UTF8,
                TimeSpan.FromMinutes(30));

            // Assert

            var insertedRows = (await connection
                    .QueryAsync<BulkInsertTestTarget>(
                        "SELECT * FROM [dbo].[BulkInsertTestTarget] order by Id",
                        transaction: transaction))
                .ToArray();

            insertedRows.ShouldNotBeEmpty("書き出したデータが読み込まれるはず");
            insertedRows.Length.ShouldBe(Count);

            // 先頭行などを必要に応じて検証
            var expected = Targets.First();
            var actual = insertedRows.First();
            ShouldBe(expected, actual);

            transaction.Rollback();

            using var newConnection = new SqlConnection(SqlBulkCopierConnectionString);
            await newConnection.OpenAsync(CancellationToken.None);
            (await newConnection.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM [dbo].[BulkInsertTestTarget]"))
                .ShouldBe(0);
        }

        [Fact]
        public async Task WithRowFilter()
        {
            // Arrange
            var sqlBulkCopier = ProvideBuilder()
                .SetRowFilter(reader =>
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
                })
                .Build(await OpenConnectionAsync());

            await sqlBulkCopier.WriteToServerAsync(
                await CreateCsvAsync(Targets), 
                Encoding.UTF8, 
                TimeSpan.FromMinutes(30));

            // Assert
            await AssertAsync();
        }

        [Fact]
        public async Task FromNoHeaderCsvAsync()
        {
            // Arrange
            var sqlBulkCopier = ProvideNoHeaderBuilder()
                .Build(await OpenConnectionAsync());

            await sqlBulkCopier.WriteToServerAsync(
                await CreateNoHeaderCsvAsync(Targets), 
                Encoding.UTF8, 
                TimeSpan.FromMinutes(30));

            // Assert
            await AssertAsync();
        }


        [Fact]
        public async Task FromNoHeader_WithRowFilterCsvAsync()
        {
            // Arrange
            var sqlBulkCopier = ProvideNoHeaderBuilder()
                .SetRowFilter(reader =>
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
                })
                .Build(await OpenConnectionAsync());

            await sqlBulkCopier.WriteToServerAsync(
                await CreateNoHeaderCsvAsync(Targets), 
                Encoding.UTF8, 
                TimeSpan.FromMinutes(30));

            // Assert
            await AssertAsync();
        }

        private async Task<SqlConnection> OpenConnectionAsync()
        {
            var sqlConnection = new SqlConnection(SqlBulkCopierConnectionString);
            await sqlConnection.OpenAsync(CancellationToken.None);
            return sqlConnection;
        }

        private static async Task<Stream> CreateCsvAsync(IEnumerable<BulkInsertTestTarget> targets, bool withHeaderAndFooter = false)
        {
            var encoding = new UTF8Encoding(false);
            using var stream = new MemoryStream();
            using var writer = new StreamWriter(stream, encoding);

            var csvWriter = new CsvWriter(writer, CultureInfo.InvariantCulture);
            csvWriter.Context.RegisterClassMap<BulkInsertTestTargetMap>();
            await csvWriter.WriteRecordsAsync(targets, CancellationToken.None);
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


        private static async Task<Stream> CreateNoHeaderCsvAsync(IEnumerable<BulkInsertTestTarget> targets, bool withHeaderAndFooter = false)
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
            await csvWriter.WriteRecordsAsync(targets, CancellationToken.None);
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

        private ICsvBulkCopierBuilder ProvideBuilder()
            => CsvBulkCopierBuilder
                .Create("[dbo].[BulkInsertTestTarget]")
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

        private ICsvBulkCopierNoHeaderBuilder ProvideNoHeaderBuilder()
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

        private async Task AssertAsync()
        {
            using var connection = new SqlConnection(SqlBulkCopierConnectionString);
            await connection.OpenAsync(CancellationToken.None);

            var insertedRows = (await connection.QueryAsync<BulkInsertTestTarget>(
                "SELECT * FROM [dbo].[BulkInsertTestTarget] order by Id")).ToArray();

            insertedRows.ShouldNotBeEmpty("書き出したデータが読み込まれるはず");
            insertedRows.Length.ShouldBe(Count);

            // 先頭行などを必要に応じて検証
            var expected = Targets.First();
            var actual = insertedRows.First();
            ShouldBe(expected, actual);
        }

    }
}