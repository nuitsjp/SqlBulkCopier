using System.Globalization;
using System.Text;
using CsvHelper;
using CsvHelper.Configuration;
using Dapper;
using Microsoft.Data.SqlClient;
using Moq;
using Shouldly;
using SqlBulkCopier.CsvHelper;
using SqlBulkCopier.Test.CsvHelper.Util;

// ReSharper disable UseAwaitUsing

namespace SqlBulkCopier.Test.CsvHelper;

public class CsvBulkCopierWithHeaderBuilderTest
{
    [Fact]
    public void SetDefaultColumnContext()
    {
        // Arrange
        const decimal expected = 1234567.89m;
        var builder = (CsvBulkCopierBuilder)CsvBulkCopierBuilder
            .CreateWithHeader("[dbo].[BulkInsertTestTarget]")
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

    public abstract class WriteToServerAsync() : BulkCopierBuilderTestBase(DatabaseName)
    {
        private const string DatabaseName = "CsvBulkCopierBuilderTest";

        const int Count = 100;

        private List<BulkInsertTestTarget> Targets { get; } = GenerateBulkInsertTestTargetData(Count);

        public class WithoutRetry : WriteToServerAsync
        {
            [Fact]
            public async Task ByConnection()
            {
                // Arrange
                using var sqlBulkCopier = ProvideBuilder()
                    .SetBatchSize(10)
                    .SetTruncateBeforeBulkInsert(true)
                    .Build(await OpenConnectionAsync());
                var callbackCount = 0;
                sqlBulkCopier.NotifyAfter = 10;
                sqlBulkCopier.SqlRowsCopied += (_, args) =>
                {
                    callbackCount++;
                    args.RowsCopied.ShouldBeGreaterThanOrEqualTo(0);
                };

                sqlBulkCopier.BatchSize.ShouldBe(10);
                sqlBulkCopier.RowsCopied.ShouldBe(0);
                sqlBulkCopier.RowsCopied64.ShouldBe(0);
                sqlBulkCopier.NotifyAfter.ShouldBe(10);
                sqlBulkCopier.DestinationTableName.ShouldBe("[dbo].[BulkInsertTestTarget]");

                // Act
                await sqlBulkCopier.WriteToServerAsync(
                    await CreateCsvAsync(Targets),
                    Encoding.UTF8,
                    TimeSpan.FromMinutes(30));

                await sqlBulkCopier.WriteToServerAsync(
                    await CreateCsvAsync(Targets),
                    Encoding.UTF8,
                    TimeSpan.FromMinutes(30));

                // Assert
                await AssertAsync();
                callbackCount.ShouldBe(Count * 2 / 10);
            }

            [Fact]
            public async Task ByConnectionString()
            {
                // Arrange
                using var sqlBulkCopier = ProvideBuilder()
                    .SetTruncateBeforeBulkInsert(true)
                    .Build(SqlBulkCopierConnectionString);

                // Act
                await sqlBulkCopier.WriteToServerAsync(
                    await CreateCsvAsync(Targets),
                    Encoding.UTF8,
                    TimeSpan.FromMinutes(30));

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
                using var sqlBulkCopier = ProvideBuilder()
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
                using var sqlBulkCopier = ProvideBuilder()
                    .SetTruncateBeforeBulkInsert(true)
                    .Build(connection, SqlBulkCopyOptions.Default, transaction);

                await sqlBulkCopier.WriteToServerAsync(
                    await CreateCsvAsync(Targets),
                    Encoding.UTF8,
                    TimeSpan.FromMinutes(30));

                await sqlBulkCopier.WriteToServerAsync(
                    await CreateCsvAsync(Targets),
                    Encoding.UTF8,
                    TimeSpan.FromMinutes(30));

                // Assert
                await AssertAsync(connection, transaction);

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
                using var sqlBulkCopier = ProvideBuilder()
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
                using var sqlBulkCopier = ProvideNoHeaderBuilder()
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
                using var sqlBulkCopier = ProvideNoHeaderBuilder()
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


        }

        public class WithRetry : WriteToServerAsync
        {
            [Fact]
            public async Task ByConnection()
            {
                // Arrange
                using var sqlBulkCopier = ProvideBuilder()
                    .SetMaxRetryCount(3)
                    .SetTruncateBeforeBulkInsert(true)
                    .Build(await OpenConnectionAsync());

                // Act
                using var stream = await CreateCsvAsync(Targets);
                // ReSharper disable once AccessToDisposedClosure
                Func<Task> func = () => sqlBulkCopier.WriteToServerAsync(
                    // ReSharper disable once AccessToDisposedClosure
                    stream,
                    Encoding.UTF8,
                    TimeSpan.FromMinutes(30));

                // Assert
                func.ShouldThrow<InvalidOperationException>();
            }

            [Fact]
            public async Task ByConnectionString()
            {
                //////////////////////////////////////////////////////////////////////////////////
                // Arrange
                //////////////////////////////////////////////////////////////////////////////////
                var maxRetryCount = 3;
                var builder = (CsvBulkCopierBuilder)ProvideBuilder();
                var csvDataReaderBuilder = new CsvDataReaderBuilder(true, builder.Columns, reader => true);
                var csvDataReaderBuilderMock = new Mock<IDataReaderBuilder>();
                csvDataReaderBuilderMock
                    .Setup(x => x.SetupColumnMappings(It.IsAny<SqlBulkCopy>()))
                    .Callback<SqlBulkCopy>(sqlBulkCopy => csvDataReaderBuilder.SetupColumnMappings(sqlBulkCopy));
                // csvDataReaderBuilderMockのBuildが呼ばれたら、csvDataReaderBuilderのBuildを呼ぶ
                var callCount = 0;
                csvDataReaderBuilderMock
                    .Setup(x => x.Build(It.IsAny<Stream>(), It.IsAny<Encoding>()))
                    .Returns<Stream, Encoding>((stream, encoding) =>
                    {
                        if (callCount++ < maxRetryCount)
                        {
                            throw new InvalidOperationException("Simulate failure");
                        }
                        return csvDataReaderBuilder.Build(stream, encoding);
                    });

                using var sqlBulkCopier = new BulkCopier(
                    "[dbo].[BulkInsertTestTarget]",
                    csvDataReaderBuilderMock.Object,
                    SqlBulkCopierConnectionString)
                {
                    MaxRetryCount = maxRetryCount,
                    TruncateBeforeBulkInsert = true, 
                    InitialDelay = TimeSpan.FromMilliseconds(1),
                    UseExponentialBackoff = true
                };

                var stream = await CreateCsvAsync(Targets);
                // Move to the end of the stream to check that the stream position is correctly returned to the beginning on retry
                stream.Seek(0, SeekOrigin.End);

                //////////////////////////////////////////////////////////////////////////////////
                // Act
                //////////////////////////////////////////////////////////////////////////////////
                await sqlBulkCopier.WriteToServerAsync(
                    stream,
                    Encoding.UTF8,
                    TimeSpan.FromMinutes(30));

                //////////////////////////////////////////////////////////////////////////////////
                // Assert
                //////////////////////////////////////////////////////////////////////////////////
                await AssertAsync();
                // csvDataReaderBuilderMockのBuildが(maxRetryCount + 1)呼ばれている事を確認
                csvDataReaderBuilderMock.Verify(
                    x => x.Build(It.IsAny<Stream>(), It.IsAny<Encoding>()), 
                    Times.Exactly(maxRetryCount + 1));
            }

            [Fact]
            public async Task ByConnectionString_RetryOver()
            {
                //////////////////////////////////////////////////////////////////////////////////
                // Arrange
                //////////////////////////////////////////////////////////////////////////////////
                var maxRetryCount = 3;
                var builder = (CsvBulkCopierBuilder)ProvideBuilder();
                var csvDataReaderBuilder = new CsvDataReaderBuilder(true, builder.Columns, reader => true);
                var csvDataReaderBuilderMock = new Mock<IDataReaderBuilder>();
                csvDataReaderBuilderMock
                    .Setup(x => x.SetupColumnMappings(It.IsAny<SqlBulkCopy>()))
                    .Callback<SqlBulkCopy>(sqlBulkCopy => csvDataReaderBuilder.SetupColumnMappings(sqlBulkCopy));
                // csvDataReaderBuilderMockのBuildが呼ばれたら、csvDataReaderBuilderのBuildを呼ぶ
                var callCount = 0;
                csvDataReaderBuilderMock
                    .Setup(x => x.Build(It.IsAny<Stream>(), It.IsAny<Encoding>()))
                    .Returns<Stream, Encoding>((stream, encoding) => throw new InvalidOperationException("Simulate failure"));

                using var sqlBulkCopier = new BulkCopier(
                    "[dbo].[BulkInsertTestTarget]",
                    csvDataReaderBuilderMock.Object,
                    SqlBulkCopierConnectionString)
                {
                    MaxRetryCount = maxRetryCount,
                    TruncateBeforeBulkInsert = true,
                    InitialDelay = TimeSpan.FromMilliseconds(1),
                    UseExponentialBackoff = false
                };

                var stream = await CreateCsvAsync(Targets);
                // Move to the end of the stream to check that the stream position is correctly returned to the beginning on retry
                stream.Seek(0, SeekOrigin.End);

                //////////////////////////////////////////////////////////////////////////////////
                // Act
                //////////////////////////////////////////////////////////////////////////////////
                // ReSharper disable once AccessToDisposedClosure
                Func<Task> func = () => sqlBulkCopier.WriteToServerAsync(
                    stream,
                    Encoding.UTF8,
                    TimeSpan.FromMinutes(30));

                //////////////////////////////////////////////////////////////////////////////////
                // Assert
                //////////////////////////////////////////////////////////////////////////////////
                func.ShouldThrow<Exception>();
            }


            [Fact]
            public async Task ByConnectionStringAndOptions()
            {
                // Arrange
                using var sqlBulkCopier = ProvideBuilder()
                    .SetMaxRetryCount(3)
                    .SetTruncateBeforeBulkInsert(true)
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
                using var sqlBulkCopier = ProvideBuilder()
                    .SetMaxRetryCount(3)
                    .SetTruncateBeforeBulkInsert(true)
                    .Build(connection, SqlBulkCopyOptions.Default, transaction);

                // Act
                using var stream = await CreateCsvAsync(Targets);
                Func<Task> func = () => sqlBulkCopier.WriteToServerAsync(
                    // ReSharper disable once AccessToDisposedClosure
                    stream,
                    Encoding.UTF8,
                    TimeSpan.FromMinutes(30));

                // Assert
                func.ShouldThrow<InvalidOperationException>();
            }

            [Fact]
            public async Task WithRowFilter()
            {
                // Arrange
                using var sqlBulkCopier = ProvideBuilder()
                    .SetMaxRetryCount(3)
                    .SetTruncateBeforeBulkInsert(true)
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

                // Act
                using var stream = await CreateCsvAsync(Targets);
                Func<Task> func = () => sqlBulkCopier.WriteToServerAsync(
                    // ReSharper disable once AccessToDisposedClosure
                    stream,
                    Encoding.UTF8,
                    TimeSpan.FromMinutes(30));

                // Assert
                func.ShouldThrow<InvalidOperationException>();
            }

            [Fact]
            public async Task FromNoHeaderCsvAsync()
            {
                // Arrange
                using var sqlBulkCopier = ProvideNoHeaderBuilder()
                    .SetMaxRetryCount(3)
                    .SetTruncateBeforeBulkInsert(true)
                    .Build(await OpenConnectionAsync());

                // Act
                using var stream = await CreateCsvAsync(Targets);
                Func<Task> func = () => sqlBulkCopier.WriteToServerAsync(
                    // ReSharper disable once AccessToDisposedClosure
                    stream,
                    Encoding.UTF8,
                    TimeSpan.FromMinutes(30));

                // Assert
                func.ShouldThrow<InvalidOperationException>();
            }


            [Fact]
            public async Task FromNoHeader_WithRowFilterCsvAsync()
            {
                // Arrange
                using var sqlBulkCopier = ProvideNoHeaderBuilder()
                    .SetMaxRetryCount(3)
                    .SetTruncateBeforeBulkInsert(true)
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

                // Act
                using var stream = await CreateCsvAsync(Targets);
                Func<Task> func = () => sqlBulkCopier.WriteToServerAsync(
                    // ReSharper disable once AccessToDisposedClosure
                    stream,
                    Encoding.UTF8,
                    TimeSpan.FromMinutes(30));

                // Assert
                func.ShouldThrow<InvalidOperationException>();
            }


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

        private ICsvBulkCopierWithHeaderBuilder ProvideBuilder()
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

            await AssertAsync(connection);
        }

        private async Task AssertAsync(SqlConnection connection, SqlTransaction? transaction = null)
        {
            var insertedRows = 
                transaction is not null
                    ? (await connection.QueryAsync<BulkInsertTestTarget>(
                        "SELECT * FROM [dbo].[BulkInsertTestTarget] with(nolock) order by Id", transaction: transaction)).ToArray()
                    : (await connection.QueryAsync<BulkInsertTestTarget>(
                        "SELECT * FROM [dbo].[BulkInsertTestTarget] with(nolock) order by Id")).ToArray();

            insertedRows.ShouldNotBeEmpty("書き出したデータが読み込まれるはず");
            insertedRows.Length.ShouldBe(Count);

            // 先頭行などを必要に応じて検証
            var expected = Targets.First();
            var actual = insertedRows.First();
            ShouldBe(expected, actual);
        }

    }
}