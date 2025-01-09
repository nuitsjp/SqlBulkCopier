using System.Globalization;
using System.Text;
using CsvHelper;
using CsvHelper.Configuration;
using Dapper;
using FluentAssertions;
using Microsoft.Data.SqlClient;
using SqlBulkCopier.CsvHelper;
using SqlBulkCopier.Test.CsvHelper.Util;

// ReSharper disable UseAwaitUsing

namespace SqlBulkCopier.Test.CsvHelper;

public class CsvBulkCopierBuilderTest() : BulkCopierBuilderTestBase(DatabaseName)
{
    private const string DatabaseName = "CsvBulkCopierBuilderTest";

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
        first.Convert("1.234.567,89xy").Should().Be(expected);
        second.Convert("1,234,567.89xy").Should().Be(expected);
    }

    [Fact]
    public async Task WriteToServerAsync()
    {
        // Arrange
        const int count = 100;
        var targets = GenerateBulkInsertTestTargetData(count);
        using var stream = await CreateCsvAsync(targets);

        using var sqlConnection = new SqlConnection(SqlBulkCopierConnectionString);
        await sqlConnection.OpenAsync(CancellationToken.None);

        // 例示のビルダーAPI。実際の実装に応じて修正してください。
        var sqlBulkCopier = CsvBulkCopierBuilder
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
            .AddColumnMapping("VarBinaryValue", c => c.AsVarBinary())

            // ビルド
            .Build(sqlConnection);

        await sqlBulkCopier.WriteToServerAsync(stream, Encoding.UTF8, TimeSpan.FromMinutes(30));

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
    public async Task WriteToServerAsync_ByConnectionString()
    {
        // Arrange
        const int count = 100;
        var targets = GenerateBulkInsertTestTargetData(count);
        using var stream = await CreateCsvAsync(targets);

        // 例示のビルダーAPI。実際の実装に応じて修正してください。
        var sqlBulkCopier = CsvBulkCopierBuilder
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
            .AddColumnMapping("VarBinaryValue", c => c.AsVarBinary())

            // ビルド
            .Build(SqlBulkCopierConnectionString);

        await sqlBulkCopier.WriteToServerAsync(
            stream,
            Encoding.UTF8,
            TimeSpan.FromMinutes(30));

        // Assert
        using var connection = new SqlConnection(SqlBulkCopierConnectionString);
        await connection.OpenAsync(CancellationToken.None);
        using var transaction = connection.BeginTransaction();

        var insertedRows = (await connection
                .QueryAsync<BulkInsertTestTarget>(
                    "SELECT * FROM [dbo].[BulkInsertTestTarget] order by Id",
                    transaction: transaction))
            .ToArray();

        insertedRows.Should().NotBeEmpty("書き出したデータが読み込まれるはず");
        insertedRows.Length.Should().Be(count);

        // 先頭行などを必要に応じて検証
        var expected = targets.First();
        var actual = insertedRows.First();
        ShouldBe(expected, actual);
    }

    [Fact]
    public async Task WriteToServerAsync_ByConnectionStringAndOptions()
    {
        // Arrange
        const int count = 100;
        var targets = GenerateBulkInsertTestTargetData(count);
        using var stream = await CreateCsvAsync(targets);

        // 例示のビルダーAPI。実際の実装に応じて修正してください。
        var sqlBulkCopier = CsvBulkCopierBuilder
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
            .AddColumnMapping("VarBinaryValue", c => c.AsVarBinary())

            // ビルド
            .Build(SqlBulkCopierConnectionString, SqlBulkCopyOptions.Default);

        await sqlBulkCopier.WriteToServerAsync(
            stream,
            Encoding.UTF8,
            TimeSpan.FromMinutes(30));

        // Assert
        using var connection = new SqlConnection(SqlBulkCopierConnectionString);
        await connection.OpenAsync(CancellationToken.None);
        using var transaction = connection.BeginTransaction();

        var insertedRows = (await connection
                .QueryAsync<BulkInsertTestTarget>(
                    "SELECT * FROM [dbo].[BulkInsertTestTarget] order by Id",
                    transaction: transaction))
            .ToArray();

        insertedRows.Should().NotBeEmpty("書き出したデータが読み込まれるはず");
        insertedRows.Length.Should().Be(count);

        // 先頭行などを必要に応じて検証
        var expected = targets.First();
        var actual = insertedRows.First();
        ShouldBe(expected, actual);
    }


    [Fact]
    public async Task WriteToServerAsync_WithTransaction()
    {
        // Arrange
        const int count = 100;
        var targets = GenerateBulkInsertTestTargetData(count);
        using var stream = await CreateCsvAsync(targets);

        using var connection = new SqlConnection(SqlBulkCopierConnectionString);
        await connection.OpenAsync(CancellationToken.None);
        using var transaction = connection.BeginTransaction();

        // 例示のビルダーAPI。実際の実装に応じて修正してください。
        var sqlBulkCopier = CsvBulkCopierBuilder
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
            .AddColumnMapping("VarBinaryValue", c => c.AsVarBinary())

            // ビルド
            .Build(connection, SqlBulkCopyOptions.Default, transaction);

        await sqlBulkCopier.WriteToServerAsync(
            stream, 
            Encoding.UTF8, 
            TimeSpan.FromMinutes(30));

        // Assert

        var insertedRows = (await connection
                .QueryAsync<BulkInsertTestTarget>(
                    "SELECT * FROM [dbo].[BulkInsertTestTarget] order by Id",
                    transaction: transaction))
            .ToArray();

        insertedRows.Should().NotBeEmpty("書き出したデータが読み込まれるはず");
        insertedRows.Length.Should().Be(count);

        // 先頭行などを必要に応じて検証
        var expected = targets.First();
        var actual = insertedRows.First();
        ShouldBe(expected, actual);

        transaction.Rollback();

        using var newConnection = new SqlConnection(SqlBulkCopierConnectionString);
        await newConnection.OpenAsync(CancellationToken.None);
        (await newConnection.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM [dbo].[BulkInsertTestTarget]"))
            .Should().Be(0);
    }

    [Fact]
    public async Task WriteToServerWithRowFilter()
    {
        // Arrange
        const int count = 100;
        var targets = GenerateBulkInsertTestTargetData(count);
        using var stream = await CreateCsvAsync(targets, true);

        using var sqlConnection = new SqlConnection(SqlBulkCopierConnectionString);
        await sqlConnection.OpenAsync(CancellationToken.None);

        // 例示のビルダーAPI。実際の実装に応じて修正してください。
        var sqlBulkCopier = CsvBulkCopierBuilder
            .Create("[dbo].[BulkInsertTestTarget]")
            .SetDefaultColumnContext(c => c.TrimEnd().TreatEmptyStringAsNull())
            .SetRowFilter(reader =>
            {
                if(reader.Parser.RawRecord.StartsWith("Header"))
                {
                    return false;
                }
                if(reader.Parser.RawRecord.StartsWith("Footer"))
                {
                    return false;
                }
                return true;
            })

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
            .AddColumnMapping("VarBinaryValue", c => c.AsVarBinary())

            // ビルド
            .Build(sqlConnection);

        await sqlBulkCopier.WriteToServerAsync(stream, Encoding.UTF8, TimeSpan.FromMinutes(30));

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
    public async Task WriteToServerFromNoHeaderCsvAsync()
    {
        // Arrange
        var targets = GenerateBulkInsertTestTargetData(1);
        using var stream = await CreateNoHeaderCsvAsync(targets);

        using var sqlConnection = new SqlConnection(SqlBulkCopierConnectionString);
        await sqlConnection.OpenAsync(CancellationToken.None);

        // 例示のビルダーAPI。実際の実装に応じて修正してください。
        var sqlBulkCopier = CsvBulkCopierBuilder
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
            .AddColumnMapping("VarBinaryValue", 23, c => c.AsVarBinary())

            // ビルド
            .Build(sqlConnection);

        await sqlBulkCopier.WriteToServerAsync(stream, Encoding.UTF8, TimeSpan.FromMinutes(30));

        // Assert
        var insertedRows = (await sqlConnection.QueryAsync<BulkInsertTestTarget>(
            "SELECT * FROM [dbo].[BulkInsertTestTarget] order by Id")).ToArray();

        insertedRows.Should().NotBeEmpty("書き出したデータが読み込まれるはず");

        // 先頭行などを必要に応じて検証
        var expected = targets.First();
        var actual = insertedRows.First();
        ShouldBe(expected, actual);
    }


    [Fact]
    public async Task WriteToServerFromNoHeaderWithRowFilterCsvAsync()
    {
        // Arrange
        var targets = GenerateBulkInsertTestTargetData(1);
        using var stream = await CreateNoHeaderCsvAsync(targets, true);

        using var sqlConnection = new SqlConnection(SqlBulkCopierConnectionString);
        await sqlConnection.OpenAsync(CancellationToken.None);

        // 例示のビルダーAPI。実際の実装に応じて修正してください。
        var sqlBulkCopier = CsvBulkCopierBuilder
            .CreateNoHeader("[dbo].[BulkInsertTestTarget]")
            .SetDefaultColumnContext(c => c.TrimEnd().TreatEmptyStringAsNull())
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
            .AddColumnMapping("VarBinaryValue", 23, c => c.AsVarBinary())

            // ビルド
            .Build(sqlConnection);

        await sqlBulkCopier.WriteToServerAsync(stream, Encoding.UTF8, TimeSpan.FromMinutes(30));

        // Assert
        var insertedRows = (await sqlConnection.QueryAsync<BulkInsertTestTarget>(
            "SELECT * FROM [dbo].[BulkInsertTestTarget] order by Id")).ToArray();

        insertedRows.Should().NotBeEmpty("書き出したデータが読み込まれるはず");

        // 先頭行などを必要に応じて検証
        var expected = targets.First();
        var actual = insertedRows.First();
        ShouldBe(expected, actual);
    }

    [Fact]
    public async Task Customer_WriteToServerAsync()
    {
        // Arrange
        const int count = 50;
        var testCustomers = GenerateCustomers(count);

        // CSV 作成
        var stream = await CreateCustomerCsvAsync(testCustomers);

        using var connection = new SqlConnection(SqlBulkCopierConnectionString);
        await connection.OpenAsync();

        // 実際のバルクインサートライブラリーを利用（例示的に書きます）
        // カラムのマッピング設定
        var sqlBulkCopier = CsvBulkCopierBuilder
            .Create("[dbo].[Customer]")
            .SetDefaultColumnContext(c => c.TrimEnd().TreatEmptyStringAsNull())

            .AddColumnMapping("CustomerId")  // int
            .AddColumnMapping("FirstName")
            .AddColumnMapping("LastName")
            .AddColumnMapping("Email")
            .AddColumnMapping("PhoneNumber")
            .AddColumnMapping("AddressLine1")
            .AddColumnMapping("AddressLine2")
            .AddColumnMapping("City")
            .AddColumnMapping("State")
            .AddColumnMapping("PostalCode")
            .AddColumnMapping("Country")
            .AddColumnMapping("BirthDate", c => c.AsDate("yyyyMMdd"))
            .AddColumnMapping("Gender")
            .AddColumnMapping("Occupation")
            .AddColumnMapping("Income")  // decimal
            .AddColumnMapping("RegistrationDate", c => c.AsDateTime("yyyyMMddHHmmss"))
            .AddColumnMapping("LastLogin", c => c.AsDateTime("yyyyMMddHHmmss"))
            .AddColumnMapping("IsActive", c => c.AsBit()) // bool → "0" or "1" で CSV 出力している場合など
            .AddColumnMapping("Notes")

            // CreatedAt, UpdatedAt はテーブル上で DEFAULT GETDATE() になっているが、
            // CSV に出力している場合はマッピングも追加。
            // もし CSV に含めないなら AddColumnMapping しない。
            //.AddColumnMapping("CreatedAt", c => c.AsDateTime("yyyyMMddHHmmss"))
            //.AddColumnMapping("UpdatedAt", c => c.AsDateTime("yyyyMMddHHmmss"))

            .Build(connection);

        // CSV から SQL Server へ
        await sqlBulkCopier.WriteToServerAsync(stream, Encoding.UTF8, TimeSpan.FromMinutes(30));

        // Assert
        var insertedRows = (await connection.QueryAsync<Customer>(
            "SELECT * FROM [dbo].[Customer] ORDER BY CustomerId")).ToArray();

        insertedRows.Should().NotBeEmpty("書き出したデータが読み込まれるはず");
        insertedRows.Length.Should().Be(count);

        // 先頭行だけ検証例
        var expected = testCustomers.First();
        var actual = insertedRows.First();
        AssertCustomer(expected, actual);
    }

    private static void AssertCustomer(Customer expected, Customer actual)
    {
        actual.CustomerId.Should().Be(expected.CustomerId);
        actual.FirstName.Should().Be(expected.FirstName);
        actual.LastName.Should().Be(expected.LastName);
        actual.Email.Should().Be(expected.Email);
        actual.PhoneNumber.Should().Be(expected.PhoneNumber);
        actual.AddressLine1.Should().Be(expected.AddressLine1);
        actual.AddressLine2.Should().Be(expected.AddressLine2);
        actual.City.Should().Be(expected.City);
        actual.State.Should().Be(expected.State);
        actual.PostalCode.Should().Be(expected.PostalCode);
        actual.Country.Should().Be(expected.Country);

        // 日付は年月日だけ一致を確認
        if (expected.BirthDate.HasValue)
        {
            actual.BirthDate.Should().HaveYear(expected.BirthDate.Value.Year);
            actual.BirthDate.Should().HaveMonth(expected.BirthDate.Value.Month);
            actual.BirthDate.Should().HaveDay(expected.BirthDate.Value.Day);
        }

        actual.Gender.Should().Be(expected.Gender);
        actual.Occupation.Should().Be(expected.Occupation);
        actual.Income.Should().Be(expected.Income);

        // DateTime も秒精度で比較してみる
        if (expected.RegistrationDate.HasValue)
        {
            actual.RegistrationDate.Should().BeCloseTo(expected.RegistrationDate.Value, TimeSpan.FromSeconds(1));
        }

        if (expected.LastLogin.HasValue)
        {
            actual.LastLogin.Should().BeCloseTo(expected.LastLogin.Value, TimeSpan.FromSeconds(1));
        }

        actual.IsActive.Should().Be(expected.IsActive);
        actual.Notes.Should().Be(expected.Notes);

        // CreatedAt, UpdatedAt は DB の DEFAULT に任せるなら比較対象外でもよい
        // ここでは一応比較
        if (expected.CreatedAt.HasValue)
        {
            actual.CreatedAt.Should().BeCloseTo(expected.CreatedAt.Value, TimeSpan.FromSeconds(5));
        }
        if (expected.UpdatedAt.HasValue)
        {
            actual.UpdatedAt.Should().BeCloseTo(expected.UpdatedAt.Value, TimeSpan.FromSeconds(5));
        }
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

        if(withHeaderAndFooter == false)
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

    public static async Task<Stream> CreateCustomerCsvAsync(IEnumerable<Customer> customers,
        bool withHeaderAndFooter = false)
    {
        var encoding = new UTF8Encoding(false);
        using var stream = new MemoryStream();
        using var writer = new StreamWriter(stream, encoding);

        var csvWriter = new CsvWriter(writer, CultureInfo.InvariantCulture);
        csvWriter.Context.RegisterClassMap<CustomerMap>();
        await csvWriter.WriteRecordsAsync(customers, CancellationToken.None);
        await csvWriter.FlushAsync();

        // ReSharper disable once MethodHasAsyncOverload
        File.WriteAllBytes("Customer.csv", stream.ToArray());

        if (withHeaderAndFooter == false)
        {
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
}