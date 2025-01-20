using Bogus;
using Dapper;
using Microsoft.Data.SqlClient;
using Shouldly;
using System.Text;
// ReSharper disable UseAwaitUsing
// ReSharper disable AccessToDisposedClosure

namespace SqlBulkCopier.Test;

public abstract class BulkCopierBuilderTestBase2<TBuilder> : BulkCopierBuilderTestBase
    where TBuilder : IBulkCopierBuilder<TBuilder>
{
    protected List<BulkInsertTestTarget> Targets { get; } = GenerateBulkInsertTestTargetData(Count);

    [Fact]
    public abstract void SetDefaultColumnContext();

    [Fact]
    public async Task WriteToServerAsync_NoRetry_WithConnection_ShouldSucceed()
    {
        // Arrange
        using var sqlBulkCopier = ProvideBuilder()
            .SetBatchSize(Count / 10)
            .SetNotifyAfter(Count / 10)
            .Build(await OpenConnectionAsync());

        // ファイルを開いて実行
        await sqlBulkCopier.WriteToServerAsync(
            await CreateBulkInsertStreamAsync(Targets),
            new UTF8Encoding(false),
            TimeSpan.FromMinutes(30));

        // Assert
        await AssertAsync();
    }

    [Fact]
    public async Task WriteToServerAsync_NoRetry_WithConnection_BeforeTruncate_ShouldSucceed()
    {
        // Arrange
        using var sqlBulkCopier = ProvideBuilder()
            .SetTruncateBeforeBulkInsert(true)
            .Build(await OpenConnectionAsync());

        await sqlBulkCopier.WriteToServerAsync(
            await CreateBulkInsertStreamAsync(Targets),
            new UTF8Encoding(false),
            TimeSpan.FromMinutes(30));

        await AssertAsync();

        // Act
        await sqlBulkCopier.WriteToServerAsync(
            await CreateBulkInsertStreamAsync(Targets),
            new UTF8Encoding(false),
            TimeSpan.FromMinutes(30));

        // Assert
        await AssertAsync();
    }

    [Fact]
    public async Task WriteToServerAsync_NoRetry_WithConnectionString_ShouldSucceed()
    {
        // Arrange
        using var sqlBulkCopier = ProvideBuilder()
            .Build(SqlBulkCopierConnectionString);

        // ファイルを開いて実行
        await sqlBulkCopier.WriteToServerAsync(
            await CreateBulkInsertStreamAsync(Targets),
            new UTF8Encoding(false),
            TimeSpan.FromMinutes(30));

        // Assert
        await AssertAsync();
    }

    [Fact]
    public async Task WriteToServerAsync_NoRetry_WithConnectionString_WithOptions_ShouldSucceed()
    {
        // Arrange
        using var sqlBulkCopier = ProvideBuilder()
            .Build(SqlBulkCopierConnectionString, SqlBulkCopyOptions.Default);

        // ファイルを開いて実行
        await sqlBulkCopier.WriteToServerAsync(
            await CreateBulkInsertStreamAsync(Targets),
            new UTF8Encoding(false),
            TimeSpan.FromMinutes(30));

        // Assert
        await AssertAsync();
    }

    [Fact]
    public async Task WriteToServerAsync_NoRetry_WithConnection_WithTransaction_ShouldSucceed()
    {
        // Arrange
        using var connection = await OpenConnectionAsync();
        using var transaction = connection.BeginTransaction();

        using var sqlBulkCopier = ProvideBuilder()
            .Build(connection, SqlBulkCopyOptions.Default, transaction);

        // ファイルを開いて実行
        await sqlBulkCopier.WriteToServerAsync(
            await CreateBulkInsertStreamAsync(Targets),
            new UTF8Encoding(false),
            TimeSpan.FromMinutes(30));

        // Assert
        await AssertAsync();


        transaction.Rollback();

        using var newConnection = new SqlConnection(SqlBulkCopierConnectionString);
        await newConnection.OpenAsync(CancellationToken.None);
        (await newConnection.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM [dbo].[BulkInsertTestTarget]"))
            .ShouldBe(0);
    }

    [Fact]
    public async Task WriteToServerAsync_NoRetry_WithConnection_WithHeaderAndFooter_ShouldSucceed()
    {
        // Arrange
        Encoding encoding = new UTF8Encoding(false);

        var builder = ProvideBuilder();
        SetRowFilter(builder);
            // ビルド
        using var sqlBulkCopier =    builder.Build(await OpenConnectionAsync());

        // ファイルを開いて実行
        await sqlBulkCopier.WriteToServerAsync(
            await CreateBulkInsertStreamAsync(Targets),
            encoding,
            TimeSpan.FromMinutes(30));

        // Assert
        await AssertAsync();
    }

    [Fact]
    public async Task WriteToServerAsync_WithRetry_WithConnection_ShouldError()
    {
        // Arrange
        using var sqlBulkCopier = ProvideBuilder()
            .SetMaxRetryCount(3)
            .Build(await OpenConnectionAsync());

        // ファイルを開いて実行
        using var stream = await CreateBulkInsertStreamAsync(Targets);
        var func = () => sqlBulkCopier.WriteToServerAsync(
            // ReSharper disable once AccessToDisposedClosure
            stream,
            new UTF8Encoding(false),
            TimeSpan.FromMinutes(30));

        // Assert
        func.ShouldThrow<InvalidOperationException>();
    }

    [Fact]
    public async Task WriteToServerAsync_WithRetry_WithConnectionString_ShouldSucceed()
    {
        // Arrange
        using var sqlBulkCopier = ProvideBuilder()
            .SetMaxRetryCount(3)
            .SetTruncateBeforeBulkInsert(true)
            .SetInitialDelay(TimeSpan.FromMilliseconds(1))
            .SetUseExponentialBackoff(false)
            .Build(SqlBulkCopierConnectionString);

        // ファイルを開いて実行
        await sqlBulkCopier.WriteToServerAsync(
            await CreateBulkInsertStreamAsync(Targets),
            new UTF8Encoding(false),
            TimeSpan.FromMinutes(30));

        // Assert
        await AssertAsync();
    }

    [Fact]
    public async Task WriteToServerAsync_WithRetry_WithConnectionString_WithoutTruncate_ShouldError()
    {
        // Arrange
        using var sqlBulkCopier = ProvideBuilder()
            .SetMaxRetryCount(3)
            .Build(SqlBulkCopierConnectionString);

        // ファイルを開いて実行
        using var stream = await CreateBulkInsertStreamAsync(Targets);
        var func = () => sqlBulkCopier.WriteToServerAsync(
            // ReSharper disable once AccessToDisposedClosure
            stream,
            new UTF8Encoding(false),
            TimeSpan.FromMinutes(30));

        // Assert
        func.ShouldThrow<InvalidOperationException>();
    }

    [Fact]
    public async Task WriteToServerAsync_WithRetry_WithConnectionString_WithOptions_ShouldSucceed()
    {
        // Arrange
        using var sqlBulkCopier = ProvideBuilder()
            .SetMaxRetryCount(3)
            .SetTruncateBeforeBulkInsert(true)
            .Build(SqlBulkCopierConnectionString, SqlBulkCopyOptions.Default);

        // ファイルを開いて実行
        await sqlBulkCopier.WriteToServerAsync(
            await CreateBulkInsertStreamAsync(Targets),
            new UTF8Encoding(false),
            TimeSpan.FromMinutes(30));

        // Assert
        await AssertAsync();
    }

    [Fact]
    public async Task WriteToServerAsync_WithRetry_WithConnection_WithTransaction_ShouldError()
    {
        // Arrange
        using var connection = await OpenConnectionAsync();
        using var transaction = connection.BeginTransaction();

        using var sqlBulkCopier = ProvideBuilder()
            .SetMaxRetryCount(3)
            .Build(connection, SqlBulkCopyOptions.Default, transaction);

        // Act & Assert
        using var stream = await CreateBulkInsertStreamAsync(Targets);
        var func = () => sqlBulkCopier.WriteToServerAsync(
            // ReSharper disable once AccessToDisposedClosure
            stream,
            new UTF8Encoding(false),
            TimeSpan.FromMinutes(30));
        func.ShouldThrow<InvalidOperationException>();
    }


    protected abstract TBuilder ProvideBuilder();

    protected abstract Task<Stream> CreateBulkInsertStreamAsync(List<BulkInsertTestTarget> dataList, bool withHeaderAndFooter = false);

    protected abstract void SetRowFilter(TBuilder builder);

    protected async Task<SqlConnection> OpenConnectionAsync()
    {
        var sqlConnection = new SqlConnection(SqlBulkCopierConnectionString);
        await sqlConnection.OpenAsync(CancellationToken.None);
        return sqlConnection;
    }
    private async Task AssertAsync()
    {
        using var sqlConnection = await OpenConnectionAsync();

        var insertedRows = (await sqlConnection.QueryAsync<BulkInsertTestTarget>(
            "SELECT * FROM [dbo].[BulkInsertTestTarget] with(nolock) order by Id")).ToArray();

        insertedRows.ShouldNotBeEmpty("書き出したデータが読み込まれるはず");
        insertedRows.Length.ShouldBe(Count);

        // 先頭行などを必要に応じて検証
        var expected = Targets.First();
        var actual = insertedRows.First();
        ShouldBe(expected, actual);
    }

    /// <summary>
    /// 文字列を指定されたバイト数になるように右側をスペースでパディング
    /// </summary>
    protected static string PadRightBytes(string str, int totalBytes, Encoding encoding)
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

[Collection("Use SqlBulkCopier")]
public abstract class BulkCopierBuilderTestBase
{
    protected const int Count = 100;

    private static readonly string MasterConnectionString = new SqlConnectionStringBuilder
    {
        DataSource = ".",
        InitialCatalog = "master",
        IntegratedSecurity = true,
        TrustServerCertificate = true
    }.ToString();

    protected string SqlBulkCopierConnectionString => new SqlConnectionStringBuilder
    {
        DataSource = ".",
        InitialCatalog = DatabaseName,
        IntegratedSecurity = true,
        TrustServerCertificate = true
    }.ToString();

    private string DatabaseName => this.GetType().Name;

    protected BulkCopierBuilderTestBase()
    {
        using SqlConnection mainConnection = new(MasterConnectionString);
        mainConnection.Open();

        mainConnection.Execute(
            // ReSharper disable StringLiteralTypo
            $"""
             -- 自分自身の接続を除いたユーザープロセスを対象にした接続の強制切断
             DECLARE @kill varchar(8000) = '';
             DECLARE @spid int;

             -- 自分自身のプロセスIDを取得
             SET @spid = @@SPID;

             SELECT @kill = @kill + 'kill ' + CONVERT(varchar(5), spid) + ';'
             FROM master.dbo.sysprocesses 
             WHERE DB_NAME(dbid) = '{DatabaseName}'
             AND spid <> @spid
             AND spid > 50;  -- システムプロセスを除外するため、spid が 50 より大きいものを対象

             EXEC(@kill);
             """);
        mainConnection.Execute($"DROP DATABASE IF EXISTS [{DatabaseName}]");
        mainConnection.Execute($"CREATE DATABASE [{DatabaseName}]");
        mainConnection.Close();

        using SqlConnection sqlConnection = new(SqlBulkCopierConnectionString);
        sqlConnection.Open();

        sqlConnection.Execute(
            """
            CREATE TABLE dbo.BulkInsertTestTarget
            (
                -- 一意に識別するための主キー
                Id INT NOT NULL PRIMARY KEY,
            
                -- 数値系 (Exact Numerics)
                TinyInt TINYINT,
                SmallInt SMALLINT,
                IntValue INT,
                BigInt BIGINT,
                BitValue BIT,
                DecimalValue DECIMAL(11, 2),
                NumericValue NUMERIC(11, 2),
                MoneyValue MONEY,
                SmallMoneyValue SMALLMONEY,
            
                -- 数値系 (Approximate Numerics)
                FloatValue FLOAT,
                RealValue REAL,
            
                -- 日付・時刻系
                DateValue DATE,
                TimeValue TIME(7),
                DateTimeValue DATETIME,
                SmallDateTimeValue SMALLDATETIME,
                DateTime2Value DATETIME2(7),
                DateTimeOffsetValue DATETIMEOFFSET(7),
            
                -- 文字列系
                CharValue CHAR(10),
                VarCharValue VARCHAR(50),
                NCharValue NCHAR(10),
                NVarCharValue NVARCHAR(50),
            
                -- バイナリ系
                BinaryValue BINARY(10),
                VarBinaryValue VARBINARY(50),
            
                -- その他（一般的に使用されるもの）
                UniqueIdValue UNIQUEIDENTIFIER,
                XmlValue XML
            );
            """);

        sqlConnection.Execute(
            """
            -- テーブルの作成
            CREATE TABLE dbo.Customer (
                CustomerId INT PRIMARY KEY,
                FirstName NVARCHAR(50),
                LastName NVARCHAR(50),
                Email NVARCHAR(100),
                PhoneNumber NVARCHAR(20),
                AddressLine1 NVARCHAR(100),
                AddressLine2 NVARCHAR(100),
                City NVARCHAR(50),
                State NVARCHAR(50),
                PostalCode NVARCHAR(10),
                Country NVARCHAR(50),
                BirthDate DATE,
                Gender NVARCHAR(10),
                Occupation NVARCHAR(50),
                Income DECIMAL(18,2),
                RegistrationDate DATETIME,
                LastLogin DATETIME,
                IsActive BIT,
                Notes NVARCHAR(MAX),
                CreatedAt DATETIME DEFAULT GETDATE(),  -- デフォルト値として GetDate() を設定
                UpdatedAt DATETIME DEFAULT GETDATE()   -- デフォルト値として GetDate() を設定
            )
            """);
    }

    /// <summary>
    /// Bogusを使ってテスト用データを生成
    /// </summary>
    // ReSharper disable once UnusedMember.Local
    protected static List<BulkInsertTestTarget> GenerateBulkInsertTestTargetData(int rowCount)
    {
        var idSeed = 0;

        var faker = new Faker<BulkInsertTestTarget>("ja")
            // int? Id → 連番、NULLになっても良いが例として常に設定
            .RuleFor(x => x.Id, _ => ++idSeed)

            // byte? TinyInt → 0～255
            .RuleFor(x => x.TinyInt, f => f.Random.Byte())

            // short? SmallInt → 0～32767
            .RuleFor(x => x.SmallInt, f => f.Random.Short(0))

            // int? IntValue → 0～int.MaxValue
            .RuleFor(x => x.IntValue, f => f.Random.Int(0))

            // long? BigInt → 0～long.MaxValue
            .RuleFor(x => x.BigInt, f => f.Random.Long(0))

            // bool? BitValue
            .RuleFor(x => x.BitValue, f => f.Random.Bool())

            // decimal? DecimalValue, NumericValue, MoneyValue, SmallMoneyValue
            .RuleFor(x => x.DecimalValue, f => f.Finance.Amount(0, 999999999))
            .RuleFor(x => x.NumericValue, f => f.Finance.Amount(0, 999999999))
            .RuleFor(x => x.MoneyValue, f => f.Finance.Amount(0, 999999999))
            .RuleFor(x => x.SmallMoneyValue, f => f.Finance.Amount(0, 214748)) // 小さめの範囲

            // double? FloatValue
            .RuleFor(x => x.FloatValue, f => f.Random.Double())

            // float? RealValue
            .RuleFor(x => x.RealValue, f => f.Random.Float())

            // DateTime? 系
            .RuleFor(x => x.DateValue, f => f.Date.Past(5))        // DATE
            .RuleFor(x => x.DateTimeValue, f => f.Date.Past(5))    // DATETIME
            .RuleFor(x => x.SmallDateTimeValue, f => f.Date.Past(5))
            .RuleFor(x => x.DateTime2Value, f => f.Date.Past(5))

            // TimeSpan? TimeValue → Bogusでは DateTime のうちTime部分を使うなど工夫
            .RuleFor(x => x.TimeValue, f => f.Date.Past(0).TimeOfDay)

            // DateTimeOffset? DateTimeOffsetValue
            .RuleFor(x => x.DateTimeOffsetValue, f => f.Date.RecentOffset(5))

            // string? CharValue, VarCharValue, NCharValue, NVarCharValue
            .RuleFor(x => x.CharValue, f => f.Random.AlphaNumeric(10))
            .RuleFor(x => x.VarCharValue, f => f.Random.Words(3).Truncate(40))
            .RuleFor(x => x.NCharValue, f => f.Random.String2(10))
            .RuleFor(x => x.NVarCharValue, f => f.Lorem.Sentence(3).Truncate(50))

            // byte[]? BinaryValue, VarBinaryValue → 適当にバイト列を生成
            .RuleFor(x => x.BinaryValue, f => f.Random.Bytes(10))
            .RuleFor(x => x.VarBinaryValue, f => f.Random.Bytes(50))

            // Guid? UniqueIdValue
            .RuleFor(x => x.UniqueIdValue, f => f.Random.Guid())

            // string? XmlValue → XMLっぽい文字列を適当に
            .RuleFor(x => x.XmlValue, f => $"<root><value>{f.Random.Int()}</value></root>");

        return faker.Generate(rowCount);
    }

    public static List<Customer> GenerateCustomers(int count)
    {
        var idSeed = 0;
        var faker = new Faker<Customer>("ja")
            .RuleFor(x => x.CustomerId, _ => ++idSeed)
            .RuleFor(x => x.FirstName, f => f.Name.FirstName())
            .RuleFor(x => x.LastName, f => f.Name.LastName())
            .RuleFor(x => x.Email, f => f.Internet.Email())
            .RuleFor(x => x.PhoneNumber, f => f.Phone.PhoneNumber())
            .RuleFor(x => x.AddressLine1, f => f.Address.StreetAddress())
            .RuleFor(x => x.AddressLine2, f => f.Address.SecondaryAddress())
            .RuleFor(x => x.City, f => f.Address.City())
            .RuleFor(x => x.State, f => f.Address.StateAbbr())
            .RuleFor(x => x.PostalCode, f => f.Address.ZipCode())
            .RuleFor(x => x.Country, f => f.Address.Country().Truncate(50))
            .RuleFor(x => x.BirthDate, f => f.Date.Past(40, DateTime.Now.AddYears(-20))) // 20〜60歳ぐらい
            .RuleFor(x => x.Gender, f => f.PickRandom("Male", "Female", "Other"))
            .RuleFor(x => x.Occupation, f => f.Name.JobTitle())
            .RuleFor(x => x.Income, f => f.Finance.Amount(1000, 1000000))  // 年収を大まかに
            .RuleFor(x => x.RegistrationDate, f => f.Date.Past(3))         // 過去3年ぐらい
            .RuleFor(x => x.LastLogin, f => f.Date.Recent(30))             // 過去1ヶ月
            .RuleFor(x => x.IsActive, f => f.Random.Bool(0.8f))            // 80% を true
            .RuleFor(x => x.Notes, f => f.Lorem.Sentence())

            // テーブル上は DEFAULT GETDATE() のため本来必須ではない
            // 例として現在時刻を設定しておく
            .RuleFor(x => x.CreatedAt, DateTime.Now)
            .RuleFor(x => x.UpdatedAt, DateTime.Now);

        return faker.Generate(count);
    }

    protected static void ShouldBe(BulkInsertTestTarget expected, BulkInsertTestTarget actual)
    {
        actual.Id.ShouldBe(expected.Id);
        actual.TinyInt.ShouldBe(expected.TinyInt);
        actual.SmallInt.ShouldBe(expected.SmallInt);
        actual.IntValue.ShouldBe(expected.IntValue);
        actual.BigInt.ShouldBe(expected.BigInt);
        actual.BitValue.ShouldBe(expected.BitValue);
        actual.DecimalValue.ShouldBe(expected.DecimalValue);
        actual.NumericValue.ShouldBe(expected.NumericValue);
        actual.MoneyValue.ShouldBe(expected.MoneyValue);
        actual.SmallMoneyValue.ShouldBe(expected.SmallMoneyValue);
        actual.FloatValue!.Value.ShouldBe(expected.FloatValue!.Value, 0.001d);
        actual.RealValue!.Value.ShouldBe(expected.RealValue!.Value, 0.001d);
        actual.DateValue.ShouldBe(new DateTime(expected.DateValue!.Value.Year, expected.DateValue!.Value.Month, expected.DateValue!.Value.Day));
        actual.DateTimeValue.ShouldBe(DateTime.Parse(expected.DateTimeValue!.Value.ToString("yyyy/MM/dd HH:mm:ss")));
        Math.Abs((actual.SmallDateTimeValue!.Value - expected.SmallDateTimeValue!.Value).TotalMinutes).ShouldBeLessThan(1);
        actual.DateTime2Value.ShouldBe(DateTime.Parse(expected.DateTime2Value!.Value.ToString("yyyy/MM/dd HH:mm:ss")));
        actual.TimeValue.ShouldBe(new TimeSpan(expected.TimeValue!.Value.Hours, expected.TimeValue!.Value.Minutes, expected.TimeValue!.Value.Seconds));
        actual.DateTimeOffsetValue.ShouldBe(DateTime.Parse(expected.DateTimeOffsetValue!.Value.ToString("yyyy/MM/dd HH:mm")));
        actual.CharValue.ShouldBe(expected.CharValue?.TrimEnd());
        actual.VarCharValue.ShouldBe(expected.VarCharValue?.TrimEnd());
        actual.NCharValue.ShouldBe(expected.NCharValue?.TrimEnd());
        actual.BinaryValue.ShouldBeEquivalentTo(expected.BinaryValue);
        actual.VarBinaryValue.ShouldBeEquivalentTo(expected.VarBinaryValue);
        actual.UniqueIdValue.ShouldBe(expected.UniqueIdValue);
        actual.XmlValue.ShouldBe(expected.XmlValue);

    }
}