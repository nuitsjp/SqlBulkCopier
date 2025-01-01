using Bogus;
using Dapper;
using FluentAssertions;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier.Test
{
    [Collection("Use SqlBulkCopier")]
    public abstract class BulkCopierBuilderTestBase
    {
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
            InitialCatalog = _databaseName,
            IntegratedSecurity = true,
            TrustServerCertificate = true
        }.ToString();

        private readonly string _databaseName;

        protected BulkCopierBuilderTestBase(string databaseName)
        {
            _databaseName = databaseName;
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
                 WHERE DB_NAME(dbid) = '{databaseName}'
                 AND spid <> @spid
                 AND spid > 50;  -- システムプロセスを除外するため、spid が 50 より大きいものを対象

                 EXEC(@kill);
                 """);
            mainConnection.Execute($"DROP DATABASE IF EXISTS [{databaseName}]");
            mainConnection.Execute($"CREATE DATABASE [{databaseName}]");
            mainConnection.Close();

            using SqlConnection sqlConnection = new(SqlBulkCopierConnectionString);
            sqlConnection.Open();

            sqlConnection.Execute(
                """
                CREATE TABLE dbo.BulkInsertTestTarget
                (
                    -- 一意に識別するための主キー
                    Id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                
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
            actual.Id.Should().Be(expected.Id);
            actual.TinyInt.Should().Be(expected.TinyInt);
            actual.SmallInt.Should().Be(expected.SmallInt);
            actual.IntValue.Should().Be(expected.IntValue);
            actual.BigInt.Should().Be(expected.BigInt);
            actual.BitValue.Should().Be(expected.BitValue);
            actual.DecimalValue.Should().Be(expected.DecimalValue);
            actual.NumericValue.Should().Be(expected.NumericValue);
            actual.MoneyValue.Should().Be(expected.MoneyValue);
            actual.SmallMoneyValue.Should().Be(expected.SmallMoneyValue);
            actual.FloatValue.Should().BeApproximately(expected.FloatValue, 0.001f);
            actual.RealValue.Should().BeApproximately(expected.RealValue, 0.001f);
            actual.DateValue.Should().Be(new DateTime(expected.DateValue!.Value.Year, expected.DateValue!.Value.Month, expected.DateValue!.Value.Day));
            actual.DateTimeValue.Should().Be(DateTime.Parse(expected.DateTimeValue!.Value.ToString("yyyy/MM/dd HH:mm:ss")));
            Math.Abs((actual.SmallDateTimeValue!.Value - expected.SmallDateTimeValue!.Value).TotalMinutes).Should().BeLessThan(1);
            actual.DateTime2Value.Should().Be(DateTime.Parse(expected.DateTime2Value!.Value.ToString("yyyy/MM/dd HH:mm:ss")));
            actual.TimeValue.Should().Be(new TimeSpan(expected.TimeValue!.Value.Hours, expected.TimeValue!.Value.Minutes, expected.TimeValue!.Value.Seconds));
            actual.DateTimeOffsetValue.Should().Be(DateTime.Parse(expected.DateTimeOffsetValue!.Value.ToString("yyyy/MM/dd HH:mm")));
            actual.CharValue.Should().Be(expected.CharValue?.TrimEnd());
            actual.VarCharValue.Should().Be(expected.VarCharValue?.TrimEnd());
            actual.NCharValue.Should().Be(expected.NCharValue?.TrimEnd());
            actual.BinaryValue.Should().BeEquivalentTo(expected.BinaryValue);
            actual.VarBinaryValue.Should().BeEquivalentTo(expected.VarBinaryValue);
            actual.UniqueIdValue.Should().Be(expected.UniqueIdValue);
            actual.XmlValue.Should().Be(expected.XmlValue);

        }
    }
}