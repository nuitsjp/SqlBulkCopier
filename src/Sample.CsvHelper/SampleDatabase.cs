using Dapper;
using Microsoft.Data.SqlClient;

namespace Sample.CsvHelper
{
    public static class SampleDatabase
    {
        public const string DatabaseName = "SqlBulkCopier";

        public static async Task SetupAsync()
        {
            // Create a new database
            await CreateDatabaseAsync();

            // Set up the database
            await SetupDatabase();
        }

        private static async Task SetupDatabase()
        {
            var connectionString = new SqlConnectionStringBuilder
            {
                DataSource = ".",
                InitialCatalog = DatabaseName,
                IntegratedSecurity = true,
                TrustServerCertificate = true
            }.ToString();

            await using SqlConnection connection = new(connectionString);
            connection.Open();

            await connection.ExecuteAsync("DROP TABLE IF EXISTS dbo.Customer");
            await connection.ExecuteAsync(
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

        private static async Task CreateDatabaseAsync()
        {
            var connectionString = new SqlConnectionStringBuilder
            {
                DataSource = ".",
                InitialCatalog = "master",
                IntegratedSecurity = true,
                TrustServerCertificate = true
            }.ToString();

            await using SqlConnection connection = new(connectionString);
            await connection.ExecuteAsync(
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
            await connection.ExecuteAsync($"DROP DATABASE IF EXISTS [{DatabaseName}]");
            await connection.ExecuteAsync($"CREATE DATABASE [{DatabaseName}]");
        }

    }
}