using BenchmarkDotNet.Attributes;
using Dapper;
using Microsoft.Data.SqlClient;
using Sample.SetupSampleDatabase;

namespace Benchmark;

public class BenchmarksBase
{
    protected static readonly TimeSpan CommandTimeout = TimeSpan.FromMinutes(30);

    protected const string ArtifactsPath = @"C:\Repos\SqlBulkCopier\src\Sample.SetupSampleDatabase\Asserts";

    [Params(
        //1_000
        //10_000
        100_000
        //, 1_000_000
        //, 10_000_000
    )]
    public int Count = 100_000;

    protected string CsvFile => $@"{ArtifactsPath}\Customer_{Count:###_###_###_###}.csv";
    protected string FixedLengthFile => $@"{ArtifactsPath}\Customer_{Count:###_###_###_###}.dat";

    [IterationSetup]
    public void Setup()
    {
        if (File.Exists(CsvFile) is false)
        {
            Console.WriteLine("Create CSV file...");
            Customer.WriteCsvAsync(CsvFile, Count).Wait();
            Console.WriteLine();
        }
        if (File.Exists(FixedLengthFile) is false)
        {
            Console.WriteLine("Create Fixed Length file...");
            Customer.WriteFixedLengthAsync(FixedLengthFile, Count).Wait();
            Console.WriteLine();
        }

        // Database.SetupAsync(true).Wait();

        using var connection = Database.Open();
        connection.Execute(
            """
            USE SqlBulkCopier;

            truncate table [SqlBulkCopier].[dbo].[Customer]

            DBCC SHRINKFILE ('SqlBulkCopier_log', 1);

            DECLARE @DataFileSize INT;
            DECLARE @LogFileSize INT;
            DECLARE @TargetDataSize INT = 5120; -- 目標データファイルサイズ（MB）
            DECLARE @TargetLogSize INT = 10240; -- 目標ログファイルサイズ（MB）

            -- 現在のファイルサイズを取得（MB単位）
            SELECT @DataFileSize = size/128
            FROM sys.master_files
            WHERE database_id = DB_ID('SqlBulkCopier')
            AND name = 'SqlBulkCopier';

            SELECT @LogFileSize = size/128
            FROM sys.master_files
            WHERE database_id = DB_ID('SqlBulkCopier')
            AND name = 'SqlBulkCopier_log';

            -- データファイルのサイズチェックと変更
            IF @DataFileSize < @TargetDataSize
            BEGIN
                ALTER DATABASE SqlBulkCopier 
                MODIFY FILE (
                    NAME = 'SqlBulkCopier',
                    SIZE = 5120MB
                );
            END

            -- ログファイルのサイズチェックと変更
            IF @LogFileSize < @TargetLogSize
            BEGIN
                ALTER DATABASE SqlBulkCopier 
                MODIFY FILE (
                    NAME = 'SqlBulkCopier_log',
                    SIZE = 10240MB
                );
            END

            """
        );
    }

    protected void AssertResultCount(SqlConnection connection)
    {
        var count = connection.ExecuteScalar<int>("SELECT COUNT(*) FROM [dbo].[Customer]");
        if (count != Count)
        {
            throw new InvalidOperationException($"The number of records is incorrect. Expected: {Count}, Actual: {count}");
        }
    }
}