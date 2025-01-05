using System.Diagnostics;
using Dapper;
using FluentTextTable;
using Sample.SetupSampleDatabase;

namespace Benchmark;

public class SqlBulkCopierBenchmarks
{
    public async Task RunAsync()
    {
        await SetupAsync();

        (string Name, Func<Task> Task)[] benchmarks =
        [
            ("SQL BULK INSERT", NativeBulkInsert)
        ];

        List<Result> results = [];
        foreach (var benchmark in benchmarks)
        {
            await TruncateAsync();

            var stopwatch = Stopwatch.StartNew();
            await benchmark.Task();
            stopwatch.Stop();
            results.Add(new Result(benchmark.Name, stopwatch.Elapsed));
        }

        Build
            .TextTable<Result>()
            .WriteLine(results);
    }

    public async Task SetupAsync()
    {
        await Database.SetupAsync(true);
    }

    public async Task TruncateAsync()
    {
        await using var connection = Database.Open();
        await connection.ExecuteAsync(
            """
            USE SqlBulkCopier;

            truncate table [SqlBulkCopier].[dbo].[Customer]

            DBCC SHRINKFILE ('SqlBulkCopier_log', 1);
            """
        );
    }

    public async Task NativeBulkInsert()
    {
        await using var connection = Database.Open();
        await connection.ExecuteAsync(
            """
            BULK INSERT SqlBulkCopier.dbo.Customer
            FROM 'D:\SqlBulkCopier\src\Sample.SetupSampleDatabase\Asserts\Customer.csv'
            WITH
            (
                FORMATFILE = 'D:\SqlBulkCopier\src\Sample.SetupSampleDatabase\Asserts\Customer.fmt',
                FIRSTROW = 2,    -- ヘッダー行をスキップ
                DATAFILETYPE = 'char',
                CODEPAGE = '65001'  -- UTF-8エンコーディングを指定
            );
            """
        );
    }
}


public record Result(string Name, TimeSpan Time);