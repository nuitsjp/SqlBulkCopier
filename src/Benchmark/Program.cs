using System.Diagnostics;
using Benchmark;

var benchmarks = new SqlBulkCopierBenchmarks();
var benchmarkItems = benchmarks.GetBenchmarkItems();

List<BenchmarkResult> results = [];
const int Count = 5;
for (var i = 0; i < Count; i++)
{
    Console.WriteLine($"Iteration: {i + 1} / {Count}");
    foreach (var item in benchmarkItems)
    {
        Console.WriteLine($"FileType: {item.FileType}, Name: {item.Name}...");
        benchmarks.Setup();
        var stopwatch = Stopwatch.StartNew();
        await item.BenchmarkAsync();
        stopwatch.Stop();
        Console.WriteLine($"Elapsed: {stopwatch.ElapsedMilliseconds}ms");
        results.Add(new BenchmarkResult(item.FileType, item.Name, stopwatch.Elapsed));
    }
}

// 結果の集計処理
var aggregatedResults = results
    .GroupBy(r => new { r.FileType, r.Name })
    .Select(group =>
    {
        var elapsedTicks = group.Select(r => r.Elapsed.Ticks).ToList();
        // 中央値の計算
        var sortedTicks = elapsedTicks.OrderBy(t => t).ToList();
        var median = sortedTicks[sortedTicks.Count / 2];

        // 中央値から倍以上乖離しているデータを除外
        var validResults = group.Where(r =>
        {
            var ratio = (double)r.Elapsed.Ticks / median;
            return ratio >= 0.5 && ratio <= 2.0;
        });

        // 平均値の計算
        var averageTimeSpan = TimeSpan.FromTicks((long)validResults.Average(r => r.Elapsed.Ticks));

        return new BenchmarkResult(
            group.Key.FileType,
            group.Key.Name,
            averageTimeSpan
        );
    })
    .ToList();

// 集計結果の表示
foreach (var result in aggregatedResults)
{
    Console.WriteLine($"FileType: {result.FileType}, Name: {result.Name}, Average: {result.Elapsed.TotalMilliseconds}ms");
}


public record BenchmarkResult(string FileType, string Name, TimeSpan Elapsed);

//var summary = BenchmarkRunner.Run<SqlBulkCopierBenchmarks>();

//benchmarks.Setup();
//var stopwatch = Stopwatch.StartNew();
//await benchmarks.EfCoreWithBulkExtensionsFromCsv();
//stopwatch.Stop();
//Console.WriteLine($"Elapsed: {stopwatch.ElapsedMilliseconds}ms");

//benchmarks.Setup();
//stopwatch.Restart();
//await benchmarks.SqlBulkCopierFromCsv();
//stopwatch.Stop();
//Console.WriteLine($"Elapsed: {stopwatch.ElapsedMilliseconds}ms");


//benchmarks.Setup();
//stopwatch.Restart();
//await benchmarks.NativeBulkInsertFromCsv();
//stopwatch.Stop();
//Console.WriteLine($"Elapsed: {stopwatch.ElapsedMilliseconds}ms");