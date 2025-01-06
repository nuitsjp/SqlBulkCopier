using System.Diagnostics;
using Benchmark;
using BenchmarkDotNet.Running;

var summary = BenchmarkRunner.Run<SqlBulkCopierBenchmarks>();

//var benchmarks = new SqlBulkCopierBenchmarks();
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