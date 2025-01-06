using Benchmark;
using BenchmarkDotNet.Running;

var summary = BenchmarkRunner.Run<SqlBulkCopierBenchmarks>();

//var benchmarks = new SqlBulkCopierBenchmarks();
//benchmarks.Setup();
//await benchmarks.CsvEfCore();