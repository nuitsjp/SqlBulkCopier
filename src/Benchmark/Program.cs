using System.Diagnostics;
using Benchmark;
using BenchmarkDotNet.Running;

var summary1 = BenchmarkRunner.Run<CsvBenchmarks>();

