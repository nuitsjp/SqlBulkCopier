using Benchmark;
using BenchmarkDotNet.Running;

// ReSharper disable once UnusedVariable
var summary = BenchmarkRunner.Run<CsvBenchmarks>();

