using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Exporters.Csv;
using BenchmarkDotNet.Jobs;

namespace Benchmark;

public class TestConfig : ManualConfig
{
    public TestConfig()
    {
        AddJob(Job.MediumRun
            .WithWarmupCount(1));
        AddDiagnoser(MemoryDiagnoser.Default);
        AddColumnProvider(DefaultColumnProviders.Instance);
        AddExporter(CsvExporter.Default);
    }
}