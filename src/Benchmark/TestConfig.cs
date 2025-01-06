using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Jobs;

namespace Benchmark;

public class TestConfig : ManualConfig
{
    public TestConfig()
    {
        //AddJob(Job.Dry);
        AddJob(Job.ShortRun
            .WithIterationCount(7)
            .WithLaunchCount(1)
            .WithWarmupCount(0)
            .WithStrategy(RunStrategy.ColdStart));

        AddDiagnoser([MemoryDiagnoser.Default]);
        AddColumnProvider(DefaultColumnProviders.Instance);
    }
}