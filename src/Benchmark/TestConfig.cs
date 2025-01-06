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
        //AddJob(Job.Default);
        //AddJob(Job.Default.WithStrategy(RunStrategy.ColdStart));

        // 最適化検証を無効化
        AddDiagnoser([MemoryDiagnoser.Default]);
        AddColumnProvider(DefaultColumnProviders.Instance);
        WithOptions(ConfigOptions.DisableOptimizationsValidator);
    }
}