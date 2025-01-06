using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Jobs;

namespace Benchmark;

public class TestConfig : ManualConfig
{
    public TestConfig()
    {
        AddJob(Job.ShortRun);
        //AddJob(Job.Default);
        //AddJob(Job.Default.WithStrategy(RunStrategy.ColdStart));

        // 最適化検証を無効化
        //WithOptions(ConfigOptions.DisableOptimizationsValidator);
    }
}