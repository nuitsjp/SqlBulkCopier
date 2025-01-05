using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;

namespace Benchmark;

[SimpleJob(RuntimeMoniker.Net80, launchCount: 1, warmupCount: 0, iterationCount: 1)]
public class SqlBulkCopierBenchmarks
{
    [Benchmark]
    public void NativeBulkInsert()
    {
        // 重たい処理をここに記述
    }
}