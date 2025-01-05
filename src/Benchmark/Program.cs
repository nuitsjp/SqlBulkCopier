using Benchmark;

await new SqlBulkCopierBenchmarks()
    .RunAsync();