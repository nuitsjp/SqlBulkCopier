```

BenchmarkDotNet v0.14.0, Windows 11 (10.0.26100.2605)
AMD Ryzen 9 7950X, 1 CPU, 32 logical and 16 physical cores
.NET SDK 9.0.101
  [Host]   : .NET 9.0.0 (9.0.24.52809), X64 RyuJIT AVX-512F+CD+BW+DQ+VL+VBMI [AttachedDebugger]
  ShortRun : .NET 9.0.0 (9.0.24.52809), X64 RyuJIT AVX-512F+CD+BW+DQ+VL+VBMI

Job=ShortRun  InvocationCount=1  IterationCount=7  
LaunchCount=1  RunStrategy=ColdStart  UnrollFactor=1  
WarmupCount=0  

```
| Method                 | Count  | Mean        | Error      | StdDev   | Median      | Ratio | RatioSD | Gen0        | Gen1        | Allocated   | Alloc Ratio |
|----------------------- |------- |------------:|-----------:|---------:|------------:|------:|--------:|------------:|------------:|------------:|------------:|
| SqlBulkCopier          | 100000 |    668.8 ms |   766.5 ms | 340.3 ms |    479.6 ms |  1.18 |    0.70 |   6000.0000 |           - |   101.93 MB |        1.00 |
| &#39;Dapper Single Insert&#39; | 100000 | 27,716.4 ms | 1,694.8 ms | 752.5 ms | 27,221.0 ms | 48.83 |   15.98 | 781000.0000 | 135000.0000 | 12471.93 MB |      122.35 |
