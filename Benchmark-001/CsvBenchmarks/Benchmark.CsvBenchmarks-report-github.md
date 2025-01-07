```

BenchmarkDotNet v0.14.0, Windows 11 (10.0.22631.4602/23H2/2023Update/SunValley3)
13th Gen Intel Core i9-13900H, 1 CPU, 20 logical and 14 physical cores
.NET SDK 9.0.101
  [Host]    : .NET 9.0.0 (9.0.24.52809), X64 RyuJIT AVX2 [AttachedDebugger]
  MediumRun : .NET 9.0.0 (9.0.24.52809), X64 RyuJIT AVX2

Job=MediumRun  InvocationCount=1  IterationCount=15  
LaunchCount=2  UnrollFactor=1  WarmupCount=1  

```
| Method        | Count    | Mean    | Error    | StdDev   | Ratio | RatioSD | Gen0        | Gen1       | Gen2      | Allocated      | Alloc Ratio |
|-------------- |--------- |--------:|---------:|---------:|------:|--------:|------------:|-----------:|----------:|---------------:|------------:|
| &#39;BULK INSERT&#39; | 10000000 | 2.022 m | 0.0685 m | 0.1004 m |  0.99 |    0.07 |   7000.0000 |  7000.0000 |         - |       20.38 KB |       0.000 |
| SqlBulkCopier | 10000000 | 2.045 m | 0.0683 m | 0.0980 m |  1.00 |    0.07 | 882000.0000 | 12000.0000 | 1000.0000 | 10800740.41 KB |       1.000 |
