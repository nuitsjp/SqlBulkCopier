```

BenchmarkDotNet v0.14.0, Windows 11 (10.0.22631.4602/23H2/2023Update/SunValley3)
13th Gen Intel Core i9-13900H, 1 CPU, 20 logical and 14 physical cores
.NET SDK 9.0.101
  [Host]   : .NET 8.0.11 (8.0.1124.51707), X64 RyuJIT AVX2 [AttachedDebugger]
  ShortRun : .NET 8.0.11 (8.0.1124.51707), X64 RyuJIT AVX2

Job=ShortRun  InvocationCount=1  IterationCount=3  
LaunchCount=1  RunStrategy=ColdStart  UnrollFactor=1  
WarmupCount=3  

```
| Method                            | Count   | Mean         | Error        | StdDev       | Median       |
|---------------------------------- |-------- |-------------:|-------------:|-------------:|-------------:|
| **&#39;CSV : BULK INSERT&#39;**               | **10000**   |     **84.53 ms** |    **163.08 ms** |     **8.939 ms** |     **84.81 ms** |
| &#39;CSV : SqlBulkCopier&#39;             | 10000   |    235.14 ms |  1,362.06 ms |    74.659 ms |    194.01 ms |
| &#39;CSV : CsvHelper and Dapper Plus&#39; | 10000   |    396.28 ms |  4,660.28 ms |   255.446 ms |    263.96 ms |
| &#39;CSV : EF Core Bulk Extensions&#39;   | 10000   |    198.58 ms |  2,755.93 ms |   151.062 ms |    129.59 ms |
| &#39;Fixed Length : BULK INSERT&#39;      | 10000   |    205.84 ms |  2,763.00 ms |   151.450 ms |    123.18 ms |
| &#39;Fixed Length : SqlBulkCopier&#39;    | 10000   |    403.02 ms |  2,944.97 ms |   161.424 ms |    330.89 ms |
| **&#39;CSV : BULK INSERT&#39;**               | **100000**  |    **862.88 ms** |  **2,808.71 ms** |   **153.955 ms** |    **791.35 ms** |
| &#39;CSV : SqlBulkCopier&#39;             | 100000  |  1,221.87 ms | 12,453.84 ms |   682.637 ms |    910.23 ms |
| &#39;CSV : CsvHelper and Dapper Plus&#39; | 100000  |  1,904.72 ms |  7,885.92 ms |   432.254 ms |  1,803.65 ms |
| &#39;CSV : EF Core Bulk Extensions&#39;   | 100000  |    588.65 ms |  5,739.57 ms |   314.605 ms |    546.96 ms |
| &#39;Fixed Length : BULK INSERT&#39;      | 100000  |  2,073.55 ms | 10,349.30 ms |   567.280 ms |  1,917.81 ms |
| &#39;Fixed Length : SqlBulkCopier&#39;    | 100000  |  2,824.31 ms | 15,134.33 ms |   829.564 ms |  2,495.85 ms |
| **&#39;CSV : BULK INSERT&#39;**               | **1000000** | **10,103.54 ms** | **18,203.28 ms** |   **997.783 ms** |  **9,812.12 ms** |
| &#39;CSV : SqlBulkCopier&#39;             | 1000000 |  9,246.17 ms | 17,849.29 ms |   978.380 ms |  8,940.25 ms |
| &#39;CSV : CsvHelper and Dapper Plus&#39; | 1000000 | 16,834.91 ms | 30,157.58 ms | 1,653.039 ms | 16,789.33 ms |
| &#39;CSV : EF Core Bulk Extensions&#39;   | 1000000 |  3,336.95 ms |  5,490.15 ms |   300.934 ms |  3,166.24 ms |
| &#39;Fixed Length : BULK INSERT&#39;      | 1000000 | 25,546.74 ms | 68,451.08 ms | 3,752.035 ms | 27,441.13 ms |
| &#39;Fixed Length : SqlBulkCopier&#39;    | 1000000 | 25,296.11 ms | 64,513.01 ms | 3,536.176 ms | 26,150.81 ms |
