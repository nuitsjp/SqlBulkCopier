```

BenchmarkDotNet v0.14.0, Windows 11 (10.0.22631.4602/23H2/2023Update/SunValley3)
AMD Ryzen 9 7950X, 1 CPU, 32 logical and 16 physical cores
.NET SDK 9.0.100-rc.2.24474.11
  [Host]    : .NET 9.0.0 (9.0.24.47305), X64 RyuJIT AVX-512F+CD+BW+DQ+VL+VBMI [AttachedDebugger]
  MediumRun : .NET 9.0.0 (9.0.24.47305), X64 RyuJIT AVX-512F+CD+BW+DQ+VL+VBMI

Job=MediumRun  InvocationCount=1  IterationCount=15  
LaunchCount=2  UnrollFactor=1  WarmupCount=1  

```
| Method        | Count    | Mean          | Error        | StdDev       | Median        | Ratio | RatioSD | Gen0         | Gen1      | Allocated      | Alloc Ratio  |
|-------------- |--------- |--------------:|-------------:|-------------:|--------------:|------:|--------:|-------------:|----------:|---------------:|-------------:|
| **&#39;BULK INSERT&#39;** | **1000**     |      **12.79 ms** |     **0.246 ms** |     **0.361 ms** |      **12.78 ms** |  **1.00** |    **0.04** |            **-** |         **-** |        **7.16 KB** |         **1.00** |
| SqlBulkCopier | 1000     |      13.73 ms |     0.705 ms |     0.916 ms |      13.52 ms |  1.07 |    0.08 |            - |         - |     4678.21 KB |       653.01 |
|               |          |               |              |              |               |       |         |              |           |                |              |
| **&#39;BULK INSERT&#39;** | **100000**   |   **1,133.98 ms** |   **324.420 ms** |   **485.577 ms** |     **825.25 ms** |  **1.15** |    **0.62** |            **-** |         **-** |        **7.31 KB** |         **1.00** |
| SqlBulkCopier | 100000   |   1,106.19 ms |   241.257 ms |   338.208 ms |     967.26 ms |  1.12 |    0.49 |   28000.0000 |         - |   467640.77 KB |    63,950.88 |
|               |          |               |              |              |               |       |         |              |           |                |              |
| **&#39;BULK INSERT&#39;** | **10000000** | **122,136.81 ms** |   **346.039 ms** |   **507.219 ms** | **122,090.02 ms** |  **1.00** |    **0.01** |            **-** |         **-** |       **22.63 KB** |         **1.00** |
| SqlBulkCopier | 10000000 | 136,659.96 ms | 1,612.352 ms | 2,413.293 ms | 135,980.22 ms |  1.12 |    0.02 | 2827000.0000 | 8000.0000 | 46174234.03 KB | 2,040,145.65 |
