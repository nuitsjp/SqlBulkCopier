# CSVのFluent APIの使い方

## 目次
- [はじめに](#はじめに)
- [Getting Started](#getting-started)
- [APIの詳細](#apiの詳細)

## はじめに
このドキュメントでは、SqlBulkCopierライブラリのCSV用Fluent APIの使い方について説明します。Fluent APIを使用することで、CSVデータを効率的にデータベースにバルクコピーすることができます。

## Getting Started

このライブラリは、.NET 8.0 または .NET Framework 4.8が必要です。NuGetから以下のパッケージをインストールしてください：

### CSV用パッケージ
```
Install-Package SqlBulkCopier.CsvHelper
```

以下は、CSVファイルをFluent APIで設定し、`Microsoft.Extensions.Hosting`を使用してGeneric Hostに対応したコンソールアプリケーションを構築する簡単なサンプルコードです。詳細な設定やその他の例については、該当する詳細ドキュメントページをご参照ください。

### Program.cs

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Sample.CsvHelper.FromApi;
using SqlBulkCopier.CsvHelper.Hosting;

var builder = Host.CreateApplicationBuilder(args);

builder.Configuration
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json");

builder.Services
    .AddHostedService<BulkCopyService>()
    .AddSqlBulkCopier();

await builder.Build().RunAsync();
```

### BulkCopyService.cs

```csharp
using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using SqlBulkCopier.CsvHelper;

namespace Sample.CsvHelper.FromApi;

public class BulkCopyService(
    IConfiguration configuration,
    IHostApplicationLifetime applicationLifetime) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var bulkCopier = CsvBulkCopierBuilder
            .CreateWithHeader("[dbo].[Customer]")
            .SetTruncateBeforeBulkInsert(true)
            .SetDefaultColumnContext(c => c.TrimEnd().TreatEmptyStringAsNull())
            .AddColumnMapping("CustomerId")
            .AddColumnMapping("FirstName")
            .AddColumnMapping("LastName")
            .AddColumnMapping("BirthDate", c => c.AsDate("yyyy-MM-dd"))
            .AddColumnMapping("IsActive", c => c.AsBit())
            .Build(configuration.GetConnectionString("DefaultConnection")!);

        await using var stream = File.OpenRead(
            Path.Combine(AppContext.BaseDirectory, "Assets", "Customer.csv"));
        await bulkCopier.WriteToServerAsync(stream, Encoding.UTF8, TimeSpan.FromMinutes(30));

        Console.WriteLine("Bulk copy completed");

        // Stop the application when the task is completed
        applicationLifetime.StopApplication();
    }
}
```

### appsettings.json

```json
{
  "ConnectionStrings": {
    "DefaultConnection": "YourDatabaseConnectionString"
  }
}
```

このサンプルでは、`appsettings.json`を使用してデータベース接続文字列を設定し、`BulkCopyService`をホストしています。`BulkCopyService`は、データベースへの接続を開き、CSVファイルを読み込んでバルクコピーを実行します。`AddSqlBulkCopier`メソッドを使用して、必要なサービスをDIコンテナに追加しています。

## APIの詳細

以下の表は、CSVのFluent APIを使用する際の目的別に、関連する関数とその使用方法へのリンクを示しています。

| 目的 | 関数 | 使用方法 |
|------|------|----------|
| CSVファイルをヘッダー有りで処理する | `CreateWithHeader` | [使用方法](#createwithheader) |
| CSVファイルをヘッダー無しで処理する | `CreateNoHeader` | [使用方法](#createnoheader) |
| `IBulkCopier`のインスタンスを作成する | `Build` | [使用方法](#buildメソッド) |
| 事前にテーブルをトランケートする | `SetTruncateBeforeBulkInsert` | [使用方法](#settruncatebeforebulkinsert) |
| 行ごとに取り込み対象を判定する | `SetRowFilter` | [使用方法](#setrowfilter) |
| リトライ設定 | `SetMaxRetryCount`, `SetInitialDelay`, `SetUseExponentialBackoff` | [使用方法](#リトライ設定) |
| バッチサイズを設定する | `SetBatchSize` | [使用方法](#setbatchsize) |
| 通知イベントの行数を設定する | `SetNotifyAfter` | [使用方法](#setnotifyafter) |
| デフォルトのカラムコンテキストを設定する | `SetDefaultColumnContext` | [使用方法](#setdefaultcolumncontext) |

### 使用方法

#### CreateWithHeader
このメソッドは、ヘッダーを持つCSVファイルを処理するためのビルダーを作成します。

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .AddColumnMapping("CustomerId")
    .AddColumnMapping("FirstName")
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

この関数は、CSVファイルのヘッダー名を使用して、データベース列にマッピングします。

#### CreateNoHeader
このメソッドは、ヘッダーを持たないCSVファイルを処理するためのビルダーを作成します。

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateNoHeader("[dbo].[Customer]")
    .AddColumnMapping("CustomerId", 0)
    .AddColumnMapping("FirstName", 1)
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

この関数は、CSVファイルの列位置を使用して、データベース列にマッピングします。

#### Buildメソッド
`Build`メソッドは、`IBulkCopier`のインスタンスを作成するための重要なメソッドです。以下の4つのオーバーロードがあります：

1. **`Build(SqlConnection connection)`**:
   - 指定されたSQL接続を使用して`IBulkCopier`のインスタンスを作成します。
   - `connection`パラメータは、バルクコピー操作を実行する前に開かれている必要があります。
   - `ArgumentNullException`が、`connection`が`null`の場合にスローされます。

2. **`Build(string connectionString)`**:
   - 指定された接続文字列を使用して`IBulkCopier`のインスタンスを作成します。
   - `connectionString`パラメータは、SQL Serverへの接続を確立するために必要なすべての情報を含んでいる必要があります。
   - `ArgumentNullException`が、`connectionString`が`null`または空の場合にスローされます。
   - `ArgumentException`が、`connectionString`が無効な場合にスローされます。

3. **`Build(string connectionString, SqlBulkCopyOptions copyOptions)`**:
   - 指定された接続文字列とコピーオプションを使用して`IBulkCopier`のインスタンスを作成します。
   - `connectionString`パラメータは、SQL Serverへの接続を確立するために必要なすべての情報を含んでいる必要があります。
   - `copyOptions`は、操作の動作を構成するためのSQLバルクコピーオプションです。
   - `ArgumentNullException`が、`connectionString`が`null`または空の場合にスローされます。
   - `ArgumentException`が、`connectionString`が無効な場合にスローされます。

4. **`Build(SqlConnection connection, SqlBulkCopyOptions copyOptions, SqlTransaction externalTransaction)`**:
   - 指定された接続、オプション、およびトランザクションを使用して`IBulkCopier`のインスタンスを作成します。
   - `connection`パラメータは、バルクコピー操作を実行する前に開かれている必要があります。
   - `copyOptions`は、操作の動作を構成するためのSQLバルクコピーオプションです。
   - `externalTransaction`は、バルクコピー操作のために使用される外部トランザクションです。すべてのバルクコピー操作はこのトランザクションの一部となります。
   - `ArgumentNullException`が、`connection`または`externalTransaction`が`null`の場合にスローされます。

#### SetTruncateBeforeBulkInsert
この関数は、バルクコピーを実行する前に、指定したテーブルをトランケートするために使用します。

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .SetTruncateBeforeBulkInsert(true)
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

#### SetRowFilter
この関数は、CSVデータの各行を評価し、取り込み対象とするかどうかを判定するために使用します。指定した条件に合致する行のみをデータベースにコピーします。

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .SetRowFilter(reader => reader.GetField<string>("Status") == "Active")
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

この例では、`Status`列の値が`"Active"`である行のみを取り込み対象としています。

#### リトライ設定
リトライ設定を使用することで、特定の条件下で自動的にリトライを実行することができます。リトライが可能な条件は以下の通りです：

- **接続文字列を使用している場合**: 外部接続ではなく、接続文字列を使用してデータベースに接続している場合、リトライが可能です。
- **テーブルのトランケートが有効である場合**: テーブルのトランケートが有効である場合、リトライが可能です。

リトライ設定には、以下のオプションがあります：

- **SetMaxRetryCount**: リトライの最大回数を設定します。
- **SetInitialDelay**: リトライ間の初期遅延時間を設定します。
- **SetUseExponentialBackoff**: この設定を有効にすると、リトライ間の待機時間が指数関数的に増加します。例えば、最初のリトライで5秒待機した場合、次のリトライでは10秒、さらにその次では20秒といった具合に待機時間が増加します。これにより、短時間での連続的なリトライを避け、システムの負荷を軽減することができます。

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .SetMaxRetryCount(3)
    .SetInitialDelay(TimeSpan.FromSeconds(5))
    .SetUseExponentialBackoff(true)
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

#### SetBatchSize
この関数は、バルクコピー操作のバッチサイズを設定します。

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .SetBatchSize(1000)
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

#### SetNotifyAfter
この関数は、通知イベントを発生させる行数を設定します。

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .SetNotifyAfter(500)
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

#### SetDefaultColumnContext
この関数は、すべてのカラムに対するデフォルトのコンテキストを設定します。

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .SetDefaultColumnContext(c => c.TrimEnd().TreatEmptyStringAsNull())
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

