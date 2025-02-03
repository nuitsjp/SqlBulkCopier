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

| 目的 | 関数 |
|------|------|
| [CSVファイルをヘッダー有りで処理する](#CSVファイルをヘッダー有りで処理する) | `CreateWithHeader` |
| [CSVファイルをヘッダー無しで処理する](#createnoheader) | `CreateNoHeader` |
| [データ型の設定](#データ型の設定) | `AsInt`, `AsDate`, `AsDecimal`, etc. |
| [トリム操作](#トリム操作) | `Trim`, `TrimStart`, `TrimEnd` |
| [空文字列のNULL扱い](#空文字列のnull扱い) | `TreatEmptyStringAsNull` |
| [カスタム変換](#カスタム変換) | `Convert` |
| [`IBulkCopier`のインスタンスを作成する](#IBulkCopierのインスタンスを作成する) | `Build` |
| [事前にテーブルをトランケートする](#settruncatebeforebulkinsert) | `SetTruncateBeforeBulkInsert` |
| [行ごとに取り込み対象を判定する](#setrowfilter) | `SetRowFilter` |
| [リトライ設定](#リトライ設定) | `SetMaxRetryCount`, `SetInitialDelay`, `SetUseExponentialBackoff` |
| [バッチサイズを設定する](#setbatchsize) | `SetBatchSize` |
| [通知イベントの行数を設定する](#setnotifyafter) | `SetNotifyAfter` |
| [デフォルトのカラムコンテキストを設定する](#setdefaultcolumncontext) | `SetDefaultColumnContext` |

### 使用方法

#### CSVファイルをヘッダー有りで処理する
このメソッドは、ヘッダーを持つCSVファイルを処理するためのビルダーを作成します。以下のコード例は、CSVファイルのヘッダー名を使用してデータベース列にマッピングする方法を示しています。

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .AddColumnMapping("CustomerId")
    .AddColumnMapping("FirstName")
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

#### CreateNoHeader
このメソッドは、ヘッダーを持たないCSVファイルを処理するためのビルダーを作成します。以下のコード例は、CSVファイルの列位置を使用してデータベース列にマッピングする方法を示しています。

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateNoHeader("[dbo].[Customer]")
    .AddColumnMapping("CustomerId", 0)
    .AddColumnMapping("FirstName", 1)
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

#### データ型の設定
`IColumnContext`を使用して、CSVデータをSQL Serverのデータ型にマッピングすることができます。以下のコード例は、いくつかの代表的なデータ型へのマッピング方法を示しています。

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .AddColumnMapping("CustomerId", c => c.AsInt())
    .AddColumnMapping("BirthDate", c => c.AsDate("yyyy-MM-dd"))
    .AddColumnMapping("Salary", c => c.AsDecimal())
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

利用可能なデータ型：

すべての型を明示的に設定する必要はありません。文字列から自動変換できる型は記述を省略することができま
す。

- **AsBigInt**: SQL BIGINT型にマッピング
- **AsBit**: SQL BIT型にマッピング
- **AsUniqueIdentifier**: SQL UNIQUEIDENTIFIER型にマッピング
- **AsDate**: SQL DATE型にマッピング
- **AsDateTime**: SQL DATETIME型にマッピング
- **AsDecimal**: SQL DECIMAL型にマッピング
- **AsFloat**: SQL FLOAT型にマッピング
- **AsInt**: SQL INT型にマッピング
- **AsMoney**: SQL MONEY型にマッピング
- **AsReal**: SQL REAL型にマッピング
- **AsSmallDateTime**: SQL SMALLDATETIME型にマッピング
- **AsSmallInt**: SQL SMALLINT型にマッピング
- **AsSmallMoney**: SQL SMALLMONEY型にマッピング
- **AsTimestamp**: SQL TIMESTAMP型にマッピング
- **AsTinyInt**: SQL TINYINT型にマッピング
- **AsDateTime2**: SQL DATETIME2型にマッピング
- **AsTime**: SQL TIME型にマッピング
- **AsDateTimeOffset**: SQL DATETIMEOFFSET型にマッピング
- **AsBinary**: SQL BINARY型にマッピング
- **AsVarBinary**: SQL VARBINARY型にマッピング
- **AsImage**: SQL IMAGE型にマッピング

#### トリム操作
文字列のトリム操作を行うことができます。これにより、データの前後の空白や特定の文字を削除できます。以下のコード例は、トリム操作の使用方法を示しています。

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .AddColumnMapping("FirstName", c => c.Trim())
    .AddColumnMapping("LastName", c => c.TrimEnd())
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

#### 空文字列のNULL扱い
空の文字列をデータベースに挿入する際にNULLとして扱うことができます。以下のコード例は、その方法を示しています。

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .AddColumnMapping("MiddleName", c => c.TreatEmptyStringAsNull())
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

#### カスタム変換
カスタム変換関数を指定することができます。これにより、文字列を任意のオブジェクトに変換することができます。以下のコード例は、カスタム変換の使用方法を示しています。

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .AddColumnMapping("CustomField", c => c.Convert(value => CustomConversion(value)))
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

#### `IBulkCopier`のインスタンスを作成する
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
この関数は、バルクコピーを実行する前に、指定したテーブルをトランケートするために使用します。以下のコード例は、その方法を示しています。

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .SetTruncateBeforeBulkInsert(true)
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

#### SetRowFilter
この関数は、CSVデータの各行を評価し、取り込み対象とするかどうかを判定するために使用します。指定した条件に合致する行のみをデータベースにコピーします。以下のコード例は、その方法を示しています。

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .SetRowFilter(reader => reader.GetField<string>("Status") == "Active")
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

この例では、`Status`列の値が`"Active"`である行のみを取り込み対象としています。

#### リトライ設定
リトライ設定を使用することで、特定の条件下で自動的にリトライを実行することができます。リトライが可能
な条件は以下の通りです：

- **接続文字列を使用している場合**: 外部接続ではなく、接続文字列を使用してデータベースに接続している
場合、リトライが可能です。
- **テーブルのトランケートが有効である場合**: テーブルのトランケートが有効である場合、リトライが可能
です。

リトライ設定には、以下のオプションがあります：

- **SetMaxRetryCount**: リトライの最大回数を設定します。
- **SetInitialDelay**: リトライ間の初期遅延時間を設定します。
- **SetUseExponentialBackoff**: この設定を有効にすると、リトライ間の待機時間が指数関数的に増加し
ます。例えば、最初のリトライで5秒待機した場合、次のリトライでは10秒、さらにその次では20秒といった具
合に待機時間が増加します。これにより、短時間での連続的なリトライを避け、システムの負荷を軽減すること
ができます。

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .SetMaxRetryCount(3)
    .SetInitialDelay(TimeSpan.FromSeconds(5))
    .SetUseExponentialBackoff(true)
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

#### SetBatchSize
この関数は、バルクコピー操作のバッチサイズを設定します。以下のコード例は、その方法を示しています。

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .SetBatchSize(1000)
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

#### SetNotifyAfter
この関数は、通知イベントを発生させる行数を設定します。以下のコード例は、その方法を示しています。

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .SetNotifyAfter(500)
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

#### SetDefaultColumnContext
この関数は、すべてのカラムに対するデフォルトのコンテキストを設定します。以下のコード例は、その方法を示しています。

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .SetDefaultColumnContext(c => c.TrimEnd().TreatEmptyStringAsNull())
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

個別のカラムに異なる設定がされていた場合は、そちらが優先されます。

