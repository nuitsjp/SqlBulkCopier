# はじめに
SqlBulkCopierは、CSVおよび固定長ファイルを効率的にデータベースにバルクコピーすることができます。ここでは固定長ファイルに対する利用方法を説明します。

固定長ファイル利用時の設定方法として、以下の2つのアプローチを提供しています：

1. **Fluent API アプローチ**
   - コードでの設定を好む場合に適しています
   - より詳細な制御が可能で、動的な設定が必要な場合に適しています
   - IntelliSenseのサポートにより、設定オプションを確認しやすい利点があります

2. **Configuration アプローチ**
   - 設定をappsettings.jsonで管理したい場合に適しています
   - より簡潔なコードで実装が可能です
   - 設定の変更に再コンパイルが不要です
   - 環境ごとの設定変更が容易です

## 目次
- [Getting Started](#getting-started)
  - [Fluent APIアプローチのサンプル](#fluent-apiアプローチのサンプル)
  - [Configuration アプローチ](#configuration-アプローチ)
- [設定の詳細](#設定の詳細)
  - [データ型の設定](#データ型の設定)
  - [トリム操作](#トリム操作)
  - [空文字列のNULL扱い](#空文字列のnull扱い)
  - [カスタム変換](#カスタム変換)
  - [IBulkCopierのインスタンスを作成する](#ibulkcopierのインスタンスを作成する)
  - [事前にテーブルをトランケートする](#事前にテーブルをトランケートする)
  - [トランケート方法を設定する](#トランケート方法を設定する)
  - [行ごとに取り込み対象を判定する](#行ごとに取り込み対象を判定する)
  - [リトライ設定](#リトライ設定)
  - [バッチサイズを設定する](#バッチサイズを設定する)
  - [通知イベントの行数を設定する](#通知イベントの行数を設定する)
  - [デフォルトのカラムコンテキストを設定する](#デフォルトのカラムコンテキストを設定する)

## Getting Started
このライブラリは、.NET 8.0 または .NET Framework 4.8が必要です。NuGetから以下のパッケージをインストールしてください：

```
Install-Package SqlBulkCopier.FixedLength.Hosting
```

このドキュメントで説明する2つのアプローチのサンプルコードを以下に示します。どちらも`Microsoft.Extensions.Hosting`を使用してGeneric Hostに対応したコンソールアプリケーションを構築する例です。コンソールアプリケーションプロジェクトで作成する前提となります。

### Fluent APIアプローチのサンプル

Fluent APIを使用する場合、以下の手順で実装します：

1. appsettings.jsonにSQL Serverへの接続文字列を追加
2. Program.csでSqlBulkCopierサービスを登録（AddSqlBulkCopier()）
3. FixedLengthBulkCopierBuilderを利用して詳細を設定し、固定長ファイルを取り込む

実装例の詳細は以下の通りです。

#### appsettings.json

```json
{
  "ConnectionStrings": {
    "DefaultConnection": "YourDatabaseConnectionString"
  }
}
```

#### Program.cs

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Sample.FixedLength.FromApi;
using SqlBulkCopier.FixedLength.Hosting;

var builder = Host.CreateApplicationBuilder(args);

builder.Configuration
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json");

builder.Services
    .AddHostedService<FixedLengthBulkCopyService>()
    .AddSqlBulkCopier();

await builder
    .Build()
    .RunAsync();
```

#### FixedLengthBulkCopyService.cs

```csharp
using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using SqlBulkCopier.FixedLength;

namespace Sample.FixedLength.FromApi;

public class FixedLengthBulkCopyService(
    IConfiguration configuration,
    IHostApplicationLifetime applicationLifetime) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Create a bulk copier instance
        var bulkCopier = FixedLengthBulkCopierBuilder
            .Create("[dbo].[Customer]")
            .SetTruncateBeforeBulkInsert(true)
            .SetDefaultColumnContext(c => c.TrimEnd().TreatEmptyStringAsNull())
            .AddColumnMapping("CustomerId", 0, 10)
            .AddColumnMapping("FirstName", 10, 50)
            .AddColumnMapping("LastName", 60, 50)
            .AddColumnMapping("BirthDate", 590, 10, c => c.AsDate("yyyy-MM-dd"))
            .AddColumnMapping("IsActive", 727, 1, c => c.AsBit())
            .Build(configuration.GetConnectionString("DefaultConnection")!);

        // Open the fixed length file
        await using var stream = File.OpenRead(
            Path.Combine(AppContext.BaseDirectory, "Assets", "Customer.dat"));

        // Start the bulk copy operation
        await bulkCopier.WriteToServerAsync(stream, new UTF8Encoding(false), TimeSpan.FromMinutes(30));

        // Stop the application when the task is completed
        applicationLifetime.StopApplication();
    }
}
```

詳細は「[設定の詳細](#設定の詳細)」を参照

### Configuration アプローチ

設定ファイルを使用する場合、以下の手順で実装します：

1. appsettings.jsonにバルクコピーの設定を追加
2. Program.csでSqlBulkCopierサービスを登録（AddSqlBulkCopier()）
3. DIでIBulkCopierProviderを注入し、Provideメソッドで設定を読み込み

各実装例の詳細は以下の通りです。

#### appsettings.json

```json
{
  "ConnectionStrings": {
    "DefaultConnection": "Data Source=.;Initial Catalog=SqlBulkCopier;Integrated Security=True;Trust Server Certificate=True"
  },
  "SqlBulkCopier1": {
    "DestinationTableName": "[dbo].[Customer]",
    "TruncateBeforeBulkInsert": true,
    "DefaultColumnSettings": {
      "TrimMode": "TrimEnd",
      "TreatEmptyStringAsNull": true
    },
    "Columns": {
      "CustomerId": { "Start": 0, "Length": 10 },
      "FirstName": { "Start": 10, "Length": 50 },
      "LastName": { "Start": 60, "Length": 50 },
      "BirthDate": {
        "Start": 590,
        "Length": 10,
        "SqlDbType": "Date",
        "Format": "yyyy-MM-dd"
      },
      "IsActive": { "Start": 727, "Length": 1, "SqlDbType": "Bit" }
    }
  }
}
```

詳細は「[設定の詳細](#設定の詳細)」を参照

#### Program.cs

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Sample.FixedLength.FromAppSettings;
using SqlBulkCopier.FixedLength.Hosting;

var builder = Host.CreateApplicationBuilder(args);

builder.Configuration
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json");

builder.Services
    .AddHostedService<FixedLengthBulkCopyService>()
    .AddSqlBulkCopier();

await builder
    .Build()
    .RunAsync();
```

#### FixedLengthBulkCopyService.cs

```csharp
using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using SqlBulkCopier.Hosting;

namespace Sample.FixedLength.FromAppSettings;

public class FixedLengthBulkCopyService(
    IConfiguration configuration,
    IBulkCopierProvider bulkCopierProvider,
    IHostApplicationLifetime applicationLifetime) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Provide the bulk copier
        var bulkCopier = bulkCopierProvider
            .Provide("SqlBulkCopier1", configuration.GetConnectionString("DefaultConnection")!);
        
        // Write data to the database using the bulk copier
        await using var stream = File.OpenRead(
            Path.Combine(AppContext.BaseDirectory, "Assets", "Customer.dat"));

        // Bulk copy to the database
        await bulkCopier.WriteToServerAsync(stream, new UTF8Encoding(false), TimeSpan.FromMinutes(30));

        // Stop the application when the task is completed
        applicationLifetime.StopApplication();
    }
}
```

## 設定の詳細

以下の表は、固定長ファイルのバルクコピー設定において実現可能な機能と、それぞれのアプローチでの設定方法を示しています。

| 目的 | Fluent API | appsettings.json |
|------|------------|------------------|
| [データ型の設定](#データ型の設定) | `AsInt`, `AsDate`, `AsDecimal`, etc. | `"SqlDbType": "Int"`, `"Date"`, `"Decimal"`, etc. |
| [トリム操作](#トリム操作) | `Trim`, `TrimStart`, `TrimEnd` | `"TrimMode": "Trim"`, `"TrimStart"`, `"TrimEnd"` |
| [空文字列のNULL扱い](#空文字列のnull扱い) | `TreatEmptyStringAsNull` | `"TreatEmptyStringAsNull": true`, `false` |
| [カスタム変換](#カスタム変換) | `Convert` | 設定ファイルでは未対応 |
| [IBulkCopierのインスタンスを作成する](#ibulkcopierのインスタンスを作成する) | `Build` | 設定ファイルでは未対応 |
| [事前にテーブルをトランケートする](#事前にテーブルをトランケートする) | `SetTruncateBeforeBulkInsert` | `"TruncateBeforeBulkInsert": true/false` |
| [トランケート方法を設定する](#トランケート方法を設定する) | `SetTruncateMethod` | `"TruncateMethod": "Truncate"`, `"Delete"` |
| [行ごとに取り込み対象を判定する](#行ごとに取り込み対象を判定する) | `SetRowFilter` | `"RowFilter": { ... }` |
| [リトライ設定](#リトライ設定) | `SetMaxRetryCount`, `SetInitialDelay`, `SetUseExponentialBackoff` | `"MaxRetryCount": 3`, `"InitialDelay": "00:00:05"`, `"UseExponentialBackoff": true` |
| [バッチサイズを設定する](#バッチサイズを設定する) | `SetBatchSize` | `"BatchSize": 1000` |
| [通知イベントの行数を設定する](#通知イベントの行数を設定する) | `SetNotifyAfter` | `"NotifyAfter": 500` |
| [デフォルトのカラムコンテキストを設定する](#デフォルトのカラムコンテキストを設定する) | `SetDefaultColumnContext` | `"DefaultColumnSettings": { ... }` |

### 使用方法

#### [データ型の設定](#設定の詳細)
`IColumnContext`を使用して、固定長ファイルデータをSQL Serverのデータ型にマッピングすることができます。以下のコード例は、いくつかの代表的なデータ型へのマッピング方法を示しています。

SqlBulkcopyで自動的な変換が可能な場合、必ずしも個別に指定する必要はありません。

```csharp
    .AddColumnMapping("BirthDate", 590, 10, c => c.AsDate("yyyy-MM-dd"))
    .AddColumnMapping("Salary", 600, 10, c => c.AsDecimal())
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

appsettings.jsonでの設定例:

```json
{
  "SqlBulkCopier": {
    "DestinationTableName": "[dbo].[Customer]",
    "Columns": {
      "BirthDate": {
        "Start": 590,
        "Length": 10,
        "SqlDbType": "Date",
        "Format": "yyyy-MM-dd"
      },
      "Salary": {
        "Start": 600,
        "Length": 10,
        "SqlDbType": "Decimal"
      }
    }
  }
}
```

 有効な型は以下の通り。下記以外はSqlBulkCopyのデフォルトのコンバーターを利用するため指定は不要です。

| SQL Server 型     | Fluent API (IColumnContext) | appsettings.json 設定例                   |
|-------------------|-----------------------------|-------------------------------------------|
| BIGINT            | AsBigInt                    | "SqlDbType": "BigInt"                      |
| BIT               | AsBit                       | "SqlDbType": "Bit"                         |
| UNIQUEIDENTIFIER  | AsUniqueIdentifier          | "SqlDbType": "UniqueIdentifier"            |
| DATE              | AsDate                      | "SqlDbType": "Date", "Format": "yyyy-MM-dd" |
| DATETIME          | AsDateTime                  | "SqlDbType": "DateTime", "Format": "..."    |
| DECIMAL           | AsDecimal                   | "SqlDbType": "Decimal"                      |
| FLOAT             | AsFloat                     | "SqlDbType": "Float"                        |
| INT               | AsInt                       | "SqlDbType": "Int"                          |
| MONEY             | AsMoney                     | "SqlDbType": "Money"                        |
| REAL              | AsReal                      | "SqlDbType": "Real"                         |


#### [トリム操作](#設定の詳細)
文字列のトリム操作を行うことができます。これにより、データの前後の空白や特定の文字を削除できます。以下のコード例は、トリム操作の使用方法を示しています。

```csharp
var bulkCopier = FixedLengthBulkCopierBuilder
    .Create("[dbo].[Customer]")
    .AddColumnMapping("FirstName", 10, 50, c => c.Trim())
    .AddColumnMapping("LastName", 60, 50, c => c.TrimEnd())
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

appsettings.jsonでの設定例:

```json
{
  "SqlBulkCopier": {
    "DestinationTableName": "[dbo].[Customer]",
    "DefaultColumnSettings": {
      "TrimMode": "Trim",
      "TrimChars": " "
    },
    "Columns": {
      "FirstName": { "Start": 10, "Length": 50 },
      "LastName": {
        "Start": 60,
        "Length": 50,
        "TrimMode": "TrimEnd"
      }
    }
  }
}
```

#### [空文字列のNULL扱い](#設定の詳細)
空の文字列をデータベースに挿入する際にNULLとして扱うことができます。以下のコード例は、その方法を示しています。

```csharp
var bulkCopier = FixedLengthBulkCopierBuilder
    .Create("[dbo].[Customer]")
    .AddColumnMapping("MiddleName", 110, 50, c => c.TreatEmptyStringAsNull())
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

appsettings.jsonでの設定例:

```json
{
  "SqlBulkCopier": {
    "DestinationTableName": "[dbo].[Customer]",
    "DefaultColumnSettings": {
      "TreatEmptyStringAsNull": true
    },
    "Columns": {
      "MiddleName": { "Start": 110, "Length": 50 }
    }
  }
}
```

#### [カスタム変換](#設定の詳細)
カスタム変換関数を指定することができます。これにより、文字列を任意のオブジェクトに変換することができます。以下のコード例は、カスタム変換の使用方法を示しています。

```csharp
var bulkCopier = FixedLengthBulkCopierBuilder
    .Create("[dbo].[Customer]")
    .AddColumnMapping("CustomField", 160, 50, c => c.Convert(value => CustomConversion(value)))
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

appsettings.jsonでの設定例は未対応です。

#### [`IBulkCopier`のインスタンスを作成する](#設定の詳細)
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

appsettings.jsonでの設定例:

```json
{
  "SqlBulkCopier": {
    "DestinationTableName": "[dbo].[Customer]",
    "ConnectionString": "YourDatabaseConnectionString"
  }
}
```

#### [事前にテーブルをトランケートする](#設定の詳細)
この関数は、バルクコピーを実行する前に、指定したテーブルをトランケートするために使用します。以下のコード例は、その方法を示しています。

```csharp
var bulkCopier = FixedLengthBulkCopierBuilder
    .Create("[dbo].[Customer]")
    .SetTruncateBeforeBulkInsert(true)
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

appsettings.jsonでの設定例:

```json
{
  "SqlBulkCopier": {
    "DestinationTableName": "[dbo].[Customer]",
    "TruncateBeforeBulkInsert": true
  }
}
```

#### [トランケート方法を設定する](#設定の詳細)
この関数は、バルクインサート前にテーブルからデータを削除する方法を設定します。デフォルトでは `TRUNCATE TABLE` が使用されますが、代わりに `DELETE FROM` を使用することもできます。これは、テーブルに外部キー制約がある場合に便利です。`TRUNCATE TABLE` はそのような場合には使用できません。

```csharp
var bulkCopier = FixedLengthBulkCopierBuilder
    .Create("[dbo].[Customer]")
    .SetTruncateBeforeBulkInsert(true)
    .SetTruncateMethod(TruncateMethod.Delete)
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

appsettings.jsonでの設定例:

```json
{
  "SqlBulkCopier": {
    "DestinationTableName": "[dbo].[Customer]",
    "TruncateBeforeBulkInsert": true,
    "TruncateMethod": "Delete"
  }
}
```

| 方法 | 説明 |
|------|------|
| `Truncate` | `TRUNCATE TABLE` 文を使用します。デフォルト。高速ですが、外部キー制約がある場合は使用できません。 |
| `Delete` | `DELETE FROM` 文を使用します。低速ですが、外部キー制約がある場合でも動作します。 |

この機能を使用すると、固定長ファイルデータの各行を評価し、取り込み対象とするかどうかを判定できます。指定した条件に合致する行のみをデータベースにコピーします。

以下は、Fluent APIと`appsettings.json`の両方での設定例です。

##### Fluent APIでの設定例

```csharp
var bulkCopier = FixedLengthBulkCopierBuilder
    .Create("[dbo].[Customer]")
    .SetRowFilter(reader => reader.Parser.RawRecord.StartsWith("Comment"))
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

この例では、行が`"Comment"`で始まる場合、読取の対象外とします。RawRecordには改行も含まれるため注意してください。

##### appsettings.jsonでの設定例

以下のように、`RowFilter`セクションを使用して条件を指定できます。

```json
{
  "SqlBulkCopier": {
    "DestinationTableName": "[dbo].[Customer]",
    "RowFilter": {
      "Equals": [ "Comment"],
      "StartsWith": [ "Prefix"],
      "EndsWith": [ "Suffix" ]
    }
  }
}
```

- **`Equals`**: 行が指定した値と一致する場合、取込対象外とします。
- **`StartsWith`**: 行が指定した値で始まる場合、取込対象外とします。
- **`EndsWith`**: 行が指定した値で終わる場合、取込対象外とします。

##### 注意事項

- `RowFilter`は複数の条件を組み合わせて使用できます。
- 条件はANDで結合されます（すべての条件を満たす行のみが対象となります）。
- `appsettings.json`での設定は、Fluent APIでの設定と同等の機能を提供します。

#### [リトライ設定](#設定の詳細)
リトライ設定を使用することで、特定の条件下で自動的にリトライを実行することができます。リトライが可能な条件は以下の通りです：
- **接続文字列を使用している場合**: 外部接続ではなく、接続文字列を使用してデータベースに接続している場合、リトライが可能です。
- **テーブルのトランケートが有効である場合**: テーブルのトランケートが有効である場合、リトライが可能です。

リトライ設定には、以下のオプションがあります：
- **SetMaxRetryCount**: リトライの最大回数を設定します。
- **SetInitialDelay**: リトライ間の初期遅延時間を設定します。
- **SetUseExponentialBackoff**: この設定を有効にすると、リトライ間の待機時間が指数関数的に増加します。例えば、最初のリトライで5秒待機した場合、次のリトライでは10秒、さらにその次では20秒といった具合に待機時間が増加します。これにより、短時間での連続的なリトライを避け、システムの負荷を軽減することができます。

```csharp
var bulkCopier = FixedLengthBulkCopierBuilder
    .Create("[dbo].[Customer]")
    .SetMaxRetryCount(3)
    .SetInitialDelay(TimeSpan.FromSeconds(5))
    .SetUseExponentialBackoff(true)
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

appsettings.jsonでの設定例:

```json
{
  "SqlBulkCopier": {
    "DestinationTableName": "[dbo].[Customer]",
    "MaxRetryCount": 3,
    "InitialDelay": "00:00:05",
    "UseExponentialBackoff": true
  }
}
```

#### [バッチサイズを設定する](#設定の詳細)
この関数は、バルクコピー操作のバッチサイズを設定します。以下のコード例は、その方法を示しています。

```csharp
var bulkCopier = FixedLengthBulkCopierBuilder
    .Create("[dbo].[Customer]")
    .SetBatchSize(1000)
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

appsettings.jsonでの設定例:

```json
{
  "SqlBulkCopier": {
    "DestinationTableName": "[dbo].[Customer]",
    "BatchSize": 1000
  }
}
```

#### [通知イベントの行数を設定する](#設定の詳細)
この関数は、通知イベントを発生させる行数を設定します。以下のコード例は、その方法を示しています。

```csharp
var bulkCopier = FixedLengthBulkCopierBuilder
    .Create("[dbo].[Customer]")
    .SetNotifyAfter(500)
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

appsettings.jsonでの設定例:

```json
{
  "SqlBulkCopier": {
    "DestinationTableName": "[dbo].[Customer]",
    "NotifyAfter": 500
  }
}
```

#### [デフォルトのカラムコンテキストを設定する](#設定の詳細)
この関数は、すべてのカラムに対するデフォルトのコンテキストを設定します。以下のコード例は、その方法を示しています。

個別のカラムに異なる設定がされていた場合は、そちらが優先されます。

```csharp
var bulkCopier = FixedLengthBulkCopierBuilder
    .Create("[dbo].[Customer]")
    .SetDefaultColumnContext(c => c.TrimEnd().TreatEmptyStringAsNull())
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

appsettings.jsonでの設定例:

```json
{
  "SqlBulkCopier": {
    "DestinationTableName": "[dbo].[Customer]",
    "DefaultColumnSettings": {
      "TrimMode": "TrimEnd",
      "TreatEmptyStringAsNull": true
    }
  }
}
```
