# SqlBulkCopier

[![Build Status](https://example.com/path/to/build_badge)](https://example.com) [![Coverage Status](https://example.com/path/to/coverage_badge)](https://example.com)

## 概要
SqlBulkCopierは、SQL Serverの高速なバルクコピー機能であるSqlBulkCopyを、CSVファイルおよび固定長ファイルでより扱いやすくするためのライブラリです。大量データの取り込みを効率化し、使いやすい設定手段（appsettings.jsonおよびFluent API）を提供します。

## 特徴
- **高速処理**: SQL ServerのSqlBulkCopyを活用した高性能なデータ転送
- **ファイル形式対応**: CSVファイルと固定長ファイルの両対応
- **柔軟な設定方法**: appsettings.json と Fluent API（またはAPI）の2種類の設定手段
- **多言語対応**: マルチバイト文字やUTFの結合文字に対応
- **柔軟性**: CSVファイルや固定長ファイルの無関係な列や行の変更に影響を受けない設計

## 目次
- [サポート対象](#サポート対象)
- [Getting Started](#getting-started)
- CSVの詳細設定
  - [CSV - appsettings.json編](doc/CSV_Appsettings.md)
  - [CSV - Fluent API編](doc/CSV_FluentAPI.md)
- 固定長ファイルの詳細設定
  - [固定長 - appsettings.json編](doc/FixedLength_Appsettings.md)
  - [固定長 - Fluent API編](doc/FixedLength_FluentAPI.md)
- [ライセンス](#ライセンス)

## サポート対象

このライブラリは、以下のプラットフォームでサポートされています：
- .NET 8.0
- .NET Framework 4.8

## Getting Started

NuGetから以下のパッケージをインストールしてください：

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
        // Open a connection to the database
        await using var connection = new SqlConnection(configuration.GetConnectionString("DefaultConnection"));
        await connection.OpenAsync(stoppingToken);

        var bulkCopier = CsvBulkCopierBuilder
            .CreateWithHeader("[dbo].[Customer]")
            .SetTruncateBeforeBulkInsert(true)
            .SetDefaultColumnContext(c => c.TrimEnd().TreatEmptyStringAsNull())
            .AddColumnMapping("CustomerId")
            .AddColumnMapping("FirstName")
            .AddColumnMapping("LastName")
            .AddColumnMapping("BirthDate", c => c.AsDate("yyyy-MM-dd"))
            .AddColumnMapping("IsActive", c => c.AsBit())
            .Build(connection);

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

## ライセンス
このプロジェクトは [MITライセンス](LICENSE) の下で提供されています。
