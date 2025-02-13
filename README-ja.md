# SqlBulkCopier

![Build Status](https://github.com/nuitsjp/SqlBulkCopier/actions/workflows/build.yml/badge.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

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
- [利用可能パッケージ](#利用可能パッケージ)
- [Getting Started](#getting-started)
- [CSVの詳細設定](doc/CSV-ja.md)
- [固定長ファイルの詳細設定](doc/FixedLength-ja.md)
- [ライセンス](#ライセンス)

## サポート対象

このライブラリは、以下のプラットフォームでサポートされています：
- .NET 8.0
- .NET Framework 4.8

## 利用可能パッケージ
You can install any of the following NuGet packages as needed:

- [![NuGet (SqlBulkCopier)](https://img.shields.io/nuget/v/SqlBulkCopier.svg?label=SqlBulkCopier)](https://www.nuget.org/packages/SqlBulkCopier/)
- [![NuGet (SqlBulkCopier.Hosting)](https://img.shields.io/nuget/v/SqlBulkCopier.Hosting.svg?label=SqlBulkCopier.Hosting)](https://www.nuget.org/packages/SqlBulkCopier.Hosting/)
- [![NuGet (SqlBulkCopier.CsvHelper)](https://img.shields.io/nuget/v/SqlBulkCopier.CsvHelper.svg?label=SqlBulkCopier.CsvHelper)](https://www.nuget.org/packages/SqlBulkCopier.CsvHelper/)
- [![NuGet](https://img.shields.io/nuget/v/SqlBulkCopier.CsvHelper.Hosting.svg?label=SqlBulkCopier.CsvHelper.Hosting)](https://www.nuget.org/packages/SqlBulkCopier.CsvHelper.Hosting/)
- [![NuGet (SqlBulkCopier.FixedLength)](https://img.shields.io/nuget/v/SqlBulkCopier.FixedLength.svg?label=SqlBulkCopier.FixedLength)](https://www.nuget.org/packages/SqlBulkCopier.FixedLength/)
- [![NuGet (SqlBulkCopier.FixedLength.Hosting)](https://img.shields.io/nuget/v/SqlBulkCopier.FixedLength.Hosting.svg?label=SqlBulkCopier.FixedLength.Hosting)](https://www.nuget.org/packages/SqlBulkCopier.FixedLength.Hosting/)

## Getting Started

ここではCSVをFluent APIを利用して取り込む方法を紹介します。詳細はCSVと固定長のそれぞれのドキュメントを参照してください。

NuGetから以下のパッケージをインストールしてください：

```
Install-Package SqlBulkCopier.CsvHelper.Hosting
```

このドキュメントで説明する2つのアプローチのサンプルコードを以下に示します。どちらも`Microsoft.Extensions.Hosting`を使用してGeneric Hostに対応したコンソールアプリケーションを構築する例です。コンソールアプリケーションプロジェクトで作成する前提となります。

### Fluent APIアプローチのサンプル

Fluent APIを使用する場合、以下の手順で実装します：

1. appsettings.jsonにSQL Serverへの接続文字列を追加
2. Program.csでSqlBulkCopierサービスを登録（AddSqlBulkCopier()）
3. CsvBulkCopierBuilderを利用して詳細を設定し、CSVを取り込む

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
using Sample.CsvHelper.FromApi;
using SqlBulkCopier.CsvHelper.Hosting;

var builder = Host.CreateApplicationBuilder(args);

builder.Configuration
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json");

builder.Services
    .AddHostedService<BulkCopyService>()
    .AddSqlBulkCopier();

await builder
    .Build()
    .RunAsync();
```

#### BulkCopyService.cs

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
        // Create a bulk copier instance
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

        // Open the CSV file
        await using var stream = File.OpenRead(
            Path.Combine(AppContext.BaseDirectory, "Assets", "Customer.csv"));

        // Start the bulk copy operation
        await bulkCopier.WriteToServerAsync(stream, Encoding.UTF8, TimeSpan.FromMinutes(30));

        // Stop the application when the task is completed
        applicationLifetime.StopApplication();
    }
}
```

## ライセンス
このプロジェクトは [MITライセンス](LICENSE) の下で提供されています。
