# SqlBulkCopier

## 概要
SqlBulkCopierは、CSVファイルや固定長データのバルクコピーを効率的に行うためのライブラリです。データベースへの大量データのインポートを簡素化し、パフォーマンスを向上させることを目的としています。

このライブラリは、固定長ファイルのフィラー部分が運用中に変更されたり、行の全長が長くなる場合にも影響範囲を最小限に抑えるように設計されています。また、マルチバイト文字を含む場合と含まない場合で処理が異なる固定長データの処理をサポートしており、マルチバイト文字のサポートが大きな特徴です。

CSVや固定長ファイルの読み込み設定は、設定ファイルとフルーエントAPIの2種類の方法で指定でき、利用シーンに応じて選択可能です。

## 主な機能
- **CSVデータのバルクコピー**: `CsvHelper`を使用して、CSVファイルからデータベースへの高速なデータ転送をサポートします。
- **固定長データのバルクコピー**: 固定長フォーマットのデータを効率的に処理し、データベースに転送します。
- **カスタマイズ可能なデータリーダー**: データの読み込み方法を柔軟にカスタマイズ可能です。
- **高いパフォーマンス**: 大量データの処理に最適化されています。

## インストール方法
このライブラリは、CSVと固定長データで異なるNuGetパッケージとして提供されています。以下のコマンドを使用してインストールしてください。

### CSV用パッケージ
```
Install-Package SqlBulkCopier.CsvHelper
```

### 固定長データ用パッケージ
```
Install-Package SqlBulkCopier.FixedLength
```

必要な.NETバージョン: .NET 8.0 または .NET Framework 4.8

## クイックスタート
以下は、CSVデータをデータベースにバルクコピーするための基本的なコード例です。

```csharp
using System;
using SqlBulkCopier.CsvHelper;

class Program
{
    static void Main(string[] args)
    {
        // CsvBulkCopierBuilderを使用して、CSVデータのバルクコピーを設定
        var csvBulkCopierBuilder = new CsvBulkCopierBuilder()
            .WithCsvFilePath("path/to/your/csvfile.csv")
            .WithConnectionString("YourDatabaseConnectionString")
            .WithTableName("YourTableName");

        // バルクコピーを実行
        var csvBulkCopier = csvBulkCopierBuilder.Build();
        csvBulkCopier.Copy();

        Console.WriteLine("CSV data has been successfully copied to the database.");
    }
}
```

## 詳細なドキュメント
詳細なドキュメントは、プロジェクトのWikiページをご覧ください。

## 貢献方法
このプロジェクトへの貢献を歓迎します。IssueやPull Requestを通じて貢献してください。

## ライセンス
このプロジェクトはMITライセンスの下で提供されています。詳細は`LICENSE`ファイルを参照してください。

## サポートと連絡先
質問やサポートが必要な場合は、プロジェクトのGitHubリポジトリのIssueセクションを通じてお問い合わせください。
