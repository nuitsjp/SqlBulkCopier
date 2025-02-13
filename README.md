# SqlBulkCopier

![Build Status](https://github.com/nuitsjp/SqlBulkCopier/actions/workflows/build.yml/badge.svg)

[Japanese Documents](README-ja.md)

## Overview
SqlBulkCopier is a library that makes the high-speed bulk copy feature of SQL Server, SqlBulkCopy, more manageable with CSV files and fixed-length files. It streamlines the import of large amounts of data and provides user-friendly configuration methods (appsettings.json and Fluent API).

## Features
- **High Performance**: High-performance data transfer utilizing SQL Server's SqlBulkCopy
- **File Format Support**: Supports both CSV files and fixed-length files
- **Flexible Configuration**: Two configuration methods: appsettings.json and Fluent API (or API)
- **Multilingual Support**: Supports multibyte characters and UTF combining characters
- **Flexibility**: Designed to be unaffected by irrelevant columns or rows in CSV or fixed-length files

## Table of Contents
- [Supported Platforms](#supported-platforms)
- [Getting Started](#getting-started)
- [CSV Detailed Settings](doc/CSV.md)
- [Fixed-Length File Detailed Settings](doc/FixedLength.md)
- [License](#license)

## Supported Platforms

This library is supported on the following platforms:
- .NET 8.0
- .NET Framework 4.8

## Getting Started

Here we introduce how to import CSV using Fluent API. For details, refer to the respective documents for CSV and fixed-length files.

Install the following package from NuGet:

```
Install-Package SqlBulkCopier.CsvHelper.Hosting
```

Below is sample code for the two approaches described in this document. Both examples build a console application compatible with Generic Host using `Microsoft.Extensions.Hosting`. It assumes you are creating a console application project.

### Sample for Fluent API Approach

When using Fluent API, implement as follows:

1. Add a connection string to SQL Server in appsettings.json
2. Register the SqlBulkCopier service in Program.cs (AddSqlBulkCopier())
3. Use CsvBulkCopierBuilder to set details and import CSV

Details of the implementation are as follows.

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

## License
This project is licensed under the [MIT License](LICENSE).
