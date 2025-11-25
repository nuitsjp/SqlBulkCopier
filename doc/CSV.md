# Introduction
SqlBulkCopier allows you to efficiently bulk copy CSV and fixed-length files into a database. This document explains how to use it with CSV files.

We provide two approaches for configuring CSV usage:

1. **Fluent API Approach**
   - Suitable if you prefer to configure in code
   - Allows for more detailed control and dynamic settings
   - IntelliSense support makes it easy to check configuration options

2. **Configuration Approach**
   - Suitable if you want to manage settings in appsettings.json
   - Allows for more concise code implementation
   - No need to recompile when changing settings
   - Easy to change settings per environment

## Table of Contents
- [Getting Started](#getting-started)
  - [Sample for Fluent API Approach](#sample-for-fluent-api-approach)
  - [Configuration Approach](#configuration-approach)
- [Detailed Settings](#detailed-settings)
  - [Processing CSV Files with Headers](#processing-csv-files-with-headers)
  - [Processing CSV Files without Headers](#processing-csv-files-without-headers)
  - [Setting Data Types](#setting-data-types)
  - [Trim Operations](#trim-operations)
  - [Treating Empty Strings as NULL](#treating-empty-strings-as-null)
  - [Custom Conversions](#custom-conversions)
  - [Creating an Instance of IBulkCopier](#creating-an-instance-of-ibulkcopier)
  - [Truncating Tables Before Bulk Insert](#truncating-tables-before-bulk-insert)
  - [Setting Truncate Method](#setting-truncate-method)
  - [Determining Rows to Import](#determining-rows-to-import)
  - [Retry Settings](#retry-settings)
  - [Setting Batch Size](#setting-batch-size)
  - [Setting Notification Event Rows](#setting-notification-event-rows)
  - [Setting Default Column Context](#setting-default-column-context)

## Getting Started
This library requires .NET 8.0 or .NET Framework 4.8. Install the following package from NuGet:

```
Install-Package SqlBulkCopier.CsvHelper.Hosting
```

Below are sample codes for the two approaches described in this document. Both examples build a console application that supports Generic Host using `Microsoft.Extensions.Hosting`. It is assumed that you create a console application project.

### Sample for Fluent API Approach

To use the Fluent API, follow these steps:

1. Add the connection string to SQL Server in appsettings.json
2. Register the SqlBulkCopier service in Program.cs (AddSqlBulkCopier())
3. Use CsvBulkCopierBuilder to configure details and import the CSV

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

For details, refer to [Detailed Settings](#detailed-settings).

### Configuration Approach

To use configuration files, follow these steps:

1. Add bulk copy settings to appsettings.json
2. Register the SqlBulkCopier service in Program.cs (AddSqlBulkCopier())
3. Inject IBulkCopierProvider via DI and load settings with the Provide method

Details of the implementation are as follows.

#### appsettings.json

```json
{
  "ConnectionStrings": {
    "DefaultConnection": "Data Source=.;Initial Catalog=SqlBulkCopier;Integrated Security=True;Trust Server Certificate=True"
  },
  "SqlBulkCopier1": {
    "DestinationTableName": "[dbo].[Customer]",
    "HasHeader": true,
    "TruncateBeforeBulkInsert": true,
    "DefaultColumnSettings": {
      "TrimMode": "TrimEnd",
      "TreatEmptyStringAsNull": true
    },
    "Columns": {
      "CustomerId": {},
      "FirstName": {},
      "LastName": {},
      "BirthDate": {
        "SqlDbType": "Date",
        "Format": "yyyy-MM-dd"
      },
      "IsActive": { "SqlDbType": "Bit" }
    }
  }
}
```

For details, refer to [Detailed Settings](#detailed-settings).

#### Program.cs

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Sample.CsvHelper.FromAppSettings;
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
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using SqlBulkCopier.Hosting;

namespace Sample.CsvHelper.FromAppSettings;

public class BulkCopyService(
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
            Path.Combine(AppContext.BaseDirectory, "Assets", "Customer.csv"));

        // Bulk copy to the database
        await bulkCopier.WriteToServerAsync(stream, Encoding.UTF8, TimeSpan.FromMinutes(30));

        // Stop the application when the task is completed
        applicationLifetime.StopApplication();
    }
}
```

## Detailed Settings

The following table shows the features that can be implemented in CSV bulk copy settings and how to configure them for each approach.

| Purpose | Fluent API | appsettings.json |
|------|------------|------------------|
| [Processing CSV Files with Headers](#processing-csv-files-with-headers) | `CreateWithHeader` | `"HasHeader": true` |
| [Processing CSV Files without Headers](#processing-csv-files-without-headers) | `CreateNoHeader` | `"HasHeader": false` |
| [Setting Data Types](#setting-data-types) | `AsInt`, `AsDate`, `AsDecimal`, etc. | `"SqlDbType": "Int"`, `"Date"`, `"Decimal"`, etc. |
| [Trim Operations](#trim-operations) | `Trim`, `TrimStart`, `TrimEnd` | `"TrimMode": "Trim"`, `"TrimStart"`, `"TrimEnd"` |
| [Treating Empty Strings as NULL](#treating-empty-strings-as-null) | `TreatEmptyStringAsNull` | `"TreatEmptyStringAsNull": true`, `false` |
| [Custom Conversions](#custom-conversions) | `Convert` | Not supported in configuration files |
| [Creating an Instance of IBulkCopier](#creating-an-instance-of-ibulkcopier) | `Build` | Not supported in configuration files |
| [Truncating Tables Before Bulk Insert](#truncating-tables-before-bulk-insert) | `SetTruncateBeforeBulkInsert` | `"TruncateBeforeBulkInsert": true/false` |
| [Setting Truncate Method](#setting-truncate-method) | `SetTruncateMethod` | `"TruncateMethod": "Truncate"`, `"Delete"` |
| [Determining Rows to Import](#determining-rows-to-import) | `SetRowFilter` | `"RowFilter": { ... }` |
| [Retry Settings](#retry-settings) | `SetMaxRetryCount`, `SetInitialDelay`, `SetUseExponentialBackoff` | `"MaxRetryCount": 3`, `"InitialDelay": "00:00:05"`, `"UseExponentialBackoff": true` |
| [Setting Batch Size](#setting-batch-size) | `SetBatchSize` | `"BatchSize": 1000` |
| [Setting Notification Event Rows](#setting-notification-event-rows) | `SetNotifyAfter` | `"NotifyAfter": 500` |
| [Setting Default Column Context](#setting-default-column-context) | `SetDefaultColumnContext` | `"DefaultColumnSettings": { ... }` |

### Usage

#### [Processing CSV Files with Headers](#detailed-settings)
This method creates a builder for processing CSV files with headers. The following code example shows how to map database columns using CSV file header names.

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .AddColumnMapping("CustomerId")
    .AddColumnMapping("FirstName")
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

Example setting in appsettings.json:

```json
{
  "SqlBulkCopier": {
    "DestinationTableName": "[dbo].[Customer]",
    "HasHeader": true,
    "Columns": {
      "CustomerId": {},
      "FirstName": {}
    }
  }
}
```

#### [Processing CSV Files without Headers](#detailed-settings)
This method creates a builder for processing CSV files without headers. The following code example shows how to map database columns using CSV file column positions.

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateNoHeader("[dbo].[Customer]")
    .AddColumnMapping("CustomerId", 0)
    .AddColumnMapping("FirstName", 1)
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

Example setting in appsettings.json:

```json
{
  "SqlBulkCopier": {
    "DestinationTableName": "[dbo].[Customer]",
    "HasHeader": false,
    "Columns": {
      "CustomerId": { "Ordinal": 0 },
      "FirstName": { "Ordinal": 1 }
    }
  }
}
```

#### [Setting Data Types](#detailed-settings)
You can use `IColumnContext` to map CSV data to SQL Server data types. The following code example shows how to map to some representative data types.

If automatic conversion is possible with SqlBulkcopy, it is not always necessary to specify individually.

```csharp
    .AddColumnMapping("BirthDate", c => c.AsDate("yyyy-MM-dd"))
    .AddColumnMapping("Salary", c => c.AsDecimal())
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

Example setting in appsettings.json:

```json
{
  "SqlBulkCopier": {
    "DestinationTableName": "[dbo].[Customer]",
    "HasHeader": true,
    "Columns": {
      "BirthDate": {
        "SqlDbType": "Date",
        "Format": "yyyy-MM-dd"
      },
      "Salary": {
        "SqlDbType": "Decimal"
      }
    }
  }
}
```

 Valid types are as follows. If not listed below, it is not necessary to specify as the default converter of SqlBulkCopy will be used.

| SQL Server Type   | Fluent API (IColumnContext) | Example setting in appsettings.json       |
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


#### [Trim Operations](#detailed-settings)
You can perform trim operations on strings. This allows you to remove leading and trailing spaces or specific characters from the data. The following code example shows how to use trim operations.

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .AddColumnMapping("FirstName", c => c.Trim())
    .AddColumnMapping("LastName", c => c.TrimEnd())
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

Example setting in appsettings.json:

```json
{
  "SqlBulkCopier": {
    "DestinationTableName": "[dbo].[Customer]",
    "HasHeader": true,
    "DefaultColumnSettings": {
      "TrimMode": "Trim",
      "TrimChars": " "
    },
    "Columns": {
      "FirstName": {},
      "LastName": {
        "TrimMode": "TrimEnd"
      }
    }
  }
}
```

#### [Treating Empty Strings as NULL](#detailed-settings)
You can treat empty strings as NULL when inserting into the database. The following code example shows how to do this.

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .AddColumnMapping("MiddleName", c => c.TreatEmptyStringAsNull())
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

Example setting in appsettings.json:

```json
{
  "SqlBulkCopier": {
    "DestinationTableName": "[dbo].[Customer]",
    "HasHeader": true,
    "DefaultColumnSettings": {
      "TreatEmptyStringAsNull": true
    },
    "Columns": {
      "MiddleName": {}
    }
  }
}
```

#### [Custom Conversions](#detailed-settings)
You can specify custom conversion functions. This allows you to convert strings to any object. The following code example shows how to use custom conversions.

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .AddColumnMapping("CustomField", c => c.Convert(value => CustomConversion(value)))
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

Example setting in appsettings.json is not supported.

#### [Creating an Instance of IBulkCopier](#detailed-settings)
The `Build` method is an important method for creating an instance of `IBulkCopier`. There are four overloads:

1. **`Build(SqlConnection connection)`**:
   - Creates an instance of `IBulkCopier` using the specified SQL connection.
   - The `connection` parameter must be opened before performing the bulk copy operation.
   - Throws `ArgumentNullException` if `connection` is `null`.

2. **`Build(string connectionString)`**:
   - Creates an instance of `IBulkCopier` using the specified connection string.
   - The `connectionString` parameter must contain all the information needed to establish a connection to SQL Server.
   - Throws `ArgumentNullException` if `connectionString` is `null` or empty.
   - Throws `ArgumentException` if `connectionString` is invalid.

3. **`Build(string connectionString, SqlBulkCopyOptions copyOptions)`**:
   - Creates an instance of `IBulkCopier` using the specified connection string and copy options.
   - The `connectionString` parameter must contain all the information needed to establish a connection to SQL Server.
   - The `copyOptions` parameter is the SQL bulk copy options to configure the behavior of the operation.
   - Throws `ArgumentNullException` if `connectionString` is `null` or empty.
   - Throws `ArgumentException` if `connectionString` is invalid.

4. **`Build(SqlConnection connection, SqlBulkCopyOptions copyOptions, SqlTransaction externalTransaction)`**:
   - Creates an instance of `IBulkCopier` using the specified connection, options, and transaction.
   - The `connection` parameter must be opened before performing the bulk copy operation.
   - The `copyOptions` parameter is the SQL bulk copy options to configure the behavior of the operation.
   - The `externalTransaction` parameter is the external transaction to be used for the bulk copy operation. All bulk copy operations will be part of this transaction.
   - Throws `ArgumentNullException` if `connection` or `externalTransaction` is `null`.

Example setting in appsettings.json:

```json
{
  "SqlBulkCopier": {
    "DestinationTableName": "[dbo].[Customer]",
    "HasHeader": true,
    "ConnectionString": "YourDatabaseConnectionString"
  }
}
```

#### [Truncating Tables Before Bulk Insert](#detailed-settings)
This function is used to truncate the specified table before performing the bulk copy. The following code example shows how to do this.

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .SetTruncateBeforeBulkInsert(true)
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

Example setting in appsettings.json:

```json
{
  "SqlBulkCopier": {
    "DestinationTableName": "[dbo].[Customer]",
    "HasHeader": true,
    "TruncateBeforeBulkInsert": true
  }
}
```

#### [Setting Truncate Method](#detailed-settings)
This function sets the method used to clear data from the table before the bulk insert. By default, `TRUNCATE TABLE` is used, but you can choose to use `DELETE FROM` instead. This is useful when the table has foreign key constraints, as `TRUNCATE TABLE` cannot be used in such cases.

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .SetTruncateBeforeBulkInsert(true)
    .SetTruncateMethod(TruncateMethod.Delete)
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

Example setting in appsettings.json:

```json
{
  "SqlBulkCopier": {
    "DestinationTableName": "[dbo].[Customer]",
    "HasHeader": true,
    "TruncateBeforeBulkInsert": true,
    "TruncateMethod": "Delete"
  }
}
```

| Method | Description |
|--------|-------------|
| `Truncate` | Uses `TRUNCATE TABLE` statement. Default. Faster but cannot be used with foreign key constraints. |
| `Delete` | Uses `DELETE FROM` statement. Slower but works with foreign key constraints. |

This feature allows you to evaluate each row of CSV data and determine whether to import it. Only rows that meet the specified conditions will be copied to the database.

Below are examples of how to configure this in both Fluent API and `appsettings.json`.

##### Fluent API Example

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .SetRowFilter(reader => reader.Parser.RawRecord.StartsWith("Comment"))
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

In this example, rows starting with `"Comment"` are excluded from being read. Note that `RawRecord` includes line breaks.

##### appsettings.json Example

You can specify conditions using the `RowFilter` section as follows:

```json
{
  "SqlBulkCopier": {
    "DestinationTableName": "[dbo].[Customer]",
    "HasHeader": true,
    "RowFilter": {
      "Equals": [ "Comment"],
      "StartsWith": [ "Prefix"],
      "EndsWith": [ "Suffix" ]
    }
  }
}
```

- **`Equals`**: Excludes rows that match the specified value.
- **`StartsWith`**: Excludes rows that start with the specified value.
- **`EndsWith`**: Excludes rows that end with the specified value.

##### Notes

- `RowFilter` can combine multiple conditions.
- Conditions are combined with AND (only rows that meet all conditions are included).
- The settings in `appsettings.json` provide equivalent functionality to the Fluent API.

#### [Retry Settings](#detailed-settings)
Retry settings allow you to automatically retry under certain conditions. The conditions for retrying are as follows:
- **Using a connection string**: Retrying is possible when connecting to the database using a connection string rather than an external connection.
- **Table truncation is enabled**: Retrying is possible when table truncation is enabled.

Retry settings include the following options:
- **SetMaxRetryCount**: Sets the maximum number of retries.
- **SetInitialDelay**: Sets the initial delay time between retries.
- **SetUseExponentialBackoff**: When enabled, the wait time between retries increases exponentially. For example, if the initial retry waits for 5 seconds, the next retry will wait for 10 seconds, and the next for 20 seconds. This helps to avoid rapid consecutive retries and reduces system load.

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .SetMaxRetryCount(3)
    .SetInitialDelay(TimeSpan.FromSeconds(5))
    .SetUseExponentialBackoff(true)
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

Example setting in appsettings.json:

```json
{
  "SqlBulkCopier": {
    "DestinationTableName": "[dbo].[Customer]",
    "HasHeader": true,
    "MaxRetryCount": 3,
    "InitialDelay": "00:00:05",
    "UseExponentialBackoff": true
  }
}
```

#### [Setting Batch Size](#detailed-settings)
This function sets the batch size for the bulk copy operation. The following code example shows how to do this.

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .SetBatchSize(1000)
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

Example setting in appsettings.json:

```json
{
  "SqlBulkCopier": {
    "DestinationTableName": "[dbo].[Customer]",
    "HasHeader": true,
    "BatchSize": 1000
  }
}
```

#### [Setting Notification Event Rows](#detailed-settings)
This function sets the number of rows to trigger notification events. The following code example shows how to do this.

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .SetNotifyAfter(500)
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

Example setting in appsettings.json:

```json
{
  "SqlBulkCopier": {
    "DestinationTableName": "[dbo].[Customer]",
    "HasHeader": true,
    "NotifyAfter": 500
  }
}
```

#### [Setting Default Column Context](#detailed-settings)
This function sets the default context for all columns. The following code example shows how to do this.

If different settings are applied to individual columns, those settings take precedence.

```csharp
var bulkCopier = CsvBulkCopierBuilder
    .CreateWithHeader("[dbo].[Customer]")
    .SetDefaultColumnContext(c => c.TrimEnd().TreatEmptyStringAsNull())
    .Build(configuration.GetConnectionString("DefaultConnection")!);
```

Example setting in appsettings.json:

```json
{
  "SqlBulkCopier": {
    "DestinationTableName": "[dbo].[Customer]",
    "HasHeader": true,
    "DefaultColumnSettings": {
      "TrimMode": "TrimEnd",
      "TreatEmptyStringAsNull": true
    }
  }
}
```

