using System.Diagnostics;
using System.Text;
using Dapper;
using FluentTextTable;
using Microsoft.Extensions.Configuration;
using Sample.SetupSampleDatabase;
using SqlBulkCopier.CsvHelper.Hosting;
using SqlBulkCopier.FixedLength.Hosting;

namespace Benchmark;

public class SqlBulkCopierBenchmarks
{
    private const int Count = 10_000;
    private string CsvFile => $"Customer_{Count:###_###_###_###}.csv";
    private string FixedLengthFile => $"Customer_{Count:###_###_###_###}.dat";
    //private const string CsvFile = "Customer_10_000_000.csv";
    //private const string FixedLengthFile = "Customer_10_000_000.dat";
    public async Task RunAsync()
    {
        Console.WriteLine("Setup");
        await SetupAsync();

        (string File, string Name, Func<Task> Task)[] benchmarks =
        [
            ("CSV", "SQL BULK INSERT", NativeBulkInsert),
            ("CSV", "SqlBulkCopier", SqlBulkCopierFromCsv),
            ("Fixed Length", "SqlBulkCopier", SqlBulkCopierFromFixedLength)
        ];

        List<Result> results = [];
        foreach (var benchmark in benchmarks)
        {
            await TruncateAsync();

            Console.Write($"{benchmark.Name}...");
            var stopwatch = Stopwatch.StartNew();
            await benchmark.Task();
            stopwatch.Stop();
            Console.WriteLine($" {stopwatch.Elapsed}");
            results.Add(new Result(benchmark.File, benchmark.Name, stopwatch.Elapsed));
        }

        Build
            .TextTable<Result>()
            .WriteLine(results);
    }

    public async Task SetupAsync()
    {
        if (File.Exists(CsvFile) is false)
        {
            Console.WriteLine("Create CSV file...");
            await Customer.WriteCsvAsync(CsvFile, Count);
            Console.WriteLine();
        }
        if (File.Exists(FixedLengthFile) is false)
        {
            Console.WriteLine("Create Fixed Length file...");
            await Customer.WriteFixedLengthAsync(FixedLengthFile, Count);
            Console.WriteLine();
        }

        await Database.SetupAsync(true);
    }

    public async Task TruncateAsync()
    {
        Console.WriteLine("Truncate data and logs.");
        await using var connection = Database.Open();
        await connection.ExecuteAsync(
            """
            USE SqlBulkCopier;

            truncate table [SqlBulkCopier].[dbo].[Customer]

            DBCC SHRINKFILE ('SqlBulkCopier_log', 1);
            """
        );
    }

    public async Task NativeBulkInsert()
    {
        var fullPath = new FileInfo(CsvFile).Directory!.FullName;
        await using var connection = Database.Open();
        await connection.ExecuteAsync(
            $"""
            BULK INSERT SqlBulkCopier.dbo.Customer
            FROM '{fullPath}\{CsvFile}'
            WITH
            (
                FORMATFILE = '{fullPath}\Customer.fmt',
                FIRSTROW = 2,    -- ヘッダー行をスキップ
                DATAFILETYPE = 'char',
                CODEPAGE = '65001'  -- UTF-8エンコーディングを指定
            );
            """
            ,
            // コマンドタイムアウトを5分に変更
            commandTimeout: 300
        );
    }

    public async Task SqlBulkCopierFromCsv()
    {
        const string appsettings =
            """
            {
              "ConnectionStrings": {
                "DefaultConnection": "Data Source=.;Initial Catalog=SqlBulkCopier;Integrated Security=True;Trust Server Certificate=True"
              },
              "SqlBulkCopier": {
                "DestinationTableName": "[dbo].[Customer]",
                "HasHeader": true,
                "DefaultColumnSettings": {
                  "TrimMode": "TrimEnd",
                  "TreatEmptyStringAsNull": true
                },
                "Columns": {
                  "CustomerId": {},
                  "FirstName": {},
                  "LastName": {},
                  "Email": {},
                  "PhoneNumber": {},
                  "AddressLine1": {},
                  "AddressLine2": {},
                  "City": {},
                  "State": {},
                  "PostalCode": {},
                  "Country": {},
                  "BirthDate": {
                    "SqlDbType": "Date",
                    "Format": "yyyy-MM-dd"
                  },
                  "Gender": {},
                  "Occupation": {},
                  "Income": {},
                  "RegistrationDate": {
                    "SqlDbType": "DateTime",
                    "Format": "yyyy-MM-dd HH:mm:ss.fff"
                  },
                  "LastLogin": {
                    "SqlDbType": "DateTime",
                    "Format": "yyyy-MM-dd HH:mm:ss.fff"
                  },
                  "IsActive": { "SqlDbType": "Bit" }
                }
              }
            }
            """
        ;

        var configuration = BuildJsonConfig(appsettings);
        var bulkCopier = CsvBulkCopierParser.Parse(configuration);

        // Open a connection to the database
        await using var connection = Database.Open();

        // Write data to the database using the bulk copier
        await using Stream stream = File.OpenRead(CsvFile);

        // Bulk copy to the database
        await bulkCopier.WriteToServerAsync(connection, stream, Encoding.UTF8);

    }

    public async Task SqlBulkCopierFromFixedLength()
    {
        const string appsettings =
            """
            {
              "ConnectionStrings": {
                "DefaultConnection": "Data Source=.;Initial Catalog=SqlBulkCopier;Integrated Security=True;Trust Server Certificate=True"
              },
              "SqlBulkCopier": {
                "DestinationTableName": "[dbo].[Customer]",
                "DefaultColumnSettings": {
                  "TrimEnd": true,
                  "TreatEmptyStringAsNull": true
                },
                "Columns": {
                  "CustomerId": {
                    "Offset": 0,
                    "Length": 10
                  },
                  "Income": {
                    "Offset": 10,
                    "Length": 21,
                    "SqlDbType": "Decimal"
                  },
                  "FirstName": {
                    "Offset": 31,
                    "Length": 50
                  },
                  "LastName": {
                    "Offset": 81,
                    "Length": 50
                  },
                  "Email": {
                    "Offset": 131,
                    "Length": 100
                  },
                  "PhoneNumber": {
                    "Offset": 231,
                    "Length": 20
                  },
                  "AddressLine1": {
                    "Offset": 251,
                    "Length": 100
                  },
                  "AddressLine2": {
                    "Offset": 351,
                    "Length": 100
                  },
                  "City": {
                    "Offset": 451,
                    "Length": 50
                  },
                  "State": {
                    "Offset": 501,
                    "Length": 50
                  },
                  "PostalCode": {
                    "Offset": 551,
                    "Length": 10
                  },
                  "Country": {
                    "Offset": 561,
                    "Length": 50
                  },
                  "BirthDate": {
                    "Offset": 611,
                    "Length": 8,
                    "SqlDbType": "Date",
                    "Format": "yyyyMMdd"
                  },
                  "RegistrationDate": {
                    "Offset": 619,
                    "Length": 14,
                    "SqlDbType": "DateTime",
                    "Format": "yyyyMMddHHmmss"
                  },
                  "LastLogin": {
                    "Offset": 633,
                    "Length": 14,
                    "SqlDbType": "DateTime",
                    "Format": "yyyyMMddHHmmss"
                  },
                  "CreatedAt": {
                    "Offset": 647,
                    "Length": 14,
                    "SqlDbType": "DateTime",
                    "Format": "yyyyMMddHHmmss"
                  },
                  "UpdatedAt": {
                    "Offset": 661,
                    "Length": 14,
                    "SqlDbType": "DateTime",
                    "Format": "yyyyMMddHHmmss"
                  },
                  "Gender": {
                    "Offset": 675,
                    "Length": 10
                  },
                  "Occupation": {
                    "Offset": 685,
                    "Length": 50
                  },
                  "IsActive": {
                    "Offset": 735,
                    "Length": 1,
                    "SqlDbType": "Bit"
                  },
                  "Notes": {
                    "Offset": 736,
                    "Length": 500
                  }
                }
              }
            }
            """
        ;

        var configuration = BuildJsonConfig(appsettings);
        var bulkCopier = FixedLengthBulkCopierParser.Parse(configuration);

        // Open a connection to the database
        await using var connection = Database.Open();

        // Write data to the database using the bulk copier
        await using Stream stream = File.OpenRead(FixedLengthFile);

        // Bulk copy to the database
        await bulkCopier.WriteToServerAsync(connection, stream, Encoding.UTF8);

    }


    static IConfiguration BuildJsonConfig(string json)
    {
        using var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        return new ConfigurationBuilder()
            .AddJsonStream(memoryStream)
            .Build();
    }

}


public record Result(string File, string Name, TimeSpan Time);