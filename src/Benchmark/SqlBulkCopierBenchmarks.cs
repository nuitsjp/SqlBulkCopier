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
    private const int Count = 1_000_000;
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
            ("CSV", "SQL BULK INSERT", NativeBulkInsertFromCsv),
            ("CSV", "SqlBulkCopier", SqlBulkCopierFromCsv),
            ("Fixed Length", "SQL BULK INSERT", NativeBulkInsertFromFixedLength),
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

        //await Database.SetupAsync(true);
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

    public async Task NativeBulkInsertFromCsv()
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

    public async Task NativeBulkInsertFromFixedLength()
    {
        var fullPath = new FileInfo(FixedLengthFile).Directory!.FullName;
        await using var connection = Database.Open();
        await connection.ExecuteAsync(
            $"""
             BULK INSERT SqlBulkCopier.dbo.Customer
             FROM '{fullPath}\{FixedLengthFile}'
             WITH
             (
                 FORMATFILE = '{fullPath}\Customer.xml',
             	 CODEPAGE = '65001',  -- UTF-8
                 FIRSTROW = 1,
                 DATAFILETYPE = 'widechar',  -- UTF-8ファイル用
                 MAXERRORS = 0,
                 ROWTERMINATOR = '\r\n'
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
                  "FirstName": {
                    "Offset": 10,
                    "Length": 50
                  },
                  "LastName": {
                    "Offset": 60,
                    "Length": 50
                  },
                  "Email": {
                    "Offset": 110,
                    "Length": 100
                  },
                  "PhoneNumber": {
                    "Offset": 210,
                    "Length": 20
                  },
                  "AddressLine1": {
                    "Offset": 230,
                    "Length": 100
                  },
                  "AddressLine2": {
                    "Offset": 330,
                    "Length": 100
                  },
                  "City": {
                    "Offset": 430,
                    "Length": 50
                  },
                  "State": {
                    "Offset": 480,
                    "Length": 50
                  },
                  "PostalCode": {
                    "Offset": 530,
                    "Length": 10
                  },
                  "Country": {
                    "Offset": 540,
                    "Length": 50
                  },
                  "BirthDate": {
                    "Offset": 590,
                    "Length": 10
                  },
                  "Gender": {
                    "Offset": 600,
                    "Length": 10
                  },
                  "Occupation": {
                    "Offset": 610,
                    "Length": 50
                  },
                  "Income": {
                    "Offset": 660,
                    "Length": 21
                  },
                  "RegistrationDate": {
                    "Offset": 681,
                    "Length": 23
                  },
                  "LastLogin": {
                    "Offset": 704,
                    "Length": 23
                  },
                  "IsActive": {
                    "Offset": 727,
                    "Length": 1,
                    "SqlDbType": "Bit"
                  },
                  "Notes": {
                    "Offset": 728,
                    "Length": 500
                  },
                  "CreatedAt": {
                    "Offset": 1228,
                    "Length": 23
                  },
                  "UpdatedAt": {
                    "Offset": 1251,
                    "Length": 23
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