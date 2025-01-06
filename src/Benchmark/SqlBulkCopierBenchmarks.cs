using System.Globalization;
using System.Text;
using BenchmarkDotNet.Attributes;
using CsvHelper;
using Dapper;
using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Sample.SetupSampleDatabase;
using SqlBulkCopier.CsvHelper.Hosting;
using SqlBulkCopier.FixedLength.Hosting;
using Z.Dapper.Plus;

namespace Benchmark;

[Config(typeof(TestConfig))]
//[SimpleJob(RuntimeMoniker.Net80, launchCount: 1, warmupCount: 0, iterationCount: 1)]
public class SqlBulkCopierBenchmarks
{
    [Params(
        //1_000
        10_000
        , 100_000
        , 1_000_000
        //, 10_000_000
        )]
    public int Count = 100_000;

    private const string ArtifactsPath = @"C:\Repos\SqlBulkCopier\src\Sample.SetupSampleDatabase\Asserts";

    private string CsvFile => $@"{ArtifactsPath}\Customer_{Count:###_###_###_###}.csv";
    private string FixedLengthFile => $@"{ArtifactsPath}\Customer_{Count:###_###_###_###}.dat";

    private static readonly int CommandTimeout = (int)TimeSpan.FromMinutes(10).TotalSeconds;

    [IterationSetup]
    public void Setup()
    {
        if (File.Exists(CsvFile) is false)
        {
            Console.WriteLine("Create CSV file...");
            Customer.WriteCsvAsync(CsvFile, Count).Wait();
            Console.WriteLine();
        }
        if (File.Exists(FixedLengthFile) is false)
        {
            Console.WriteLine("Create Fixed Length file...");
            Customer.WriteFixedLengthAsync(FixedLengthFile, Count).Wait();
            Console.WriteLine();
        }

        // Database.SetupAsync(true).Wait();

        using var connection = Database.Open();
        connection.Execute(
            """
            USE SqlBulkCopier;

            truncate table [SqlBulkCopier].[dbo].[Customer]

            DBCC SHRINKFILE ('SqlBulkCopier_log', 1);

            DECLARE @DataFileSize INT;
            DECLARE @LogFileSize INT;
            DECLARE @TargetDataSize INT = 5120; -- 目標データファイルサイズ（MB）
            DECLARE @TargetLogSize INT = 10240; -- 目標ログファイルサイズ（MB）
            
            -- 現在のファイルサイズを取得（MB単位）
            SELECT @DataFileSize = size/128
            FROM sys.master_files
            WHERE database_id = DB_ID('SqlBulkCopier')
            AND name = 'SqlBulkCopier';
            
            SELECT @LogFileSize = size/128
            FROM sys.master_files
            WHERE database_id = DB_ID('SqlBulkCopier')
            AND name = 'SqlBulkCopier_log';
            
            -- データファイルのサイズチェックと変更
            IF @DataFileSize < @TargetDataSize
            BEGIN
                ALTER DATABASE SqlBulkCopier 
                MODIFY FILE (
                    NAME = 'SqlBulkCopier',
                    SIZE = 5120MB
                );
            END
            
            -- ログファイルのサイズチェックと変更
            IF @LogFileSize < @TargetLogSize
            BEGIN
                ALTER DATABASE SqlBulkCopier 
                MODIFY FILE (
                    NAME = 'SqlBulkCopier_log',
                    SIZE = 10240MB
                );
            END
            
            """
        );
    }

    [Benchmark(Description = "CSV : BULK INSERT")]
    public async Task NativeBulkInsertFromCsv()
    {
        var fullPath = new FileInfo(CsvFile).Directory!.FullName;
        await using var connection = Database.Open();
        await connection.ExecuteAsync(
            $"""
            BULK INSERT SqlBulkCopier.dbo.Customer
            FROM '{CsvFile}'
            WITH
            (
                FORMATFILE = '{fullPath}\Customer.fmt',
                FIRSTROW = 2,    -- ヘッダー行をスキップ
                DATAFILETYPE = 'char',
                CODEPAGE = '65001'  -- UTF-8エンコーディングを指定
            );
            """
            ,
            commandTimeout: CommandTimeout
        );
    }

    [Benchmark(Description = "CSV : SqlBulkCopier")]
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

    //[Benchmark(Description = "CSV : CsvHelper and Dapper")]
    //public async Task CsvHelperAndDapper()
    //{
    //    using var reader = new StreamReader(CsvFile);
    //    using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
    //    csv.Context.RegisterClassMap<CustomerMap>();
    //    var customers = csv.GetRecords<Customer>();

    //    // Open a connection to the database
    //    await using var connection = Database.Open();

    //    foreach (var customer in customers)
    //    {
    //        await connection.SingleInsertAsync(customer);
    //    }

    //    //await connection.BulkInsertAsync(customers);
    //}

    [Benchmark(Description = "CSV : CsvHelper and Dapper Plus")]
    public async Task CsvHelperAndDapperPlus()
    {
        using var reader = new StreamReader(CsvFile);
        using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
        csv.Context.RegisterClassMap<CustomerMap>();
        var customers = csv.GetRecords<Customer>();

        // Open a connection to the database
        await using var connection = Database.Open();
        await connection.BulkInsertAsync(customers);
    }

    //[Benchmark(Description = "CSV : EF Core AddRangeAsync")]
    //public async Task CsvEfCore()
    //{
    //    using var reader = new StreamReader(CsvFile);
    //    using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
    //    csv.Context.RegisterClassMap<CustomerMap>();
    //    var customers = csv.GetRecords<Customer>();

    //    // 既存のSqlConnectionを使用する場合
    //    await using var connection = Database.Open();
    //    var optionsBuilder = new DbContextOptionsBuilder<ApplicationDbContext>();
    //    optionsBuilder.UseSqlServer(connection);
    //    var options = optionsBuilder.Options;
    //    var context = new ApplicationDbContext(options);
    //    await context.Customers.AddRangeAsync(customers);
    //    await context.SaveChangesAsync();
    //}

    [Benchmark(Description = "CSV : EF Core Bulk Extensions")]
    public async Task CsvEfCoreWithBulkExtensions()
    {
        using var reader = new StreamReader(CsvFile);
        using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
        csv.Context.RegisterClassMap<CustomerMap>();
        var customers = csv.GetRecords<Customer>();

        // 既存のSqlConnectionを使用する場合
        await using var connection = Database.Open();
        var optionsBuilder = new DbContextOptionsBuilder<ApplicationDbContext>();
        optionsBuilder.UseSqlServer(connection);
        var options = optionsBuilder.Options;
        var context = new ApplicationDbContext(options);
        await context.BulkInsertAsync(customers);
        await context.SaveChangesAsync();
    }

    [Benchmark(Description = "Fixed Length : BULK INSERT")]
    public async Task NativeBulkInsertFromFixedLength()
    {
        var fullPath = new FileInfo(FixedLengthFile).Directory!.FullName;
        await using var connection = Database.Open();
        await connection.ExecuteAsync(
            $"""
             BULK INSERT SqlBulkCopier.dbo.Customer
             FROM '{FixedLengthFile}'
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
            commandTimeout: CommandTimeout
        );
    }


    [Benchmark(Description = "Fixed Length : SqlBulkCopier")]
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