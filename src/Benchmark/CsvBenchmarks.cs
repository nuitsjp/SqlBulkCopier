using System.Globalization;
using System.Text;
using System.Threading.Channels;
using BenchmarkDotNet.Attributes;
using CsvHelper;
using Dapper;
using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Sample.SetupSampleDatabase;
using SqlBulkCopier.CsvHelper.Hosting;
using Z.Dapper.Plus;

namespace Benchmark;

[Config(typeof(TestConfig))]
//[SimpleJob(RuntimeMoniker.Net80, launchCount: 1, warmupCount: 0, iterationCount: 1)]
[RPlotExporter]
public class CsvBenchmarks : BenchmarksBase
{
    [Benchmark(Description = "BULK INSERT")]
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
            commandTimeout: (int)CommandTimeout.TotalSeconds
        );
        AssertResultCount(connection);
    }

    [Benchmark(Description = "SqlBulkCopier", Baseline = true)]
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
        await bulkCopier.WriteToServerAsync(connection, stream, Encoding.UTF8, CommandTimeout);
        AssertResultCount(connection);

    }

    //[Benchmark(Description = "Dapper Single Insert")]
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
    //}

    //[Benchmark(Description = "Dapper Plus Bulk Insert")]
    //public async Task CsvHelperAndDapperPlus()
    //{
    //    using var reader = new StreamReader(CsvFile);
    //    using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
    //    csv.Context.RegisterClassMap<CustomerMap>();
    //    var customers = csv.GetRecords<Customer>();

    //    // Open a connection to the database
    //    await using var connection = Database.Open();
    //    await connection.BulkInsertAsync(customers);
    //}

    //[Benchmark(Description = "EF Core AddRangeAsync")]
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

    //[Benchmark(Description = "EF Core Bulk Extensions")]
    //public async Task EfCoreWithBulkExtensionsFromCsv()
    //{
    //    using var reader = new StreamReader(CsvFile);
    //    using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
    //    csv.Context.RegisterClassMap<CustomerMap>();
    //    var customers = csv.GetRecords<Customer>().ToArray();

    //    既存のSqlConnectionを使用する場合
    //   await using var connection = Database.Open();
    //    var optionsBuilder = new DbContextOptionsBuilder<ApplicationDbContext>();
    //    optionsBuilder.UseSqlServer(connection);
    //    var options = optionsBuilder.Options;

    //    var bulkConfig = new BulkConfig
    //    {
    //        BulkCopyTimeout = (int)CommandTimeout.TotalSeconds,
    //        EnableStreaming = true
    //    };

    //    var context = new ApplicationDbContext(options);
    //    await context.BulkInsertAsync(customers, bulkConfig);
    //    await context.SaveChangesAsync();
    //    AssertResultCount(connection);
    //}


    //[Benchmark(Description = "EF Core Bulk Extensions manual batch.")]
    //// 並列処理を行う場合
    //public async Task EfCoreWithBulkExtensionsManualBatchFromCsv()
    //{
    //    const int batchSize = 5000;
    //    var bulkConfig = new BulkConfig
    //    {
    //        BulkCopyTimeout = 0,
    //        BatchSize = batchSize,
    //        EnableStreaming = true
    //    };

    //    var channel = Channel.CreateBounded<List<Customer>>(new BoundedChannelOptions(3)
    //    {
    //        FullMode = BoundedChannelFullMode.Wait
    //    });

    //    // 読み取りタスク
    //    var readTask = Task.Run(async () =>
    //    {
    //        try
    //        {
    //            using var reader = new StreamReader(CsvFile);
    //            using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
    //            csv.Context.RegisterClassMap<CustomerMap>();

    //            var buffer = new List<Customer>(batchSize);
    //            await foreach (var customer in csv.GetRecordsAsync<Customer>())
    //            {
    //                buffer.Add(customer);
    //                if (buffer.Count >= batchSize)
    //                {
    //                    await channel.Writer.WriteAsync(buffer);
    //                    buffer = new List<Customer>(batchSize);
    //                }
    //            }

    //            if (buffer.Any())
    //            {
    //                await channel.Writer.WriteAsync(buffer);
    //            }
    //        }
    //        finally
    //        {
    //            channel.Writer.Complete();
    //        }
    //    });

    //    // 書き込みタスク
    //    await using var connection = Database.Open();
    //    var writeTask = Task.Run(async () =>
    //    {
    //        var optionsBuilder = new DbContextOptionsBuilder<ApplicationDbContext>();
    //        optionsBuilder.UseSqlServer(connection);
    //        var context = new ApplicationDbContext(optionsBuilder.Options);

    //        await foreach (var batch in channel.Reader.ReadAllAsync())
    //        {
    //            await context.BulkInsertAsync(batch, bulkConfig);
    //        }
    //    });

    //    await Task.WhenAll(readTask, writeTask);
    //    AssertResultCount(connection);
    //}

    static IConfiguration BuildJsonConfig(string json)
    {
        using var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        return new ConfigurationBuilder()
            .AddJsonStream(memoryStream)
            .Build();
    }
}
