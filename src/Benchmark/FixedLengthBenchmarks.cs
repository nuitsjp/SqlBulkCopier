using System.Text;
using BenchmarkDotNet.Attributes;
using Dapper;
using Microsoft.Extensions.Configuration;
using Sample.SetupSampleDatabase;
using SqlBulkCopier.FixedLength.Hosting;

namespace Benchmark;

[Config(typeof(TestConfig))]
public class FixedLengthBenchmarks : BenchmarksBase
{
    [Benchmark(Description = "BULK INSERT", Baseline = true)]
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
            commandTimeout: (int)CommandTimeout.TotalSeconds
        );

        AssertResultCount(connection);
    }


    [Benchmark(Description = "SqlBulkCopier")]
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
        var builder = FixedLengthBulkCopierParser.Parse(configuration);

        // Open a connection to the database
        await using var connection = Database.Open();

        // Write data to the database using the bulk copier
        await using Stream stream = File.OpenRead(FixedLengthFile);

        // Bulk copy to the database
        var bulkCopier = builder.Build(connection);
        await bulkCopier.WriteToServerAsync(stream, Encoding.UTF8, CommandTimeout);

        AssertResultCount(connection);
    }


    static IConfiguration BuildJsonConfig(string json)
    {
        using var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        return new ConfigurationBuilder()
            .AddJsonStream(memoryStream)
            .Build();
    }
}
