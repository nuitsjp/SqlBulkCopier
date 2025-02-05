using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using SqlBulkCopier.FixedLength.Hosting;
using SqlBulkCopier.Hosting;

namespace SqlBulkCopier.Test.FixedLength.Hosting;

public class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddSqlBulkCopier_RegistersServicesSuccessfully()
    {
        // Arrange
        const string settings = """
                                {
                                  "SqlBulkCopier": {
                                    "DestinationTableName": "[dbo].[Customer]",
                                    "HasHeader": false,
                                    "Columns": {
                                      "CustomerId": {
                                        "Ordinal": 2
                                      },
                                      "BirthDate": {
                                        "Ordinal": 4,
                                        "SqlDbType": "Date",
                                        "Format": "yyyyMMdd"
                                      }
                                    }
                                  }
                                }
                                """;
        var services = new ServiceCollection();
        var configuration = BuildJsonConfig(settings);
        services.AddSingleton(configuration);


        // Act
        services.AddSqlBulkCopier();

        // Assert
        var provider = services.BuildServiceProvider();
        provider.GetService<IBulkCopierProvider>().ShouldNotBeNull();
    }

    static IConfiguration BuildJsonConfig(string json)
    {
        using var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        return new ConfigurationBuilder()
            .AddJsonStream(memoryStream)
            .Build();
    }

}