using System.Data;
using System.Text;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using SqlBulkCopier.Hosting;

namespace SqlBulkCopier.Test.Hosting;

public class SqlConnectionProviderTests
{
    [Fact]
    public async Task OpenAsync_WhenDefaultName()
    {
        // Arrange
        const string settings = """
                                {
                                  "ConnectionStrings": {
                                    "DefaultConnection": "Data Source=.;Initial Catalog=SqlBulkCopier;Integrated Security=True;Trust Server Certificate=True"
                                  }
                                }
                                """;
        var configuration = BuildJsonConfig(settings);
        var provider = new SqlConnectionProvider(configuration);

        // Act
        var connection = await provider.OpenAsync();

        // Assert
        connection.Should().NotBeNull();
        Assert.NotNull(connection);
        Assert.Equal(ConnectionState.Open, connection.State);
    }

    [Fact]
    public async Task OpenAsync_WhenOriginalName()
    {
        // Arrange
        const string settings = """
                                {
                                  "ConnectionStrings": {
                                    "OriginalConnection": "Data Source=.;Initial Catalog=SqlBulkCopier;Integrated Security=True;Trust Server Certificate=True"
                                  }
                                }
                                """;
        var configuration = BuildJsonConfig(settings);
        var provider = new SqlConnectionProvider(configuration);

        // Act
        var connection = await provider.OpenAsync("OriginalConnection");

        // Assert
        connection.Should().NotBeNull();
        Assert.NotNull(connection);
        Assert.Equal(ConnectionState.Open, connection.State);
    }

    [Fact]
    public async Task OpenAsync_WhenNotExists()
    {
        // Arrange
        const string settings = """
                                {
                                  "ConnectionStrings": {
                                  }
                                }
                                """;
        var configuration = BuildJsonConfig(settings);
        var provider = new SqlConnectionProvider(configuration);

        // Act & Assert
        var act = () => provider.OpenAsync();
        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    static IConfiguration BuildJsonConfig(string json)
    {
        using var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        return new ConfigurationBuilder()
            .AddJsonStream(memoryStream)
            .Build();
    }

}