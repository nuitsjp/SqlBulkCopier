using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using SqlBulkCopier.FixedLength;

namespace Sample.FixedLength.FromApi;

public class FixedLengthBulkCopyService(
    IConfiguration configuration,
    IHostApplicationLifetime applicationLifetime)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Open a connection to the database
        await using var connection = new SqlConnection(configuration.GetConnectionString("DefaultConnection"));
        await connection.OpenAsync(stoppingToken);

        var bulkCopier = FixedLengthBulkCopierBuilder
            .Create("[dbo].[Customer]")
            .SetTruncateBeforeBulkInsert(true)
            .SetDefaultColumnContext(c => c.TrimEnd().TreatEmptyStringAsNull())
            .AddColumnMapping("CustomerId", 0, 10)
            .AddColumnMapping("FirstName", 10, 50)
            .AddColumnMapping("LastName", 60, 50)
            .AddColumnMapping("BirthDate", 590, 10, c => c.AsDate("yyyy-MM-dd"))
            .AddColumnMapping("IsActive", 727, 1, c => c.AsBit())
            .Build(connection);

        await using var stream = File.OpenRead(Path.Combine(AppContext.BaseDirectory, "Assets", "Customer.dat"));
        await bulkCopier.WriteToServerAsync(stream, new UTF8Encoding(false), TimeSpan.FromMinutes(30));

        Console.WriteLine("Fixed length bulk copy completed");

        applicationLifetime.StopApplication();
    }
}