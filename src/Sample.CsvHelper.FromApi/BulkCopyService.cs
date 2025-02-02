using System.Text;
using Microsoft.Extensions.Hosting;
using SqlBulkCopier.CsvHelper;
using SqlBulkCopier.Hosting;

namespace Sample.CsvHelper.FromApi;

public class BulkCopyService(
    SqlConnectionProvider sqlConnectionProvider,
    IHostApplicationLifetime applicationLifetime) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Open a connection to the database
        await using var connection = await sqlConnectionProvider.OpenAsync();

        var bulkCopier = CsvBulkCopierBuilder
            .CreateWithHeader("[dbo].[Customer]")
            .SetTruncateBeforeBulkInsert(true)
            .SetDefaultColumnContext(c => c.TrimEnd().TreatEmptyStringAsNull())
            .AddColumnMapping("CustomerId")
            .AddColumnMapping("FirstName")
            .AddColumnMapping("LastName")
            .AddColumnMapping("BirthDate", c => c.AsDate("yyyy-MM-dd"))
            .AddColumnMapping("IsActive", c => c.AsBit())
            .Build(connection);

        await using var stream = File.OpenRead(
            Path.Combine(AppContext.BaseDirectory, "Assets", "Customer.csv"));
        await bulkCopier.WriteToServerAsync(stream, Encoding.UTF8, TimeSpan.FromMinutes(30));

        Console.WriteLine("Bulk copy completed");

        // Stop the application when the task is completed
        applicationLifetime.StopApplication();
    }
}