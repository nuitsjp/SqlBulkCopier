using System.Text;
using Microsoft.Extensions.Hosting;
using SqlBulkCopier;
using SqlBulkCopier.Hosting;

namespace Sample.CsvHelper;

/// <summary>
/// A service that uses the bulk copier to write data to the database.
/// </summary>
public class BulkCopyService(
    IBulkCopier bulkCopier, 
    SqlConnectionProvider sqlConnectionProvider,
    IHostApplicationLifetime applicationLifetime) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Open a connection to the database
        await using var connection = await sqlConnectionProvider.OpenAsync();

        // Write data to the database using the bulk copier
        await using Stream stream = File.OpenRead(@"Assets\Customer.csv");

        // Bulk copy to the database
        await bulkCopier.WriteToServerAsync(connection, stream, Encoding.UTF8, TimeSpan.FromMinutes(30));

        // Stop the application when the task is completed
        applicationLifetime.StopApplication();
    }
}