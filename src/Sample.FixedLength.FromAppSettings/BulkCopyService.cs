using System.Text;
using Microsoft.Extensions.Hosting;
using SqlBulkCopier;
using SqlBulkCopier.Hosting;

namespace Sample.FixedLength.FromAppSettings;

/// <summary>
/// A service that uses the bulk copier to write data to the database.
/// </summary>
public class BulkCopyService(
    IBulkCopierBuilder bulkCopier, 
    SqlConnectionProvider sqlConnectionProvider,
    IHostApplicationLifetime applicationLifetime) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Open a connection to the database
        await using var connection = await sqlConnectionProvider.OpenAsync();

        // Write data to the database using the bulk copier
        await using Stream stream = File.OpenRead(@"Assets\Customer.dat");

        // Bulk copy to the database
        await bulkCopier.Build(connection).WriteToServerAsync(stream, Encoding.UTF8, TimeSpan.FromMinutes(30));

        // Stop the application when the task is completed
        applicationLifetime.StopApplication();
    }
}