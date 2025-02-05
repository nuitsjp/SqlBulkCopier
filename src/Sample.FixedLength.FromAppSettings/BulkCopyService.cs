using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using SqlBulkCopier;
using SqlBulkCopier.Hosting;

namespace Sample.FixedLength.FromAppSettings;

/// <summary>
/// A service that uses the bulk copier to write data to the database.
/// </summary>
public class BulkCopyService(
    IConfiguration configuration,
    IBulkCopierProvider bulkCopierProvider,
    IHostApplicationLifetime applicationLifetime) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Provide the bulk copier
        var bulkCopier = bulkCopierProvider
            .Provide("SqlBulkCopier1", configuration.GetConnectionString("DefaultConnection")!);

        // Write data to the database using the bulk copier
        await using var stream = File.OpenRead(
            Path.Combine(AppContext.BaseDirectory, "Assets", "Customer.dat"));

        // Bulk copy to the database
        await bulkCopier.WriteToServerAsync(stream, Encoding.UTF8, TimeSpan.FromMinutes(30));

        // Stop the application when the task is completed
        applicationLifetime.StopApplication();
    }
}