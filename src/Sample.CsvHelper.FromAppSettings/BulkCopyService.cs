using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using SqlBulkCopier;

namespace Sample.CsvHelper.FromAppSettings;

/// <summary>
/// A service that uses the bulk copier to write data to the database.
/// </summary>
public class BulkCopyService(
    IConfiguration configuration,
    IBulkCopierBuilder bulkCopierBuilder, 
    IHostApplicationLifetime applicationLifetime) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Open a connection to the database
        await using var connection = new SqlConnection(configuration.GetConnectionString("DefaultConnection"));
        await connection.OpenAsync(stoppingToken);

        // Write data to the database using the bulk copier
        await using var stream = File.OpenRead(
            Path.Combine(AppContext.BaseDirectory, "Assets", "Customer.csv"));

        // Bulk copy to the database
        var bulkCopier = bulkCopierBuilder.Build(connection);
        await bulkCopier.WriteToServerAsync(stream, Encoding.UTF8, TimeSpan.FromMinutes(30));

        // Stop the application when the task is completed
        applicationLifetime.StopApplication();
    }
}