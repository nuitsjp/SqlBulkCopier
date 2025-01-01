using System.Text;
using Microsoft.Extensions.Hosting;
using SqlBulkCopier;
using SqlBulkCopier.Hosting;

namespace Sample.CsvHelper
{
    public class BulkCopyService(
        IBulkCopier bulkCopier, 
        SqlConnectionProvider sqlConnectionProvider,
        IHostApplicationLifetime applicationLifetime) : BackgroundService
    {
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await using var connection = await sqlConnectionProvider.OpenAsync();
            await using Stream stream = File.OpenRead(@"Assets\Customer.csv");
            await bulkCopier.WriteToServerAsync(connection, stream, Encoding.UTF8);

            // Stop the application when the task is completed
            applicationLifetime.StopApplication();
        }
    }
}
