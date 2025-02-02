using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Sample.CsvHelper;
using SqlBulkCopier.CsvHelper.Hosting;

var builder = Host.CreateApplicationBuilder(args);

builder.Configuration
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json");

builder.Services
    .AddHostedService<BulkCopyService>()
    .AddSqlBulkCopier();

await builder.Build().RunAsync();