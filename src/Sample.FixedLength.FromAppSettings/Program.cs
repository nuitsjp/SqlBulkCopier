using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Sample.FixedLength.FromAppSettings;
using SqlBulkCopier.FixedLength.Hosting;

var builder = Host.CreateApplicationBuilder(args);

builder.Configuration
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json");

builder.Services
    .AddHostedService<BulkCopyService>()
    .AddSqlBulkCopier();

await builder
    .Build()
    .RunAsync();