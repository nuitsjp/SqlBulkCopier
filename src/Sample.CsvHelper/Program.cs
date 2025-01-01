using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Sample.CsvHelper;
using SqlBulkCopier.CsvHelper.Hosting;

await SampleDatabase.SetupAsync();

var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddHostedService<BulkCopyService>()
    .AddSqlBulkCopier();

var app = builder.Build();

await app.RunAsync();