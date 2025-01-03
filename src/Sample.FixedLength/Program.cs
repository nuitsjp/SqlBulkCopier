using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Sample.FixedLength;
using SqlBulkCopier.FixedLength.Hosting;

var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddHostedService<BulkCopyService>()
    .AddSqlBulkCopier();

await builder
    .Build()
    .RunAsync();