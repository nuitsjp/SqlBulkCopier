using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace SqlBulkCopier.FixedLength.Hosting;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddSqlBulkCopier(
        this IServiceCollection services, 
        string sectionName = FixedLengthBulkCopierParser.DefaultSectionName)
    {
        services.AddTransient<IBulkCopierBuilder>(provider => FixedLengthBulkCopierParser.Parse(provider.GetRequiredService<IConfiguration>()));
        return services;
    }

}