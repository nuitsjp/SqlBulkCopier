using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace SqlBulkCopier.CsvHelper.Hosting;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddSqlBulkCopier(
        this IServiceCollection services, 
        string sectionName = CsvBulkCopierParser.DefaultSectionName)
    {
        services.AddTransient(provider => CsvBulkCopierParser.Parse(provider.GetRequiredService<IConfiguration>()));
        return services;
    }

}