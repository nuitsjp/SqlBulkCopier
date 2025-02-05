using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using SqlBulkCopier.Hosting;

namespace SqlBulkCopier.FixedLength.Hosting;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddSqlBulkCopier(
        this IServiceCollection services, 
        string sectionName = FixedLengthBulkCopierParser.DefaultSectionName)
    {
        services.AddTransient<IBulkCopierProvider, FixedLengthBulkCopierProvider>();
        return services;
    }

}