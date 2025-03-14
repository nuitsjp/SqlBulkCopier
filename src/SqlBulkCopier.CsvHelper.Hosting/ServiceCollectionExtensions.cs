﻿using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using SqlBulkCopier.Hosting;

namespace SqlBulkCopier.CsvHelper.Hosting;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddSqlBulkCopier(
        this IServiceCollection services, 
        string sectionName = CsvBulkCopierParser.DefaultSectionName)
    {
        services.AddTransient<IBulkCopierProvider, CsvBulkCopierProvider>();
        return services;
    }

}