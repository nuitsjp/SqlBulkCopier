using System.Text;
using Microsoft.Extensions.Hosting;
using SqlBulkCopier.FixedLength;
using SqlBulkCopier.Hosting;

namespace Sample.FixedLength.FromApi;

public class FixedLengthBulkCopyService(
    SqlConnectionProvider sqlConnectionProvider,
    IHostApplicationLifetime applicationLifetime)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await using var connection = await sqlConnectionProvider.OpenAsync();

        var bulkCopier = FixedLengthBulkCopierBuilder
            .Create("[dbo].[Customer]")
            .SetTruncateBeforeBulkInsert(true)
            .SetDefaultColumnContext(c => c.TrimEnd().TreatEmptyStringAsNull())
            .AddColumnMapping("CustomerId", 0, 10)
            .AddColumnMapping("FirstName", 10, 50)
            .AddColumnMapping("LastName", 60, 50)
            .AddColumnMapping("Email", 110, 100)
            .AddColumnMapping("PhoneNumber", 210, 20)
            .AddColumnMapping("AddressLine1", 230, 100)
            .AddColumnMapping("AddressLine2", 330, 100)
            .AddColumnMapping("City", 430, 50)
            .AddColumnMapping("State", 480, 50)
            .AddColumnMapping("PostalCode", 530, 10)
            .AddColumnMapping("Country", 540, 50)
            .AddColumnMapping("BirthDate", 590, 10, c => c.AsDate("yyyy-MM-dd"))
            .AddColumnMapping("Gender", 600, 10)
            .AddColumnMapping("Occupation", 610, 50)
            .AddColumnMapping("Income", 660, 21, c => c.AsDecimal())
            .AddColumnMapping("RegistrationDate", 681, 23, c => c.AsDateTime("yyyy-MM-dd HH:mm:ss.fff"))
            .AddColumnMapping("LastLogin", 704, 23, c => c.AsDateTime("yyyy-MM-dd HH:mm:ss.fff"))
            .AddColumnMapping("IsActive", 727, 1, c => c.AsBit())
            .Build(connection);

        await using var stream = File.OpenRead(Path.Combine(AppContext.BaseDirectory, "Assets", "Customer.dat"));
        await bulkCopier.WriteToServerAsync(stream, new UTF8Encoding(false), TimeSpan.FromMinutes(30));

        Console.WriteLine("Fixed length bulk copy completed");

        applicationLifetime.StopApplication();
    }
}