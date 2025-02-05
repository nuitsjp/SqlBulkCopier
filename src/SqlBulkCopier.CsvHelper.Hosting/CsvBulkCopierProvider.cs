using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using SqlBulkCopier.Hosting;

namespace SqlBulkCopier.CsvHelper.Hosting;

public class CsvBulkCopierProvider(IConfiguration configuration) : IBulkCopierProvider
{
    public IBulkCopier Provide(string bulkCopierName, string connectionString) =>
        CsvBulkCopierParser.Parse(configuration, bulkCopierName)
            .Build(connectionString);

    public IBulkCopier Provide(string bulkCopierName, SqlConnection connection) =>
        CsvBulkCopierParser.Parse(configuration, bulkCopierName)
            .Build(connection);

    public IBulkCopier Provide(string bulkCopierName, SqlConnection connection, SqlTransaction transaction) =>
        CsvBulkCopierParser.Parse(configuration, bulkCopierName)
            .Build(connection, SqlBulkCopyOptions.Default, transaction);
}