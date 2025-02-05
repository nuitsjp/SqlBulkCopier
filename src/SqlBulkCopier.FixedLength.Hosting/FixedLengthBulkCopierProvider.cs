using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using SqlBulkCopier.Hosting;

namespace SqlBulkCopier.FixedLength.Hosting;

public class FixedLengthBulkCopierProvider(IConfiguration configuration) : IBulkCopierProvider
{
    public IBulkCopier Provide(string bulkCopierName, string connectionString) =>
        FixedLengthBulkCopierParser.Parse(configuration, bulkCopierName)
            .Build(connectionString);

    public IBulkCopier Provide(string bulkCopierName, SqlConnection connection) =>
        FixedLengthBulkCopierParser.Parse(configuration, bulkCopierName)
            .Build(connection);

    public IBulkCopier Provide(string bulkCopierName, SqlConnection connection, SqlTransaction transaction) =>
        FixedLengthBulkCopierParser.Parse(configuration, bulkCopierName)
            .Build(connection, SqlBulkCopyOptions.Default, transaction);
}