using Microsoft.Data.SqlClient;

namespace SqlBulkCopier.Hosting;

public interface IBulkCopierProvider
{
    IBulkCopier Provide(string bulkCopierName, string connectionString);
    IBulkCopier Provide(string bulkCopierName, SqlConnection connection);
    IBulkCopier Provide(string bulkCopierName, SqlConnection connection, SqlTransaction transaction);
}