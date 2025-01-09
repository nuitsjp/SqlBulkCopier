using Microsoft.Data.SqlClient;

namespace SqlBulkCopier;

public interface IBulkCopierBuilder
{
    IBulkCopier Build(SqlConnection connection);
    IBulkCopier Build(string connectionString);
    IBulkCopier Build(string connectionString, SqlBulkCopyOptions copyOptions);
    IBulkCopier Build(SqlConnection connection, SqlBulkCopyOptions copyOptions, SqlTransaction externalTransaction);
}