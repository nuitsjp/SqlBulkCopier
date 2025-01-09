using System.Text;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier
{
    public record BulkCopier(
        string DestinationTableName,
        IDataReaderBuilder DataReaderBuilder) : IBulkCopier
    {
        public async Task WriteToServerAsync(SqlConnection connection, Stream stream, Encoding encoding, TimeSpan timeout)
        {
            using var sqlBulkCopy = new SqlBulkCopy(connection);
            sqlBulkCopy.DestinationTableName = DestinationTableName;
            DataReaderBuilder.SetupColumnMappings(sqlBulkCopy);
            sqlBulkCopy.BulkCopyTimeout = (int)timeout.TotalSeconds;
            await sqlBulkCopy.WriteToServerAsync(DataReaderBuilder.Build(stream, encoding));
        }

        public async Task WriteToServerAsync(SqlConnection connection, SqlTransaction transaction, Stream stream, Encoding encoding,
            TimeSpan timeout)
        {
            using var sqlBulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction);
            sqlBulkCopy.DestinationTableName = DestinationTableName;
            DataReaderBuilder.SetupColumnMappings(sqlBulkCopy);
            sqlBulkCopy.BulkCopyTimeout = (int)timeout.TotalSeconds;
            await sqlBulkCopy.WriteToServerAsync(DataReaderBuilder.Build(stream, encoding));
        }
    }
}