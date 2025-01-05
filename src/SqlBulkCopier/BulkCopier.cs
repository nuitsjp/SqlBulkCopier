using System.Text;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier
{
    public record BulkCopier(

        string DestinationTableName,
        IDataReaderBuilder DataReaderBuilder) : IBulkCopier
    {
        public async Task WriteToServerAsync(SqlConnection connection, Stream stream, Encoding encoding)
        {
            using var sqlBulkCopy = new SqlBulkCopy(connection);
            sqlBulkCopy.DestinationTableName = DestinationTableName;
            DataReaderBuilder.SetupColumnMappings(sqlBulkCopy);
            sqlBulkCopy.BulkCopyTimeout = 300;
            await sqlBulkCopy.WriteToServerAsync(DataReaderBuilder.Build(stream, encoding));
        }
    }
}