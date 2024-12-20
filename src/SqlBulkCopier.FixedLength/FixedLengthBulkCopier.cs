using System.Text;
using FixedLengthHelper;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier.FixedLength;

public class FixedLengthBulkCopier(
    string destinationTableName,
    FixedLengthDataReaderBuilder fixedLengthDataReaderBuilder,
    Dictionary<string, Column> columns) : ISqlBulkCopier
{
    public async Task WriteToServerAsync(SqlConnection connection, Stream stream, Encoding encoding)
    {
        using var sqlBulkCopy = new SqlBulkCopy(connection);
        sqlBulkCopy.DestinationTableName = destinationTableName;
        foreach (var column in columns)
        {
            sqlBulkCopy.ColumnMappings.Add(column.Key, column.Value.Ordinal);
        }

        await using var dataReader = fixedLengthDataReaderBuilder.Build(stream, encoding);
        await sqlBulkCopy.WriteToServerAsync(dataReader);
    }
}