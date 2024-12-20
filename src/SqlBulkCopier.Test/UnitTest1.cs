using System.Data;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier.Test;

public class UnitTest1
{
    [Fact]
    public async Task Test1()
    {
        using IDataReader dataReader = default!;
        using SqlBulkCopy sqlBulkCopy = new(new SqlConnection());

        sqlBulkCopy.ColumnMappings.Add(1, 2);

        await sqlBulkCopy.WriteToServerAsync(dataReader);
    }
}