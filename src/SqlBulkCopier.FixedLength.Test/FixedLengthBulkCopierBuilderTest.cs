using System.Text;
using Dapper;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier.FixedLength.Test;

public class FixedLengthBulkCopierBuilderTest
{
    private static readonly string ConnectionString = new SqlConnectionStringBuilder()
    {
        DataSource = @".",
        InitialCatalog = "AdventureWorksLT2022",
        IntegratedSecurity = true,
        TrustServerCertificate = true
    }.ToString();
    
    [Fact]
    public async Task Test1()
    {
        var sqlBulkCopier = FixedLengthBulkCopierBuilder.CreateBuilder("[SalesLT].[SalesOrderDetail2]")
            .AddColumnMapping("SalesOrderID", 0, 10)
            .AddColumnMapping("SalesOrderDetailID", 10, 10)
            .AddColumnMapping("OrderQty", 20, 5)
            .AddColumnMapping("ProductID", 25, 10)
            .AddColumnMapping("UnitPrice", 35, 15)
            .AddColumnMapping("UnitPriceDiscount", 50, 15)
            .AddColumnMapping("ModifiedDate", 85, 20)
            .Build();

        await using SqlConnection sqlConnection = new(ConnectionString);
        await sqlConnection.OpenAsync();

        await sqlConnection.ExecuteAsync("delete from [SalesLT].[SalesOrderDetail2]");

        Stream stream = File.Open("SalesOrderDetail.txt", FileMode.Open);
        await sqlBulkCopier.WriteToServerAsync(sqlConnection, stream, Encoding.UTF8);
    }
}