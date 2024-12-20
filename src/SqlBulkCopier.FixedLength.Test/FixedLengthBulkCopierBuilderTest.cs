using System.Text;
using Dapper;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier.FixedLength.Test;

public class FixedLengthBulkCopierBuilderTest
{
    private static readonly string MasterConnectionString = new SqlConnectionStringBuilder()
    {
        DataSource = @".",
        InitialCatalog = "master",
        IntegratedSecurity = true,
        TrustServerCertificate = true
    }.ToString();

    private static readonly string SqlBulkCopierConnectionString = new SqlConnectionStringBuilder()
    {
        DataSource = @".",
        InitialCatalog = "SqlBulkCopier",
        IntegratedSecurity = true,
        TrustServerCertificate = true
    }.ToString();

    public FixedLengthBulkCopierBuilderTest()
    {
        using SqlConnection mainConnection = new(MasterConnectionString);
        mainConnection.Open();

        mainConnection.Execute("DROP DATABASE IF EXISTS SqlBulkCopier");
        mainConnection.Execute("CREATE DATABASE [SqlBulkCopier]");
        mainConnection.Close();

        using SqlConnection sqlConnection = new(SqlBulkCopierConnectionString);
        sqlConnection.Open();

        sqlConnection.Execute("DROP TABLE IF EXISTS dbo.Customer");
        sqlConnection.Execute(
            """
            -- テーブルの作成
            CREATE TABLE dbo.Customer (
                CustomerId INT PRIMARY KEY,
                FirstName NVARCHAR(50),
                LastName NVARCHAR(50),
                Email NVARCHAR(100),
                PhoneNumber NVARCHAR(20),
                AddressLine1 NVARCHAR(100),
                AddressLine2 NVARCHAR(100),
                City NVARCHAR(50),
                State NVARCHAR(50),
                PostalCode NVARCHAR(10),
                Country NVARCHAR(50),
                BirthDate DATE,
                Gender NVARCHAR(10),
                Occupation NVARCHAR(50),
                Income DECIMAL(18,2),
                RegistrationDate DATETIME,
                LastLogin DATETIME,
                IsActive BIT,
                Notes NVARCHAR(MAX),
                CreatedAt DATETIME DEFAULT GETDATE(),  -- デフォルト値として GetDate() を設定
                UpdatedAt DATETIME DEFAULT GETDATE()   -- デフォルト値として GetDate() を設定
            )
            """);

    }

    [Fact]
    public async Task WriteToServerAsync()
    {
        // Arrange
        await using SqlConnection sqlConnection = new(SqlBulkCopierConnectionString);
        await sqlConnection.OpenAsync();
        await sqlConnection.ExecuteAsync("delete from [SalesLT].[SalesOrderDetail2]");

        // Act
        ISqlBulkCopier sqlBulkCopier = FixedLengthBulkCopierBuilder.CreateBuilder("[SalesLT].[SalesOrderDetail2]")
            .AddColumnMapping("SalesOrderID", 0, 10)
            .AddColumnMapping("SalesOrderDetailID", 10, 10)
            .AddColumnMapping("OrderQty", 20, 5)
            .AddColumnMapping("ProductID", 25, 10)
            .AddColumnMapping("UnitPrice", 35, 15)
            .AddColumnMapping("UnitPriceDiscount", 50, 15)
            .AddColumnMapping("ModifiedDate", 85, 20)
            .Build();

        Stream stream = File.Open("SalesOrderDetail.txt", FileMode.Open);
        await sqlBulkCopier.WriteToServerAsync(sqlConnection, stream, Encoding.UTF8);
    }
}