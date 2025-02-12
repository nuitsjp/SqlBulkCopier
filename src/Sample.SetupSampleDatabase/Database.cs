using Dapper;
using Microsoft.Data.SqlClient;

namespace Sample.SetupSampleDatabase;

public class Database
{
    public static readonly string ConnectionString = new SqlConnectionStringBuilder
    {
        DataSource = @"(localdb)\MSSQLLocalDB",
        InitialCatalog = "SqlBulkCopier",
        IntegratedSecurity = true,
        TrustServerCertificate = true
    }.ToString();

    public static SqlConnection Open()
    {
        var connection = new SqlConnection(ConnectionString);
        connection.Open();
        return connection;
    }

    public static async Task SetupAsync(bool alterFileSize = false)
    {
        Console.WriteLine("Create database...");
        const string databaseName = "SqlBulkCopier";

        await using SqlConnection masterConnection = new(ConnectionString);
        await masterConnection.ExecuteAsync(
            // 1. Kill all connections to the database
            // 2. Drop the database if it exists
            // 3. Create the database
            // 4. Create the Customer table
            $"""
             USE master;

             DECLARE @kill varchar(8000) = '';
             DECLARE @spid int;

             SET @spid = @@SPID;

             SELECT @kill = @kill + 'kill ' + CONVERT(varchar(5), spid) + ';'
             FROM master.dbo.sysprocesses 
             WHERE DB_NAME(dbid) = '{databaseName}';

             EXEC(@kill);

             DROP DATABASE IF EXISTS [{databaseName}]

             CREATE DATABASE [{databaseName}]
             ALTER DATABASE [{databaseName}] SET RECOVERY SIMPLE;

             CREATE TABLE {databaseName}.dbo.Customer (
                 CustomerId INT PRIMARY KEY,
                 FirstName NVARCHAR(50),
                 LastName NVARCHAR(50),
                 BirthDate DATE,
                 IsActive BIT,
                 CreatedAt DATETIME DEFAULT GETDATE(),  -- デフォルト値として GetDate() を設定
                 UpdatedAt DATETIME DEFAULT GETDATE()   -- デフォルト値として GetDate() を設定
             )
             """);

        if (!alterFileSize) return;

        await masterConnection.ExecuteAsync(
            """
            -- Alter data file size
            ALTER DATABASE SqlBulkCopier 
            MODIFY FILE (
                NAME = 'SqlBulkCopier',
                SIZE = 5120MB
            );

            -- Alter log file size
            ALTER DATABASE SqlBulkCopier 
            MODIFY FILE (
                NAME = 'SqlBulkCopier_log',
                SIZE = 10240MB
            );
            """);
    }
}