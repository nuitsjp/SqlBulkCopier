using Dapper;
using Microsoft.Data.SqlClient;
// ReSharper disable StringLiteralTypo

const string databaseName = "SqlBulkCopier";

// Create connection string for master database
var connectionString = new SqlConnectionStringBuilder
{
    DataSource = ".",
    InitialCatalog = "master",
    IntegratedSecurity = true,
    TrustServerCertificate = true
}.ToString();

await using SqlConnection masterConnection = new(connectionString);
await masterConnection.ExecuteAsync(
    // 1. Kill all connections to the database
    // 2. Drop the database if it exists
    // 3. Create the database
    // 4. Create the Customer table
    $"""
     DECLARE @kill varchar(8000) = '';
     DECLARE @spid int;
     
     SET @spid = @@SPID;
     
     SELECT @kill = @kill + 'kill ' + CONVERT(varchar(5), spid) + ';'
     FROM master.dbo.sysprocesses 
     WHERE DB_NAME(dbid) = '{databaseName}';
     
     EXEC(@kill);
     
     DROP DATABASE IF EXISTS [{databaseName}]
     
     CREATE DATABASE [{databaseName}]
     
     CREATE TABLE {databaseName}.dbo.Customer (
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


Console.WriteLine("Setup Success.");
