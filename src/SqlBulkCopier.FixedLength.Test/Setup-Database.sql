USE [master]
GO

-- 既にデータベースが存在する場合は削除
DROP DATABASE IF EXISTS SqlBulkCopier
GO

-- データベースの作成
CREATE DATABASE [SqlBulkCopier]
GO

USE [SqlBulkCopier]
GO

-- 既にテーブルが存在する場合は削除
DROP TABLE IF EXISTS dbo.Customer
GO

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
GO