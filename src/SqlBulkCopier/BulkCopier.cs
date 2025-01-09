using System.Text;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier;

public class BulkCopier : IBulkCopier
{
    private readonly SqlBulkCopy _sqlBulkCopy;
    public BulkCopier(
        string destinationTableName,
        IDataReaderBuilder dataReaderBuilder,
        SqlConnection connection)
    {
        DestinationTableName = destinationTableName;
        DataReaderBuilder = dataReaderBuilder;
        _sqlBulkCopy = new SqlBulkCopy(connection);
    }

    public BulkCopier(
        string destinationTableName,
        IDataReaderBuilder dataReaderBuilder,
        string connectionString)
    {
        DestinationTableName = destinationTableName;
        DataReaderBuilder = dataReaderBuilder;
        _sqlBulkCopy = new SqlBulkCopy(connectionString);
    }

    public BulkCopier(
        string destinationTableName,
        IDataReaderBuilder dataReaderBuilder, 
        string connectionString, 
        SqlBulkCopyOptions copyOptions)
    {
        DestinationTableName = destinationTableName;
        DataReaderBuilder = dataReaderBuilder;
        _sqlBulkCopy = new SqlBulkCopy(connectionString, copyOptions);
    }

    public BulkCopier(
        string destinationTableName,
        IDataReaderBuilder dataReaderBuilder, 
        SqlConnection connection, 
        SqlBulkCopyOptions copyOptions, 
        SqlTransaction externalTransaction)
    {
        DestinationTableName = destinationTableName;
        DataReaderBuilder = dataReaderBuilder;
        _sqlBulkCopy = new SqlBulkCopy(connection, copyOptions, externalTransaction);
    }

    public string DestinationTableName { get; init; }
    public IDataReaderBuilder DataReaderBuilder { get; init; }

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

    void IDisposable.Dispose()
    {
        ((IDisposable)_sqlBulkCopy).Dispose();
    }
}