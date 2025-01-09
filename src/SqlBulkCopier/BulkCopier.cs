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
        DataReaderBuilder = dataReaderBuilder;
        _sqlBulkCopy = new SqlBulkCopy(connection);
        _sqlBulkCopy.DestinationTableName = destinationTableName;
        DataReaderBuilder.SetupColumnMappings(_sqlBulkCopy);
    }

    public BulkCopier(
        string destinationTableName,
        IDataReaderBuilder dataReaderBuilder,
        string connectionString)
    {
        DataReaderBuilder = dataReaderBuilder;
        _sqlBulkCopy = new SqlBulkCopy(connectionString);
        _sqlBulkCopy.DestinationTableName = destinationTableName;
        DataReaderBuilder.SetupColumnMappings(_sqlBulkCopy);
    }

    public BulkCopier(
        string destinationTableName,
        IDataReaderBuilder dataReaderBuilder, 
        string connectionString, 
        SqlBulkCopyOptions copyOptions)
    {
        DataReaderBuilder = dataReaderBuilder;
        _sqlBulkCopy = new SqlBulkCopy(connectionString, copyOptions);
        _sqlBulkCopy.DestinationTableName = destinationTableName;
        DataReaderBuilder.SetupColumnMappings(_sqlBulkCopy);
    }

    public BulkCopier(
        string destinationTableName,
        IDataReaderBuilder dataReaderBuilder, 
        SqlConnection connection, 
        SqlBulkCopyOptions copyOptions, 
        SqlTransaction externalTransaction)
    {
        DataReaderBuilder = dataReaderBuilder;
        _sqlBulkCopy = new SqlBulkCopy(connection, copyOptions, externalTransaction);
        _sqlBulkCopy.DestinationTableName = destinationTableName;
        DataReaderBuilder.SetupColumnMappings(_sqlBulkCopy);
    }

    public IDataReaderBuilder DataReaderBuilder { get; init; }

    public async Task WriteToServerAsync(Stream stream, Encoding encoding, TimeSpan timeout)
    {
        _sqlBulkCopy.BulkCopyTimeout = (int)timeout.TotalSeconds;
        await _sqlBulkCopy.WriteToServerAsync(DataReaderBuilder.Build(stream, encoding));
    }

    void IDisposable.Dispose()
    {
        ((IDisposable)_sqlBulkCopy).Dispose();
    }
}