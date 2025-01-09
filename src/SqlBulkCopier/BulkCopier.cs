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

    public async Task WriteToServerAsync(Stream stream, Encoding encoding, TimeSpan timeout)
    {
        _sqlBulkCopy.DestinationTableName = DestinationTableName;
        DataReaderBuilder.SetupColumnMappings(_sqlBulkCopy);
        _sqlBulkCopy.BulkCopyTimeout = (int)timeout.TotalSeconds;
        await _sqlBulkCopy.WriteToServerAsync(DataReaderBuilder.Build(stream, encoding));
    }

    void IDisposable.Dispose()
    {
        ((IDisposable)_sqlBulkCopy).Dispose();
    }
}