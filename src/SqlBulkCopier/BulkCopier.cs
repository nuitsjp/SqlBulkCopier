using System.Text;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier;

public class BulkCopier : IBulkCopier
{
    public event SqlRowsCopiedEventHandler? SqlRowsCopied;

    private readonly SqlBulkCopy _sqlBulkCopy;

    public BulkCopier(
        string destinationTableName,
        IDataReaderBuilder dataReaderBuilder,
        SqlConnection connection)
        : this(destinationTableName, new SqlBulkCopy(connection), dataReaderBuilder)
    {
    }

    public BulkCopier(
        string destinationTableName,
        IDataReaderBuilder dataReaderBuilder,
        string connectionString)
        : this(destinationTableName, new SqlBulkCopy(connectionString), dataReaderBuilder)
    {
    }

    public BulkCopier(
        string destinationTableName,
        IDataReaderBuilder dataReaderBuilder, 
        string connectionString, 
        SqlBulkCopyOptions copyOptions)
        : this(destinationTableName, new SqlBulkCopy(connectionString, copyOptions), dataReaderBuilder)
    {
    }

    public BulkCopier(
        string destinationTableName,
        IDataReaderBuilder dataReaderBuilder, 
        SqlConnection connection, 
        SqlBulkCopyOptions copyOptions, 
        SqlTransaction externalTransaction)
        : this(destinationTableName, new SqlBulkCopy(connection, copyOptions, externalTransaction), dataReaderBuilder)
    {
    }

    private BulkCopier(
        string destinationTableName,
        SqlBulkCopy sqlBulkCopy, IDataReaderBuilder dataReaderBuilder)
    {
        _sqlBulkCopy = sqlBulkCopy;
        _sqlBulkCopy.DestinationTableName = destinationTableName;
        DataReaderBuilder = dataReaderBuilder;
        DataReaderBuilder.SetupColumnMappings(_sqlBulkCopy);

        _sqlBulkCopy.SqlRowsCopied += SqlBulkCopyOnSqlRowsCopied;
    }

    public IDataReaderBuilder DataReaderBuilder { get; init; }

    public int BatchSize
    {
        get => _sqlBulkCopy.BatchSize;
        set => _sqlBulkCopy.BatchSize = value;
    }

    public string DestinationTableName => _sqlBulkCopy.DestinationTableName;

    public int NotifyAfter
    {
        get => _sqlBulkCopy.NotifyAfter;
        set => _sqlBulkCopy.NotifyAfter = value;
    }

    public int RowsCopied => _sqlBulkCopy.RowsCopied;
    public long RowsCopied64 => _sqlBulkCopy.RowsCopied64;

    public async Task WriteToServerAsync(Stream stream, Encoding encoding, TimeSpan timeout)
    {
        _sqlBulkCopy.BulkCopyTimeout = (int)timeout.TotalSeconds;
        await _sqlBulkCopy.WriteToServerAsync(DataReaderBuilder.Build(stream, encoding));
    }

    void IDisposable.Dispose()
    {
        _sqlBulkCopy.SqlRowsCopied -= SqlBulkCopyOnSqlRowsCopied;
        ((IDisposable)_sqlBulkCopy).Dispose();
    }

    private void SqlBulkCopyOnSqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
        => SqlRowsCopied?.Invoke(sender, e);

}