using System.Data;
using System.Text;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier.Test;

public class WithRetryDataReaderBuilder(IDataReaderBuilder builder, int retryCount) : IDataReaderBuilder
{
    private int _currentCount;

    public void SetupColumnMappings(SqlBulkCopy sqlBulkCopy)
    {
        builder.SetupColumnMappings(sqlBulkCopy);
    }

    public IDataReader Build(Stream stream, Encoding encoding)
    {
        if (_currentCount < retryCount)
        {
            _currentCount++;
            throw new Exception("Simulated exception");
        }

        return builder.Build(stream, encoding);
    }
}