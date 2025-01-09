using System.Data;
using System.Text;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier;

public interface IDataReaderBuilder
{
    void SetupColumnMappings(SqlBulkCopy sqlBulkCopy);
    IDataReader Build(Stream stream, Encoding encoding);
}