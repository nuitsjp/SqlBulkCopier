using System.Text;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier
{
    public interface IBulkCopier
    {
        Task WriteToServerAsync(SqlConnection connection, Stream stream, Encoding encoding, TimeSpan timeout);
        Task WriteToServerAsync(SqlConnection connection, SqlTransaction transaction, Stream stream, Encoding encoding, TimeSpan timeout);
    }
}