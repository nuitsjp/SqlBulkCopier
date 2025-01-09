using System.Text;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier
{
    public interface IBulkCopier : IDisposable
    {
        Task WriteToServerAsync(Stream stream, Encoding encoding, TimeSpan timeout);
    }
}