using System.Text;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier;

public interface IBulkCopier : IDisposable
{
    event SqlRowsCopiedEventHandler SqlRowsCopied;

    int MaxRetryCount { get; set; }
    int BatchSize { get; set; }
    string DestinationTableName { get; }
    int NotifyAfter { get; set; }
    int RowsCopied { get; }
    long RowsCopied64 { get; }
    Task WriteToServerAsync(Stream stream, Encoding encoding, TimeSpan timeout);
}