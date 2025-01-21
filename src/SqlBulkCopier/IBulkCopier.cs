using System.Text;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier;

public interface IBulkCopier : IDisposable
{
    event SqlRowsCopiedEventHandler SqlRowsCopied;

    int BatchSize { get; set; }
    string DestinationTableName { get; }
    int NotifyAfter { get; set; }
    int RowsCopied { get; }
    long RowsCopied64 { get; }
    int MaxRetryCount { get; }
    bool TruncateBeforeBulkInsert { get; }
    bool UseExponentialBackoff { get; }
    TimeSpan InitialDelay { get; }

    Task WriteToServerAsync(Stream stream, Encoding encoding, TimeSpan timeout);
}