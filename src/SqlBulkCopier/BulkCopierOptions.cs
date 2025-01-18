namespace SqlBulkCopier;

public class BulkCopierOptions
{
    public int MaxRetryCount { get; set; } = 0;
    public TimeSpan InitialDelay { get; set; } = TimeSpan.FromSeconds(2);
    public bool TruncateBeforeBulkInsert { get; set; } = false;
    public bool UseExponentialBackoff { get; set; } = true;
    public int BatchSize { get; set; }
    public int NotifyAfter { get; set; }
}