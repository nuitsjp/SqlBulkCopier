namespace SqlBulkCopier;

/// <summary>
/// Represents how existing data is removed before executing a bulk insert.
/// </summary>
public enum TruncateMethod
{
    /// <summary>
    /// Removes data with a TRUNCATE TABLE statement.
    /// </summary>
    Truncate,

    /// <summary>
    /// Removes data with a DELETE FROM statement.
    /// </summary>
    Delete
}
