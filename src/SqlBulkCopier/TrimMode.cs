namespace SqlBulkCopier;

/// <summary>
/// Specifies how string values should be trimmed before being processed in bulk copy operations.
/// </summary>
/// <remarks>
/// Trimming is applied to string values before any type conversion or database insertion.
/// White-space characters are removed by default, but specific characters can be specified
/// in the trimming methods.
/// </remarks>
public enum TrimMode
{
    /// <summary>
    /// No trimming is applied to the string values.
    /// The original string is used as-is.
    /// </summary>
    None,

    /// <summary>
    /// Removes specified characters from both the beginning and end of the string.
    /// If no characters are specified, white-space characters are removed.
    /// </summary>
    Trim,

    /// <summary>
    /// Removes specified characters from the beginning of the string.
    /// If no characters are specified, white-space characters are removed.
    /// </summary>
    TrimStart,

    /// <summary>
    /// Removes specified characters from the end of the string.
    /// If no characters are specified, white-space characters are removed.
    /// </summary>
    TrimEnd
}