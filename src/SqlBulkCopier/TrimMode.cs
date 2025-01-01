namespace SqlBulkCopier
{
    /// <summary>
    /// Trim mode.
    /// </summary>
    public enum TrimMode
    {
        /// <summary>
        /// No trimming.
        /// </summary>
        None,
        /// <summary>
        /// Trims the field.
        /// </summary>
        Trim,
        /// <summary>
        /// Trims the field from the start.
        /// </summary>
        TrimStart,
        /// <summary>
        /// Trims the field from the end.
        /// </summary>
        TrimEnd
    }
}