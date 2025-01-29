namespace SqlBulkCopier.FixedLength;

/// <summary>
/// Represents the context for configuring a fixed-length column in bulk copy operations.
/// Inherits from ColumnContextBase and adds properties specific to fixed-length columns.
/// </summary>
/// <param name="ordinal">The zero-based ordinal position of the column.</param>
/// <param name="name">The name of the column.</param>
/// <param name="offsetBytes">The zero-based offset in bytes of the column in the fixed-length file.</param>
/// <param name="lengthBytes">The length in bytes of the column in the fixed-length file.</param>
public class FixedLengthColumnContext(int ordinal, string name, int offsetBytes, int lengthBytes) : ColumnContextBase(ordinal, name)
{
    /// <summary>
    /// Gets the zero-based offset in bytes of the column in the fixed-length file.
    /// </summary>
    public int OffsetBytes { get; } = offsetBytes;

    /// <summary>
    /// Gets the length in bytes of the column in the fixed-length file.
    /// </summary>
    public int LengthBytes { get; } = lengthBytes;

    /// <summary>
    /// Builds the final column configuration using the specified default context.
    /// </summary>
    /// <param name="setDefaultContext">An action that configures the default context for the column.</param>
    /// <returns>A FixedLengthColumn instance configured with all the specified settings.</returns>
    public override Column Build(Action<IColumnContext> setDefaultContext)
    {
        setDefaultContext(this);
        return new FixedLengthColumn(
            Ordinal,
            Name,
            OffsetBytes,
            LengthBytes,
            SqlDbType,
            NumberStyles,
            DateTimeStyles,
            Format,
            CultureInfo,
            TrimMode,
            TrimChars,
            IsTreatEmptyStringAsNull,
            Converter);
    }
}
