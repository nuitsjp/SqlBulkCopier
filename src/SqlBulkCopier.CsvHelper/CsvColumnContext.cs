namespace SqlBulkCopier.CsvHelper;

/// <summary>
/// Provides a context for configuring CSV column mappings and data conversions.
/// </summary>
/// <remarks>
/// This class extends ColumnContextBase to provide CSV-specific column configuration,
/// allowing for detailed control over how CSV data is mapped and converted during
/// bulk copy operations.
/// </remarks>
/// <param name="ordinal">The zero-based position of the column in the CSV file.</param>
/// <param name="name">The name of the column, used for header-based mapping.</param>
/// <param name="setColumnContext">Action to configure the column's context.</param>
public class CsvColumnContext(int ordinal, string name, Action<IColumnContext> setColumnContext) : ColumnContextBase(ordinal, name)
{
    /// <summary>
    /// Builds a CsvColumn instance with the configured settings.
    /// </summary>
    /// <param name="setDefaultContext">Action to apply default context settings before building.</param>
    /// <returns>A new CsvColumn instance with all the specified configuration.</returns>
    /// <remarks>
    /// The building process:
    /// 1. Applies the column-specific configuration
    /// 2. Creates a new CsvColumn with all current settings
    /// </remarks>
    public override Column Build(Action<IColumnContext> setDefaultContext)
    {
        setColumnContext(this);
        return new CsvColumn(
            Ordinal,
            Name,
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