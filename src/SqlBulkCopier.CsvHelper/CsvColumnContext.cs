namespace SqlBulkCopier.CsvHelper;

public class CsvColumnContext(int ordinal, string name, Action<IColumnContext> setColumnContext) : ColumnContextBase(ordinal, name)
{
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