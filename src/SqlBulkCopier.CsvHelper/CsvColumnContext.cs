namespace SqlBulkCopier.CsvHelper;

public class CsvColumnContext(int ordinal, string name) : ColumnContextBase
{
    public override Column Build()
    {
        return new CsvColumn(
            ordinal,
            name,
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