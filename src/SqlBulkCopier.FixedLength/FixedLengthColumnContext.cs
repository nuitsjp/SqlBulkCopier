namespace SqlBulkCopier.FixedLength;

public class FixedLengthColumnContext(int ordinal, string name, int offsetBytes, int lengthBytes) : ColumnContextBase
{
    public override Column Build()
    {
        return new FixedLengthColumn(
            ordinal,
            name,
            offsetBytes,
            lengthBytes,
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