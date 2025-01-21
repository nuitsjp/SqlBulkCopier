namespace SqlBulkCopier.FixedLength;

public class FixedLengthColumnContext(int ordinal, string name, int offsetBytes, int lengthBytes) : ColumnContextBase(ordinal, name)
{
    public int OffsetBytes { get; } = offsetBytes;
    public int LengthBytes { get; } = lengthBytes;

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