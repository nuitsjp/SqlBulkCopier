using FixedLengthHelper;

namespace SqlBulkCopier.FixedLength;

public interface IFixedLengthBulkCopierBuilder : IBulkCopierBuilder
{
    IFixedLengthBulkCopierBuilder SetDefaultColumnContext(Action<IColumnContext> c);
    IFixedLengthBulkCopierBuilder SetRowFilter(Predicate<IFixedLengthReader> rowFilter);
    IFixedLengthBulkCopierBuilder AddColumnMapping(string dbColumnName, int offsetBytes, int lengthBytes);
    IFixedLengthBulkCopierBuilder AddColumnMapping(string dbColumnName, int offsetBytes, int lengthBytes, Action<IColumnContext> c);
}