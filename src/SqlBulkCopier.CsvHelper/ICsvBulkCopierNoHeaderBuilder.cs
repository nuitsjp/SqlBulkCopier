namespace SqlBulkCopier.CsvHelper;

public interface ICsvBulkCopierNoHeaderBuilder : ICsvBulkCopierBuilder<ICsvBulkCopierNoHeaderBuilder>
{
    ICsvBulkCopierNoHeaderBuilder AddColumnMapping(string dbColumnName, int csvColumnOrdinal);
    ICsvBulkCopierNoHeaderBuilder AddColumnMapping(string dbColumnName, int csvColumnOrdinal, Action<IColumnContext> c);
}