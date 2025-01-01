using CsvHelper;

namespace SqlBulkCopier.CsvHelper
{
    public interface ICsvBulkCopierNoHeaderBuilder
    {
        /// <summary>
        /// Setup default column context
        /// </summary>
        /// <param name="c"></param>
        /// <returns></returns>
        ICsvBulkCopierNoHeaderBuilder SetDefaultColumnContext(Action<IColumnContext> c);
        /// <summary>
        /// Set row filter
        /// </summary>
        /// <param name="rowFilter"></param>
        /// <returns></returns>
        ICsvBulkCopierNoHeaderBuilder SetRowFilter(Predicate<CsvReader> rowFilter);

        ICsvBulkCopierNoHeaderBuilder AddColumnMapping(string dbColumnName, int csvColumnOrdinal);
        ICsvBulkCopierNoHeaderBuilder AddColumnMapping(string dbColumnName, int csvColumnOrdinal, Action<IColumnContext> c);
        IBulkCopier Build();
    }
}