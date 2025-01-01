using FixedLengthHelper;

namespace SqlBulkCopier.FixedLength
{
    public class FixedLengthBulkCopierBuilder : IFixedLengthBulkCopierBuilder
    {
        public static IFixedLengthBulkCopierBuilder Create(string destinationTableName)
            => new FixedLengthBulkCopierBuilder(destinationTableName);


        /// <summary>
        /// Default column context
        /// </summary>
        public Action<IColumnContext> DefaultColumnContext { get; set; } = _ => { };

        private readonly List<FixedLengthColumn> _columns = [];
        public List<FixedLengthColumn> Columns => _columns;
        private Predicate<IFixedLengthReader> _rowFilter = _ => true;
        private readonly string _destinationTableName;

        private FixedLengthBulkCopierBuilder(string destinationTableName)
        {
            _destinationTableName = destinationTableName;
        }

        public IFixedLengthBulkCopierBuilder SetDefaultColumnContext(Action<IColumnContext> c)
        {
            DefaultColumnContext = c;
            return this;
        }

        public IFixedLengthBulkCopierBuilder SetRowFilter(Predicate<IFixedLengthReader> rowFilter)
        {
            _rowFilter = rowFilter;
            return this;
        }

        public IFixedLengthBulkCopierBuilder AddColumnMapping(string dbColumnName, int offsetBytes, int lengthBytes)
            => AddColumnMapping(dbColumnName, offsetBytes, lengthBytes, _ => { });

        public IFixedLengthBulkCopierBuilder AddColumnMapping(string dbColumnName, int offsetBytes, int lengthBytes, Action<IColumnContext> c)
        {
            var columnContext = new FixedLengthColumnContext(_columns.Count, dbColumnName, offsetBytes, lengthBytes);
            DefaultColumnContext(columnContext);
            c(columnContext);
            _columns.Add((FixedLengthColumn)columnContext.Build());
            return this;
        }
        public IBulkCopier Build()
        {
            return new BulkCopier(
                _destinationTableName,
                new FixedLengthDataReaderBuilder(_columns, _rowFilter));
        }
    }
}
