﻿using CsvHelper;

namespace SqlBulkCopier.CsvHelper
{
    /// <summary>
    /// CsvHelperCsvHelperBuilder
    /// 
    /// </summary>
    public class CsvBulkCopierBuilder : ICsvBulkCopierBuilder, ICsvBulkCopierNoHeaderBuilder
    {
        public static ICsvBulkCopierBuilder Create(string destinationTableName)
            => new CsvBulkCopierBuilder(destinationTableName, true);
        public static ICsvBulkCopierNoHeaderBuilder CreateNoHeader(string destinationTableName)
            => new CsvBulkCopierBuilder(destinationTableName, false);


        /// <summary>
        /// Default column context
        /// </summary>
        public Action<IColumnContext> DefaultColumnContext { get; set; } = _ => { };

        /// <summary>
        /// Row filter
        /// </summary>
        private Predicate<CsvReader> _rowFilter = _ => true;

        private readonly List<Column> _columns = [];
        public IReadOnlyList<Column> Columns => _columns;
        private readonly string _destinationTableName;
        private readonly bool _hasHeader;

        /// <summary>
        /// CsvHelperCsvHelperBuilder
        /// 
        /// </summary>
        /// <param name="destinationTableName"></param>
        /// <param name="hasHeader"></param>
        private CsvBulkCopierBuilder(string destinationTableName, bool hasHeader)
        {
            _destinationTableName = destinationTableName;
            _hasHeader = hasHeader;
        }

        /// <summary>
        /// Setup default column context
        /// </summary>
        /// <param name="c"></param>
        /// <returns></returns>
        ICsvBulkCopierBuilder ICsvBulkCopierBuilder.SetDefaultColumnContext(Action<IColumnContext> c)
        {
            DefaultColumnContext = c;
            return this;
        }

        ICsvBulkCopierBuilder ICsvBulkCopierBuilder.SetRowFilter(Predicate<CsvReader> rowFilter)
        {
            _rowFilter = rowFilter;
            return this;
        }

        ICsvBulkCopierNoHeaderBuilder ICsvBulkCopierNoHeaderBuilder.SetDefaultColumnContext(Action<IColumnContext> c)
        {
            DefaultColumnContext = c;
            return this;
        }

        ICsvBulkCopierNoHeaderBuilder ICsvBulkCopierNoHeaderBuilder.SetRowFilter(Predicate<CsvReader> rowFilter)
        {
            _rowFilter = rowFilter;
            return this;
        }

        public ICsvBulkCopierNoHeaderBuilder AddColumnMapping(string dbColumnName, int csvColumnOrdinal)
        {
            var columnContext = new CsvColumnContext(csvColumnOrdinal, dbColumnName);
            DefaultColumnContext(columnContext);
            _columns.Add(columnContext.Build());
            return this;
        }

        public ICsvBulkCopierNoHeaderBuilder AddColumnMapping(string dbColumnName, int csvColumnOrdinal, Action<IColumnContext> c)
        {
            var columnContext = new CsvColumnContext(csvColumnOrdinal, dbColumnName);
            DefaultColumnContext(columnContext);
            c(columnContext);
            _columns.Add(columnContext.Build());
            return this;
        }

        public ICsvBulkCopierBuilder AddColumnMapping(string columnName)
        {
            return AddColumnMapping(columnName, _ => { });
        }

        public ICsvBulkCopierBuilder AddColumnMapping(string columnName, Action<IColumnContext> c)
        {
            var columnContext = new CsvColumnContext(_columns.Count, columnName);
            DefaultColumnContext(columnContext);
            c(columnContext);
            _columns.Add(columnContext.Build());
            return this;
        }

        public IBulkCopier Build()
        {
            return new BulkCopier(_destinationTableName, new CsvDataReaderBuilder(_hasHeader, _columns, _rowFilter));
        }
    }
}
