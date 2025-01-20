using CsvHelper;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier.CsvHelper;

/// <summary>
/// CsvHelperCsvHelperBuilder
/// 
/// </summary>
public class CsvBulkCopierNoHeaderBuilder : ICsvBulkCopierNoHeaderBuilder
{
    public static ICsvBulkCopierNoHeaderBuilder Create(string destinationTableName)
        => new CsvBulkCopierNoHeaderBuilder(destinationTableName, false);


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
    private int _maxRetryCount = 0;
    private TimeSpan _initialDelay = TimeSpan.FromSeconds(1);
    private bool _truncateBeforeBulkInsert = false;
    private bool _useExponentialBackoff = true;
    private int _batchSize = 0;
    private int _notifyAfter = 0;


    /// <summary>
    /// CsvHelperCsvHelperBuilder
    /// 
    /// </summary>
    /// <param name="destinationTableName"></param>
    /// <param name="hasHeader"></param>
    private CsvBulkCopierNoHeaderBuilder(string destinationTableName, bool hasHeader)
    {
        _destinationTableName = destinationTableName;
        _hasHeader = hasHeader;
    }

    public ICsvBulkCopierNoHeaderBuilder SetInitialDelay(TimeSpan value)
    {
        _initialDelay = value;
        return this;
    }

    public ICsvBulkCopierNoHeaderBuilder SetTruncateBeforeBulkInsert(bool value)
    {
        _truncateBeforeBulkInsert = value;
        return this;
    }

    public ICsvBulkCopierNoHeaderBuilder SetUseExponentialBackoff(bool value)
    {
        _useExponentialBackoff = value;
        return this;
    }

    public ICsvBulkCopierNoHeaderBuilder SetBatchSize(int value)
    {
        _batchSize = value;
        return this;
    }

    public ICsvBulkCopierNoHeaderBuilder SetNotifyAfter(int value)
    {
        _notifyAfter = value;
        return this;
    }

    public ICsvBulkCopierNoHeaderBuilder SetMaxRetryCount(int value)
    {
        _maxRetryCount = value;
        return this;
    }

    public ICsvBulkCopierNoHeaderBuilder SetDefaultColumnContext(Action<IColumnContext> c)
    {
        DefaultColumnContext = c;
        return this;
    }

    public ICsvBulkCopierNoHeaderBuilder SetRowFilter(Predicate<CsvReader> rowFilter)
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

    public ICsvBulkCopierNoHeaderBuilder AddColumnMapping(string columnName)
    {
        return AddColumnMapping(columnName, _ => { });
    }

    public ICsvBulkCopierNoHeaderBuilder AddColumnMapping(string columnName, Action<IColumnContext> c)
    {
        var columnContext = new CsvColumnContext(_columns.Count, columnName);
        DefaultColumnContext(columnContext);
        c(columnContext);
        _columns.Add(columnContext.Build());
        return this;
    }

    public IBulkCopier Build(SqlConnection connection)
    {
        return new BulkCopier(
            _destinationTableName, 
            new CsvDataReaderBuilder(_hasHeader, _columns, _rowFilter),
            connection)
        {
            MaxRetryCount = _maxRetryCount,
            InitialDelay = _initialDelay,
            TruncateBeforeBulkInsert = _truncateBeforeBulkInsert,
            UseExponentialBackoff = _useExponentialBackoff,
            BatchSize = _batchSize,
            NotifyAfter = _notifyAfter
        };
    }

    public IBulkCopier Build(string connectionString)
    {
        return new BulkCopier(
            _destinationTableName,
            new CsvDataReaderBuilder(_hasHeader, _columns, _rowFilter),
            connectionString)
        {
            MaxRetryCount = _maxRetryCount,
            InitialDelay = _initialDelay,
            TruncateBeforeBulkInsert = _truncateBeforeBulkInsert,
            UseExponentialBackoff = _useExponentialBackoff,
            BatchSize = _batchSize,
            NotifyAfter = _notifyAfter
        };
    }

    public IBulkCopier Build(string connectionString, SqlBulkCopyOptions copyOptions)
    {
        return new BulkCopier(
            _destinationTableName,
            new CsvDataReaderBuilder(_hasHeader, _columns, _rowFilter),
            connectionString,
            copyOptions)
        {
            MaxRetryCount = _maxRetryCount,
            InitialDelay = _initialDelay,
            TruncateBeforeBulkInsert = _truncateBeforeBulkInsert,
            UseExponentialBackoff = _useExponentialBackoff,
            BatchSize = _batchSize,
            NotifyAfter = _notifyAfter
        };
    }

    public IBulkCopier Build(SqlConnection connection, SqlBulkCopyOptions copyOptions, SqlTransaction externalTransaction)
    {
        return new BulkCopier(
            _destinationTableName,
            new CsvDataReaderBuilder(_hasHeader, _columns, _rowFilter),
            connection,
            copyOptions,
            externalTransaction)
        {
            MaxRetryCount = _maxRetryCount,
            InitialDelay = _initialDelay,
            TruncateBeforeBulkInsert = _truncateBeforeBulkInsert,
            UseExponentialBackoff = _useExponentialBackoff,
            BatchSize = _batchSize,
            NotifyAfter = _notifyAfter
        };
    }
}