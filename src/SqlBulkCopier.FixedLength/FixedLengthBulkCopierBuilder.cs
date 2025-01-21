using FixedLengthHelper;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier.FixedLength;

public class FixedLengthBulkCopierBuilder : IFixedLengthBulkCopierBuilder
{
    public static IFixedLengthBulkCopierBuilder Create(string destinationTableName)
        => new FixedLengthBulkCopierBuilder(destinationTableName);


    /// <summary>
    /// Default column context
    /// </summary>
    public Action<IColumnContext> DefaultColumnContext { get; set; } = _ => { };

    private readonly List<FixedLengthColumnContext> _columns = [];
    public IReadOnlyList<IColumnContext> Columns => _columns;
    public Predicate<IFixedLengthReader> RowFilter { get; private set; } = _ => true;
    private readonly string _destinationTableName;
    private int _maxRetryCount = 0;
    private TimeSpan _initialDelay = TimeSpan.FromSeconds(1);
    private bool _truncateBeforeBulkInsert = false;
    private bool _useExponentialBackoff = true;
    private int _batchSize = 0;
    private int _notifyAfter = 0;

    private FixedLengthBulkCopierBuilder(string destinationTableName)
    {
        _destinationTableName = destinationTableName;
    }

    public IFixedLengthBulkCopierBuilder SetMaxRetryCount(int value)
    {
        _maxRetryCount = value;
        return this;
    }

    public IFixedLengthBulkCopierBuilder SetInitialDelay(TimeSpan value)
    {
        _initialDelay = value;
        return this;
    }

    public IFixedLengthBulkCopierBuilder SetTruncateBeforeBulkInsert(bool value)
    {
        _truncateBeforeBulkInsert = value;
        return this;
    }

    public IFixedLengthBulkCopierBuilder SetUseExponentialBackoff(bool value)
    {
        _useExponentialBackoff = value;
        return this;
    }

    public IFixedLengthBulkCopierBuilder SetBatchSize(int value)
    {
        _batchSize = value;
        return this;
    }

    public IFixedLengthBulkCopierBuilder SetNotifyAfter(int value)
    {
        _notifyAfter = value;
        return this;
    }

    public IFixedLengthBulkCopierBuilder SetDefaultColumnContext(Action<IColumnContext> c)
    {
        DefaultColumnContext = c;
        return this;
    }

    public IFixedLengthBulkCopierBuilder SetRowFilter(Predicate<IFixedLengthReader> rowFilter)
    {
        RowFilter = rowFilter;
        return this;
    }

    public IFixedLengthBulkCopierBuilder AddColumnMapping(string dbColumnName, int offsetBytes, int lengthBytes)
        => AddColumnMapping(dbColumnName, offsetBytes, lengthBytes, _ => { });

    public IFixedLengthBulkCopierBuilder AddColumnMapping(string dbColumnName, int offsetBytes, int lengthBytes, Action<IColumnContext> c)
    {
        var columnContext = new FixedLengthColumnContext(_columns.Count, dbColumnName, offsetBytes, lengthBytes);
        DefaultColumnContext(columnContext);
        c(columnContext);
        _columns.Add(columnContext);
        return this;
    }
    public IBulkCopier Build(SqlConnection connection)
    {
        var columns = _columns.Select(x => (FixedLengthColumn)x.Build()).ToArray();
        return new BulkCopier(
            _destinationTableName,
            new FixedLengthDataReaderBuilder(columns, RowFilter),
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
        var columns = _columns.Select(x => (FixedLengthColumn)x.Build()).ToArray();
        return new BulkCopier(
            _destinationTableName,
            new FixedLengthDataReaderBuilder(columns, RowFilter),
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
        var columns = _columns.Select(x => (FixedLengthColumn)x.Build()).ToArray();
        return new BulkCopier(
            _destinationTableName,
            new FixedLengthDataReaderBuilder(columns, RowFilter),
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
        var columns = _columns.Select(x => (FixedLengthColumn)x.Build()).ToArray();
        return new BulkCopier(
            _destinationTableName,
            new FixedLengthDataReaderBuilder(columns, RowFilter),
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