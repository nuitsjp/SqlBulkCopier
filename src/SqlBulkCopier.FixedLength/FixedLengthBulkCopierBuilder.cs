﻿using FixedLengthHelper;
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

    private readonly List<FixedLengthColumnContext> _columnsContext = [];
    public IReadOnlyList<IColumnContext> ColumnContexts => _columnsContext;
    public Predicate<IFixedLengthReader> RowFilter { get; private set; } = _ => true;
    private readonly string _destinationTableName;
    private int _maxRetryCount;
    private TimeSpan _initialDelay = TimeSpan.FromSeconds(1);
    private bool _truncateBeforeBulkInsert;
    private bool _useExponentialBackoff = true;
    private int _batchSize;
    private int _notifyAfter;

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
        var columnContext = new FixedLengthColumnContext(_columnsContext.Count, dbColumnName, offsetBytes, lengthBytes);
        DefaultColumnContext(columnContext);
        c(columnContext);
        _columnsContext.Add(columnContext);
        return this;
    }

    public IEnumerable<Column> BuildColumns()
    {
        return _columnsContext.Select(x => x.Build(DefaultColumnContext));
    }

    public IBulkCopier Build(SqlConnection connection)
    {
        return new BulkCopier(
            _destinationTableName,
            new FixedLengthDataReaderBuilder(BuildColumns().Cast<FixedLengthColumn>(), RowFilter),
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
            new FixedLengthDataReaderBuilder(BuildColumns().Cast<FixedLengthColumn>(), RowFilter),
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
            new FixedLengthDataReaderBuilder(BuildColumns().Cast<FixedLengthColumn>(), RowFilter),
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
            new FixedLengthDataReaderBuilder(BuildColumns().Cast<FixedLengthColumn>(), RowFilter),
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