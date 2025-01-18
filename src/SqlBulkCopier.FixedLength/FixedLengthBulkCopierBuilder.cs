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

    private readonly List<FixedLengthColumn> _columns = [];
    public List<FixedLengthColumn> Columns => _columns;
    public Predicate<IFixedLengthReader> RowFilter { get; private set; } = _ => true;
    private readonly string _destinationTableName;
    private readonly BulkCopierOptions _bulkCopierOptions = new BulkCopierOptions();

    private FixedLengthBulkCopierBuilder(string destinationTableName)
    {
        _destinationTableName = destinationTableName;
    }

    public IFixedLengthBulkCopierBuilder SetOptions(Action<BulkCopierOptions> setOptions)
    {
        setOptions(_bulkCopierOptions);
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
        _columns.Add((FixedLengthColumn)columnContext.Build());
        return this;
    }
    public IBulkCopier Build(SqlConnection connection)
    {
        return new BulkCopier(
            _destinationTableName,
            new FixedLengthDataReaderBuilder(_columns, RowFilter),
            connection,
            _bulkCopierOptions);
    }

    public IBulkCopier Build(string connectionString)
    {
        return new BulkCopier(
            _destinationTableName,
            new FixedLengthDataReaderBuilder(_columns, RowFilter),
            connectionString,
            _bulkCopierOptions);
    }

    public IBulkCopier Build(string connectionString, SqlBulkCopyOptions copyOptions)
    {
        return new BulkCopier(
            _destinationTableName,
            new FixedLengthDataReaderBuilder(_columns, RowFilter),
            connectionString,
            copyOptions,
            _bulkCopierOptions);
    }

    public IBulkCopier Build(SqlConnection connection, SqlBulkCopyOptions copyOptions, SqlTransaction externalTransaction)
    {
        return new BulkCopier(
            _destinationTableName,
            new FixedLengthDataReaderBuilder(_columns, RowFilter),
            connection,
            copyOptions,
            _bulkCopierOptions,
            externalTransaction);
    }
}