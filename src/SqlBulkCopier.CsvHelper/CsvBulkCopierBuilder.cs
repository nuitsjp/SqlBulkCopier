using CsvHelper;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier.CsvHelper;

/// <summary>
/// Builder class for creating a bulk copier that processes CSV files.
/// Supports both CSV files with headers and without headers.
/// </summary>
public class CsvBulkCopierBuilder : ICsvBulkCopierNoHeaderBuilder, ICsvBulkCopierWithHeaderBuilder
{
    /// <summary>
    /// Creates a builder for CSV files with headers.
    /// </summary>
    /// <param name="destinationTableName">The name of the destination table.</param>
    /// <returns>A builder instance for method chaining.</returns>
    public static ICsvBulkCopierWithHeaderBuilder CreateWithHeader(string destinationTableName)
        => new CsvBulkCopierBuilder(destinationTableName, true);

    /// <summary>
    /// Creates a builder for CSV files without headers.
    /// </summary>
    /// <param name="destinationTableName">The name of the destination table.</param>
    /// <returns>A builder instance for method chaining.</returns>
    public static ICsvBulkCopierNoHeaderBuilder CreateNoHeader(string destinationTableName)
        => new CsvBulkCopierBuilder(destinationTableName, false);

    /// <summary>
    /// Default column context configuration.
    /// </summary>
    public Action<IColumnContext> DefaultColumnContext { get; set; } = _ => { };

    /// <summary>
    /// Row filter predicate.
    /// </summary>
    private Predicate<CsvReader> _rowFilter = _ => true;

    /// <summary>
    /// List of column contexts for mapping CSV columns to database columns.
    /// </summary>
    private readonly List<CsvColumnContext> _columnContexts = new();

    /// <summary>
    /// Gets the read-only list of column contexts.
    /// </summary>
    public IReadOnlyList<IColumnContext> ColumnContexts => _columnContexts;

    /// <summary>
    /// The name of the destination table.
    /// </summary>
    private readonly string _destinationTableName;

    /// <summary>
    /// Indicates whether the CSV file has a header.
    /// </summary>
    private readonly bool _hasHeader;

    /// <summary>
    /// Maximum number of retry attempts for failed operations.
    /// </summary>
    private int _maxRetryCount;

    /// <summary>
    /// Initial delay duration between retry attempts.
    /// </summary>
    private TimeSpan _initialDelay = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Indicates whether the destination table should be truncated before performing the bulk insert.
    /// </summary>
    private bool _truncateBeforeBulkInsert;

    /// <summary>
    /// Indicates whether exponential backoff should be used for retry delays.
    /// </summary>
    private bool _useExponentialBackoff = true;

    /// <summary>
    /// Number of rows in each batch.
    /// </summary>
    private int _batchSize;

    /// <summary>
    /// Number of rows to process before generating a notification event.
    /// </summary>
    private int _notifyAfter;

    /// <summary>
    /// Initializes a new instance of the CsvBulkCopierBuilder class.
    /// </summary>
    /// <param name="destinationTableName">The name of the destination table.</param>
    /// <param name="hasHeader">Indicates whether the CSV file has a header.</param>
    private CsvBulkCopierBuilder(string destinationTableName, bool hasHeader)
    {
        _destinationTableName = destinationTableName;
        _hasHeader = hasHeader;
    }

    /// <summary>
    /// Sets the row filter predicate.
    /// </summary>
    /// <param name="rowFilter">The row filter predicate.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public ICsvBulkCopierNoHeaderBuilder SetRowFilter(Predicate<CsvReader> rowFilter)
    {
        _rowFilter = rowFilter;
        return this;
    }

    /// <summary>
    /// Adds a column mapping using the CSV header name.
    /// </summary>
    /// <param name="columnName">The name of the column in the CSV header.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public ICsvBulkCopierWithHeaderBuilder AddColumnMapping(string columnName)
        => AddColumnMapping(columnName, _ => { });

    /// <summary>
    /// Adds a column mapping with custom configuration using the CSV header name.
    /// </summary>
    /// <param name="columnName">The name of the column in the CSV header.</param>
    /// <param name="c">An action that configures the column context for data conversion and handling.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public ICsvBulkCopierWithHeaderBuilder AddColumnMapping(string columnName, Action<IColumnContext> c)
    {
        var columnContext = new CsvColumnContext(_columnContexts.Count, columnName, _ => { });
        DefaultColumnContext(columnContext);
        c(columnContext);
        _columnContexts.Add(columnContext);
        return this;
    }

    /// <summary>
    /// Adds a column mapping using the CSV column ordinal position.
    /// </summary>
    /// <param name="dbColumnName">The name of the column in the destination database table.</param>
    /// <param name="csvColumnOrdinal">The zero-based ordinal position of the column in the CSV file.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public ICsvBulkCopierNoHeaderBuilder AddColumnMapping(string dbColumnName, int csvColumnOrdinal)
        => AddColumnMapping(dbColumnName, csvColumnOrdinal, _ => { });

    /// <summary>
    /// Adds a column mapping with custom configuration using the CSV column ordinal position.
    /// </summary>
    /// <param name="dbColumnName">The name of the column in the destination database table.</param>
    /// <param name="csvColumnOrdinal">The zero-based ordinal position of the column in the CSV file.</param>
    /// <param name="c">An action that configures the column context for data conversion and handling.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public ICsvBulkCopierNoHeaderBuilder AddColumnMapping(string dbColumnName, int csvColumnOrdinal, Action<IColumnContext> c)
    {
        var columnContext = new CsvColumnContext(csvColumnOrdinal, dbColumnName, _ => { });
        DefaultColumnContext(columnContext);
        c(columnContext);
        _columnContexts.Add(columnContext);
        return this;
    }

    /// <summary>
    /// Sets the row filter predicate.
    /// </summary>
    /// <param name="rowFilter">The row filter predicate.</param>
    /// <returns>The builder instance for method chaining.</returns>
    ICsvBulkCopierWithHeaderBuilder ICsvBulkCopierBuilder<ICsvBulkCopierWithHeaderBuilder>.SetRowFilter(Predicate<CsvReader> rowFilter)
    {
        _rowFilter = rowFilter;
        return this;
    }

    /// <summary>
    /// Sets the maximum number of retry attempts for failed operations.
    /// </summary>
    /// <param name="value">The maximum number of retry attempts.</param>
    /// <returns>The builder instance for method chaining.</returns>
    ICsvBulkCopierWithHeaderBuilder IBulkCopierBuilder<ICsvBulkCopierWithHeaderBuilder>.SetMaxRetryCount(int value)
    {
        _maxRetryCount = value;
        return this;
    }

    /// <summary>
    /// Sets the initial delay duration between retry attempts.
    /// </summary>
    /// <param name="value">The initial delay duration.</param>
    /// <returns>The builder instance for method chaining.</returns>
    ICsvBulkCopierWithHeaderBuilder IBulkCopierBuilder<ICsvBulkCopierWithHeaderBuilder>.SetInitialDelay(TimeSpan value)
    {
        _initialDelay = value;
        return this;
    }

    /// <summary>
    /// Sets whether the destination table should be truncated before performing the bulk insert.
    /// </summary>
    /// <param name="value">True to truncate the table; otherwise, false.</param>
    /// <returns>The builder instance for method chaining.</returns>
    ICsvBulkCopierWithHeaderBuilder IBulkCopierBuilder<ICsvBulkCopierWithHeaderBuilder>.SetTruncateBeforeBulkInsert(bool value)
    {
        _truncateBeforeBulkInsert = value;
        return this;
    }

    /// <summary>
    /// Sets whether exponential backoff should be used for retry delays.
    /// </summary>
    /// <param name="value">True to use exponential backoff; otherwise, false.</param>
    /// <returns>The builder instance for method chaining.</returns>
    ICsvBulkCopierWithHeaderBuilder IBulkCopierBuilder<ICsvBulkCopierWithHeaderBuilder>.SetUseExponentialBackoff(bool value)
    {
        _useExponentialBackoff = value;
        return this;
    }

    /// <summary>
    /// Sets the number of rows in each batch.
    /// </summary>
    /// <param name="value">The batch size.</param>
    /// <returns>The builder instance for method chaining.</returns>
    ICsvBulkCopierWithHeaderBuilder IBulkCopierBuilder<ICsvBulkCopierWithHeaderBuilder>.SetBatchSize(int value)
    {
        _batchSize = value;
        return this;
    }

    /// <summary>
    /// Sets the number of rows to process before generating a notification event.
    /// </summary>
    /// <param name="value">The number of rows to process before notification.</param>
    /// <returns>The builder instance for method chaining.</returns>
    ICsvBulkCopierWithHeaderBuilder IBulkCopierBuilder<ICsvBulkCopierWithHeaderBuilder>.SetNotifyAfter(int value)
    {
        _notifyAfter = value;
        return this;
    }

    /// <summary>
    /// Sets the default column context configuration.
    /// </summary>
    /// <param name="c">The action to configure the column context.</param>
    /// <returns>The builder instance for method chaining.</returns>
    ICsvBulkCopierWithHeaderBuilder IBulkCopierBuilder<ICsvBulkCopierWithHeaderBuilder>.SetDefaultColumnContext(Action<IColumnContext> c)
    {
        DefaultColumnContext = c;
        return this;
    }

    /// <summary>
    /// Sets the maximum number of retry attempts for failed operations.
    /// </summary>
    /// <param name="value">The maximum number of retry attempts.</param>
    /// <returns>The builder instance for method chaining.</returns>
    ICsvBulkCopierNoHeaderBuilder IBulkCopierBuilder<ICsvBulkCopierNoHeaderBuilder>.SetMaxRetryCount(int value)
    {
        _maxRetryCount = value;
        return this;
    }

    /// <summary>
    /// Sets the initial delay duration between retry attempts.
    /// </summary>
    /// <param name="value">The initial delay duration.</param>
    /// <returns>The builder instance for method chaining.</returns>
    ICsvBulkCopierNoHeaderBuilder IBulkCopierBuilder<ICsvBulkCopierNoHeaderBuilder>.SetInitialDelay(TimeSpan value)
    {
        _initialDelay = value;
        return this;
    }

    /// <summary>
    /// Sets whether the destination table should be truncated before performing the bulk insert.
    /// </summary>
    /// <param name="value">True to truncate the table; otherwise, false.</param>
    /// <returns>The builder instance for method chaining.</returns>
    ICsvBulkCopierNoHeaderBuilder IBulkCopierBuilder<ICsvBulkCopierNoHeaderBuilder>.SetTruncateBeforeBulkInsert(bool value)
    {
        _truncateBeforeBulkInsert = value;
        return this;
    }

    /// <summary>
    /// Sets whether exponential backoff should be used for retry delays.
    /// </summary>
    /// <param name="value">True to use exponential backoff; otherwise, false.</param>
    /// <returns>The builder instance for method chaining.</returns>
    ICsvBulkCopierNoHeaderBuilder IBulkCopierBuilder<ICsvBulkCopierNoHeaderBuilder>.SetUseExponentialBackoff(bool value)
    {
        _useExponentialBackoff = value;
        return this;
    }

    /// <summary>
    /// Sets the number of rows in each batch.
    /// </summary>
    /// <param name="value">The batch size.</param>
    /// <returns>The builder instance for method chaining.</returns>
    ICsvBulkCopierNoHeaderBuilder IBulkCopierBuilder<ICsvBulkCopierNoHeaderBuilder>.SetBatchSize(int value)
    {
        _batchSize = value;
        return this;
    }

    /// <summary>
    /// Sets the number of rows to process before generating a notification event.
    /// </summary>
    /// <param name="value">The number of rows to process before notification.</param>
    /// <returns>The builder instance for method chaining.</returns>
    ICsvBulkCopierNoHeaderBuilder IBulkCopierBuilder<ICsvBulkCopierNoHeaderBuilder>.SetNotifyAfter(int value)
    {
        _notifyAfter = value;
        return this;
    }

    /// <summary>
    /// Sets the default column context configuration.
    /// </summary>
    /// <param name="c">The action to configure the column context.</param>
    /// <returns>The builder instance for method chaining.</returns>
    ICsvBulkCopierNoHeaderBuilder IBulkCopierBuilder<ICsvBulkCopierNoHeaderBuilder>.SetDefaultColumnContext(Action<IColumnContext> c)
    {
        DefaultColumnContext = c;
        return this;
    }

    /// <summary>
    /// Builds the column mappings.
    /// </summary>
    /// <returns>An enumerable of columns.</returns>
    public IEnumerable<Column> BuildColumns()
    {
        foreach (var column in _columnContexts)
        {
            DefaultColumnContext(column);
            yield return column.Build(DefaultColumnContext);
        }
    }

    /// <summary>
    /// Builds the bulk copier using the specified SQL connection.
    /// </summary>
    /// <param name="connection">The SQL connection to use.</param>
    /// <returns>A configured bulk copier instance.</returns>
    public IBulkCopier Build(SqlConnection connection)
    {
        return new BulkCopier(
            _destinationTableName,
            new CsvDataReaderBuilder(_hasHeader, BuildColumns(), _rowFilter),
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

    /// <summary>
    /// Builds the bulk copier using the specified connection string.
    /// </summary>
    /// <param name="connectionString">The connection string to use.</param>
    /// <returns>A configured bulk copier instance.</returns>
    public IBulkCopier Build(string connectionString)
    {
        return new BulkCopier(
            _destinationTableName,
            new CsvDataReaderBuilder(_hasHeader, BuildColumns(), _rowFilter),
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

    /// <summary>
    /// Builds the bulk copier using the specified connection string and bulk copy options.
    /// </summary>
    /// <param name="connectionString">The connection string to use.</param>
    /// <param name="copyOptions">The SqlBulkCopyOptions to use.</param>
    /// <returns>A configured bulk copier instance.</returns>
    public IBulkCopier Build(string connectionString, SqlBulkCopyOptions copyOptions)
    {
        return new BulkCopier(
            _destinationTableName,
            new CsvDataReaderBuilder(_hasHeader, BuildColumns(), _rowFilter),
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

    /// <summary>
    /// Builds the bulk copier using the specified SQL connection, bulk copy options, and transaction.
    /// </summary>
    /// <param name="connection">The SQL connection to use.</param>
    /// <param name="copyOptions">The SqlBulkCopyOptions to use.</param>
    /// <param name="externalTransaction">The transaction to use.</param>
    /// <returns>A configured bulk copier instance.</returns>
    public IBulkCopier Build(SqlConnection connection, SqlBulkCopyOptions copyOptions, SqlTransaction externalTransaction)
    {
        return new BulkCopier(
            _destinationTableName,
            new CsvDataReaderBuilder(_hasHeader, BuildColumns(), _rowFilter),
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
