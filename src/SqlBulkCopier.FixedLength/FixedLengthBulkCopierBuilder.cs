using FixedLengthHelper;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier.FixedLength;

/// <summary>
/// Builder class for creating a bulk copier that processes fixed-length files.
/// Supports configuration of column mappings, retry logic, and other bulk copy options.
/// </summary>
public class FixedLengthBulkCopierBuilder : IFixedLengthBulkCopierBuilder
{
    /// <summary>
    /// Creates a builder for fixed-length files.
    /// </summary>
    /// <param name="destinationTableName">The name of the destination table.</param>
    /// <returns>A builder instance for method chaining.</returns>
    public static IFixedLengthBulkCopierBuilder Create(string destinationTableName)
        => new FixedLengthBulkCopierBuilder(destinationTableName);

    /// <summary>
    /// Default column context configuration.
    /// </summary>
    public Action<IColumnContext> DefaultColumnContext { get; set; } = _ => { };

    /// <summary>
    /// List of column contexts for mapping fixed-length columns to database columns.
    /// </summary>
    private readonly List<FixedLengthColumnContext> _columnsContext = [];

    /// <summary>
    /// Gets the read-only list of column contexts.
    /// </summary>
    public IReadOnlyList<IColumnContext> ColumnContexts => _columnsContext;

    /// <summary>
    /// 
    /// Row filter predicate.
    /// </summary>
    public Predicate<IFixedLengthReader> RowFilter { get; private set; } = _ => true;

    /// <summary>
    /// The name of the destination table.
    /// </summary>
    private readonly string _destinationTableName;

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
    /// Initializes a new instance of the FixedLengthBulkCopierBuilder class.
    /// </summary>
    /// <param name="destinationTableName">The name of the destination table.</param>
    private FixedLengthBulkCopierBuilder(string destinationTableName)
    {
        _destinationTableName = destinationTableName;
    }

    /// <summary>
    /// Sets the maximum number of retry attempts for failed operations.
    /// </summary>
    /// <param name="value">The maximum number of retry attempts.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public IFixedLengthBulkCopierBuilder SetMaxRetryCount(int value)
    {
        _maxRetryCount = value;
        return this;
    }

    /// <summary>
    /// Sets the initial delay duration between retry attempts.
    /// </summary>
    /// <param name="value">The initial delay duration.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public IFixedLengthBulkCopierBuilder SetInitialDelay(TimeSpan value)
    {
        _initialDelay = value;
        return this;
    }

    /// <summary>
    /// Sets whether the destination table should be truncated before performing the bulk insert.
    /// </summary>
    /// <param name="value">True to truncate the table; otherwise, false.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public IFixedLengthBulkCopierBuilder SetTruncateBeforeBulkInsert(bool value)
    {
        _truncateBeforeBulkInsert = value;
        return this;
    }

    /// <summary>
    /// Sets whether exponential backoff should be used for retry delays.
    /// </summary>
    /// <param name="value">True to use exponential backoff; otherwise, false.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public IFixedLengthBulkCopierBuilder SetUseExponentialBackoff(bool value)
    {
        _useExponentialBackoff = value;
        return this;
    }

    /// <summary>
    /// Sets the number of rows in each batch.
    /// </summary>
    /// <param name="value">The batch size.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public IFixedLengthBulkCopierBuilder SetBatchSize(int value)
    {
        _batchSize = value;
        return this;
    }

    /// <summary>
    /// Sets the number of rows to process before generating a notification event.
    /// </summary>
    /// <param name="value">The number of rows to process before notification.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public IFixedLengthBulkCopierBuilder SetNotifyAfter(int value)
    {
        _notifyAfter = value;
        return this;
    }

    /// <summary>
    /// Sets the default column context configuration.
    /// </summary>
    /// <param name="c">The action to configure the column context.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public IFixedLengthBulkCopierBuilder SetDefaultColumnContext(Action<IColumnContext> c)
    {
        DefaultColumnContext = c;
        return this;
    }

    /// <summary>
    /// Sets the row filter predicate.
    /// </summary>
    /// <param name="rowFilter">The row filter predicate.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public IFixedLengthBulkCopierBuilder SetRowFilter(Predicate<IFixedLengthReader> rowFilter)
    {
        RowFilter = rowFilter;
        return this;
    }

    /// <summary>
    /// Adds a column mapping using the specified offset and length in bytes.
    /// </summary>
    /// <param name="dbColumnName">The name of the column in the destination database table.</param>
    /// <param name="offsetBytes">The zero-based offset in bytes of the column in the fixed-length file.</param>
    /// <param name="lengthBytes">The length in bytes of the column in the fixed-length file.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public IFixedLengthBulkCopierBuilder AddColumnMapping(string dbColumnName, int offsetBytes, int lengthBytes)
        => AddColumnMapping(dbColumnName, offsetBytes, lengthBytes, _ => { });

    /// <summary>
    /// Adds a column mapping with custom configuration using the specified offset and length in bytes.
    /// </summary>
    /// <param name="dbColumnName">The name of the column in the destination database table.</param>
    /// <param name="offsetBytes">The zero-based offset in bytes of the column in the fixed-length file.</param>
    /// <param name="lengthBytes">The length in bytes of the column in the fixed-length file.</param>
    /// <param name="c">An action that configures the column context for data conversion and handling.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public IFixedLengthBulkCopierBuilder AddColumnMapping(string dbColumnName, int offsetBytes, int lengthBytes, Action<IColumnContext> c)
    {
        var columnContext = new FixedLengthColumnContext(_columnsContext.Count, dbColumnName, offsetBytes, lengthBytes);
        DefaultColumnContext(columnContext);
        c(columnContext);
        _columnsContext.Add(columnContext);
        return this;
    }

    /// <summary>
    /// Builds the column mappings.
    /// </summary>
    /// <returns>An enumerable of columns.</returns>
    public IEnumerable<Column> BuildColumns()
    {
        return _columnsContext.Select(x => x.Build(DefaultColumnContext));
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

    /// <summary>
    /// Builds the bulk copier using the specified connection string.
    /// </summary>
    /// <param name="connectionString">The connection string to use.</param>
    /// <returns>A configured bulk copier instance.</returns>
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
