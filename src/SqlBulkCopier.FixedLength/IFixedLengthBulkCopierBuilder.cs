using FixedLengthHelper;

namespace SqlBulkCopier.FixedLength;

/// <summary>
/// Defines the interface for building a bulk copier that processes fixed-length files.
/// Extends the IBulkCopierBuilder interface with additional methods specific to fixed-length files.
/// </summary>
public interface IFixedLengthBulkCopierBuilder : IBulkCopierBuilder<IFixedLengthBulkCopierBuilder>
{
    /// <summary>
    /// Sets the row filter predicate.
    /// </summary>
    /// <param name="rowFilter">The row filter predicate.</param>
    /// <returns>The builder instance for method chaining.</returns>
    IFixedLengthBulkCopierBuilder SetRowFilter(Predicate<IFixedLengthReader> rowFilter);

    /// <summary>
    /// Adds a column mapping using the specified offset and length in bytes.
    /// </summary>
    /// <param name="dbColumnName">The name of the column in the destination database table.</param>
    /// <param name="offsetBytes">The zero-based offset in bytes of the column in the fixed-length file.</param>
    /// <param name="lengthBytes">The length in bytes of the column in the fixed-length file.</param>
    /// <returns>The builder instance for method chaining.</returns>
    IFixedLengthBulkCopierBuilder AddColumnMapping(string dbColumnName, int offsetBytes, int lengthBytes);

    /// <summary>
    /// Adds a column mapping with custom configuration using the specified offset and length in bytes.
    /// </summary>
    /// <param name="dbColumnName">The name of the column in the destination database table.</param>
    /// <param name="offsetBytes">The zero-based offset in bytes of the column in the fixed-length file.</param>
    /// <param name="lengthBytes">The length in bytes of the column in the fixed-length file.</param>
    /// <param name="c">An action that configures the column context for data conversion and handling.</param>
    /// <returns>The builder instance for method chaining.</returns>
    IFixedLengthBulkCopierBuilder AddColumnMapping(string dbColumnName, int offsetBytes, int lengthBytes, Action<IColumnContext> c);
}
