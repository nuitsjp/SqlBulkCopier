using System.Data;
using System.Text;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier;

/// <summary>
/// Defines a builder interface for creating data readers that can be used in bulk copy operations.
/// This interface is responsible for configuring column mappings and creating data readers from input streams.
/// </summary>
/// <remarks>
/// The data reader builder serves two main purposes:
/// 1. Setting up the column mappings between source data and destination table
/// 2. Creating an IDataReader instance that can read and convert data from the input stream
/// </remarks>
public interface IDataReaderBuilder
{
    /// <summary>
    /// Sets up the column mappings for the SqlBulkCopy operation.
    /// This method is called to configure how source columns map to destination columns.
    /// </summary>
    /// <param name="sqlBulkCopy">The SqlBulkCopy instance to configure mappings for.</param>
    /// <remarks>
    /// The implementation should use sqlBulkCopy.ColumnMappings to define how each source column
    /// maps to its corresponding destination column in the target table.
    /// </remarks>
    void SetupColumnMappings(SqlBulkCopy sqlBulkCopy);

    /// <summary>
    /// Creates an IDataReader that can read and convert data from the specified stream.
    /// </summary>
    /// <param name="stream">The input stream containing the source data to be copied.</param>
    /// <param name="encoding">The character encoding to use when reading the stream.</param>
    /// <returns>An IDataReader instance that can read and convert the source data according to the configured mappings.</returns>
    /// <remarks>
    /// The returned IDataReader should:
    /// - Handle the specified character encoding correctly
    /// - Apply any configured data type conversions
    /// - Manage the stream's lifecycle appropriately
    /// - Implement all required IDataReader members
    /// </remarks>
    IDataReader Build(Stream stream, Encoding encoding);
}