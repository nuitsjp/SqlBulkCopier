using System.Data;
using System.Text;
using FixedLengthHelper;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier.FixedLength;

/// <summary>
/// Builder class for creating instances of FixedLengthDataReader.
/// </summary>
/// <param name="columns">The collection of FixedLengthColumn definitions.</param>
/// <param name="rowFilter">The predicate used to filter rows.</param>
public class FixedLengthDataReaderBuilder(IEnumerable<FixedLengthColumn> columns, Predicate<IFixedLengthReader> rowFilter) : IDataReaderBuilder
{
    /// <summary>
    /// Sets up the column mappings for the SqlBulkCopy operation.
    /// </summary>
    /// <param name="sqlBulkCopy">The SqlBulkCopy instance to configure mappings for.</param>
    public void SetupColumnMappings(SqlBulkCopy sqlBulkCopy)
    {
        foreach (var keyValuePair in columns)
        {
            sqlBulkCopy.ColumnMappings.Add(keyValuePair.Ordinal, keyValuePair.Name);
        }
    }

    /// <summary>
    /// Builds an IDataReader instance that can read and convert data from the specified stream.
    /// </summary>
    /// <param name="stream">The input stream containing the source data to be copied.</param>
    /// <param name="encoding">The character encoding to use when reading the stream.</param>
    /// <returns>An IDataReader instance that can read and convert the source data according to the configured mappings.</returns>
    public IDataReader Build(Stream stream, Encoding encoding)
    {
        return new FixedLengthDataReader(
            new FixedLengthReader(stream, encoding),
            columns.ToArray(),
            rowFilter);
    }
}
