using System.Data;
using System.Text;
using FixedLengthHelper;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier.FixedLength
{
    /// <summary>
    /// Builder for FixedLengthDataReader.
    /// </summary>
    public class FixedLengthDataReaderBuilder(IReadOnlyList<FixedLengthColumn> columns, Predicate<IFixedLengthReader> rowFilter) : IDataReaderBuilder
    {
        public void SetupColumnMappings(SqlBulkCopy sqlBulkCopy)
        {
            foreach (var keyValuePair in columns)
            {
                sqlBulkCopy.ColumnMappings.Add(keyValuePair.Ordinal, keyValuePair.Name);
            }
        }

        /// <summary>
        /// Builds a IDataReader.
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="encoding"></param>
        /// <returns></returns>
        public IDataReader Build(Stream stream, Encoding encoding)
        {
            return new FixedLengthDataReader(
                new FixedLengthReader(stream, encoding),
                columns.ToArray(),
                rowFilter);
        }
    }
}