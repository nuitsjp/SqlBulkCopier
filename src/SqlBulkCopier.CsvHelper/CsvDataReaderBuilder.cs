using System.Data;
using System.Globalization;
using System.Text;
using CsvHelper;
using Microsoft.Data.SqlClient;

namespace SqlBulkCopier.CsvHelper
{
    public class CsvDataReaderBuilder(bool hasHeader, IEnumerable<Column> columns, Predicate<CsvReader> rowFilter) : IDataReaderBuilder
    {
        public bool HasHeader => hasHeader;

        public void SetupColumnMappings(SqlBulkCopy sqlBulkCopy)
        {
            foreach (var column in columns)
            {
                if (hasHeader)
                {
                    // When the CSV file has a header, we can use the column name.
                    sqlBulkCopy.ColumnMappings.Add(column.Name, column.Name);
                }
                else
                {
                    // When the CSV file does not have a header, we use the column ordinal.
                    sqlBulkCopy.ColumnMappings.Add(column.Ordinal, column.Name);

                }
            }
        }

        /// <summary>
        /// Builds a FixedLengthDataReader.
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="encoding"></param>
        /// <returns></returns>
        public IDataReader Build(Stream stream, Encoding encoding)
        {
            var csvReader = new CsvReader(new StreamReader(stream, encoding), CultureInfo.CurrentCulture);
            return new CsvDataReader(csvReader, columns, hasHeader, rowFilter);
        }
    }
}