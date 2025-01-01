using System.Globalization;
using System.Text;
using CsvHelper;
using FluentAssertions;
using SqlBulkCopier.CsvHelper;
using SqlBulkCopier.Test.CsvHelper.Util;
using CsvDataReader = SqlBulkCopier.CsvHelper.CsvDataReader;
using MissingFieldException = CsvHelper.MissingFieldException;

// ReSharper disable UseAwaitUsing

// ReSharper disable UnusedMember.Global

namespace SqlBulkCopier.Test.CsvHelper
{
    public class CsvDataReaderTest
    {
#if NET8_0_OR_GREATER
    static CsvDataReaderTest()
    {
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
    }

#endif

        private static async Task<CsvReader> CreateCsvAsync(IEnumerable<BulkInsertTestTarget> targets)
        {
            using var stream = new MemoryStream();
            using var writer = new StreamWriter(stream, Encoding.UTF8);

            var csvWriter = new CsvWriter(writer, CultureInfo.InvariantCulture);
            csvWriter.Context.RegisterClassMap<BulkInsertTestTargetMap>();
            await csvWriter.WriteRecordsAsync(targets, CancellationToken.None);
            await csvWriter.FlushAsync();

            // csvWriterなどを閉じると、streamも閉じられるため作り直す
            return new CsvReader(new StreamReader(new MemoryStream(stream.ToArray()), Encoding.UTF8), CultureInfo.CurrentCulture);
        }

        [Fact]
        public async Task FieldCount()
        {
            // Arrange
            using var csvReader = await CreateCsvAsync([]);
            using var reader =
                new CsvDataReader(
                    csvReader,
                    [
                        CreateColumn("Id", 0),
                        CreateColumn("TinyInt", 1),
                        CreateColumn("SmallInt", 2)]);

            // Act & Assert
            reader.FieldCount.Should().Be(3);
            reader.Close();
        }

        public class GetOrdinal
        {
            [Fact]
            public async Task Normality()
            {
                // Arrange
                using var csvReader = await CreateCsvAsync([]);
                using var reader =
                    new CsvDataReader(
                        csvReader,
                        [
                            CreateColumn("Id", 0),
                            CreateColumn("TinyInt", 1),
                            CreateColumn("SmallInt", 2)]);

                // Act & Assert
                reader.GetOrdinal("Id").Should().Be(0);
                reader.GetOrdinal("TinyInt").Should().Be(1);
                reader.GetOrdinal("SmallInt").Should().Be(2);
            }

            [Fact]
            public async Task WhenNotExist()
            {
                // Arrange
                using var csvReader = await CreateCsvAsync([]);
                using var reader =
                    new CsvDataReader(
                        csvReader,
                        []);

                // Act
                // ReSharper disable once AccessToDisposedClosure
                var act = () => reader.GetOrdinal("CustomerId");

                // Assert
                act.Should().Throw<MissingFieldException>();
            }
        }

        [Fact]
        public async Task NotSupported()
        {
            // Arrange
            using var csvReader = await CreateCsvAsync([]);
            using var reader =
                new CsvDataReader(csvReader, []);

            // Act & Assert
            // ReSharper disable AccessToDisposedClosure
            ((Action)(() => { _ = reader[0]; })).Should().Throw<NotSupportedException>();
            ((Action)(() => { _ = reader["name"]; })).Should().Throw<NotSupportedException>();
            ((Func<int>)(() => reader.Depth)).Should().Throw<NotSupportedException>();
            ((Func<bool>)(() => reader.IsClosed)).Should().Throw<NotSupportedException>();
            ((Func<int>)(() => reader.RecordsAffected)).Should().Throw<NotSupportedException>();
            ((Action)(() => reader.GetName(0))).Should().Throw<NotSupportedException>();
            ((Action)(() => reader.GetDataTypeName(0))).Should().Throw<NotSupportedException>();
            ((Action)(() => reader.GetFieldType(0))).Should().Throw<NotSupportedException>();
            ((Action)(() => reader.GetValues(new object[1]))).Should().Throw<NotSupportedException>();
            ((Action)(() => reader.GetBoolean(0))).Should().Throw<NotSupportedException>();
            ((Action)(() => reader.GetByte(0))).Should().Throw<NotSupportedException>();
            ((Action)(() => reader.GetBytes(0, 0, new byte[1], 0, 1))).Should().Throw<NotSupportedException>();
            ((Action)(() => reader.GetChar(0))).Should().Throw<NotSupportedException>();
            ((Action)(() => reader.GetChars(0, 0, new char[1], 0, 1))).Should().Throw<NotSupportedException>();
            ((Action)(() => reader.GetGuid(0))).Should().Throw<NotSupportedException>();
            ((Action)(() => reader.GetInt16(0))).Should().Throw<NotSupportedException>();
            ((Action)(() => reader.GetInt32(0))).Should().Throw<NotSupportedException>();
            ((Action)(() => reader.GetInt64(0))).Should().Throw<NotSupportedException>();
            ((Action)(() => reader.GetFloat(0))).Should().Throw<NotSupportedException>();
            ((Action)(() => reader.GetDouble(0))).Should().Throw<NotSupportedException>();
            ((Action)(() => reader.GetString(0))).Should().Throw<NotSupportedException>();
            ((Action)(() => reader.GetDecimal(0))).Should().Throw<NotSupportedException>();
            ((Action)(() => reader.GetDateTime(0))).Should().Throw<NotSupportedException>();
            ((Action)(() => reader.GetData(0))).Should().Throw<NotSupportedException>();
            ((Action)(() => reader.GetSchemaTable())).Should().Throw<NotSupportedException>();
            ((Action)(() => reader.IsDBNull(0))).Should().Throw<NotSupportedException>();
            ((Action)(() => reader.NextResult())).Should().Throw<NotSupportedException>();
            // ReSharper restore AccessToDisposedClosure
        }

        private static CsvColumn CreateColumn(string columnName, int ordinal)
        {
            return new CsvColumn(
                ordinal,
                columnName,
                null,
                NumberStyles.None,
                DateTimeStyles.None,
                null,
                null,
                TrimMode.None,
                null,
                true,
                null);
        }
    }
}