using System.Globalization;
using System.Text;
using CsvHelper;
using Shouldly;
using SqlBulkCopier.CsvHelper;
using SqlBulkCopier.Test.CsvHelper.Util;
using CsvDataReader = SqlBulkCopier.CsvHelper.CsvDataReader;
using MissingFieldException = CsvHelper.MissingFieldException;

// ReSharper disable UseAwaitUsing

// ReSharper disable UnusedMember.Global

namespace SqlBulkCopier.Test.CsvHelper;

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
        reader.FieldCount.ShouldBe(3);
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
            reader.GetOrdinal("Id").ShouldBe(0);
            reader.GetOrdinal("TinyInt").ShouldBe(1);
            reader.GetOrdinal("SmallInt").ShouldBe(2);
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
            Func<object?> act = () => reader.GetOrdinal("CustomerId");

            // Assert
            act.ShouldThrow<MissingFieldException>();
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
        ((Action)(() => { _ = reader[0]; })).ShouldThrow<NotSupportedException>();
        ((Action)(() => { _ = reader["name"]; })).ShouldThrow<NotSupportedException>();
        ((Func<object?>)(() => reader.Depth)).ShouldThrow<NotSupportedException>();
        ((Func<object?>)(() => reader.IsClosed)).ShouldThrow<NotSupportedException>();
        ((Func<object?>)(() => reader.RecordsAffected)).ShouldThrow<NotSupportedException>();
        ((Action)(() => reader.GetName(0))).ShouldThrow<NotSupportedException>();
        ((Action)(() => reader.GetDataTypeName(0))).ShouldThrow<NotSupportedException>();
        ((Action)(() => reader.GetFieldType(0))).ShouldThrow<NotSupportedException>();
        ((Action)(() => reader.GetValues(new object[1]))).ShouldThrow<NotSupportedException>();
        ((Action)(() => reader.GetBoolean(0))).ShouldThrow<NotSupportedException>();
        ((Action)(() => reader.GetByte(0))).ShouldThrow<NotSupportedException>();
        ((Action)(() => reader.GetBytes(0, 0, new byte[1], 0, 1))).ShouldThrow<NotSupportedException>();
        ((Action)(() => reader.GetChar(0))).ShouldThrow<NotSupportedException>();
        ((Action)(() => reader.GetChars(0, 0, new char[1], 0, 1))).ShouldThrow<NotSupportedException>();
        ((Action)(() => reader.GetGuid(0))).ShouldThrow<NotSupportedException>();
        ((Action)(() => reader.GetInt16(0))).ShouldThrow<NotSupportedException>();
        ((Action)(() => reader.GetInt32(0))).ShouldThrow<NotSupportedException>();
        ((Action)(() => reader.GetInt64(0))).ShouldThrow<NotSupportedException>();
        ((Action)(() => reader.GetFloat(0))).ShouldThrow<NotSupportedException>();
        ((Action)(() => reader.GetDouble(0))).ShouldThrow<NotSupportedException>();
        ((Action)(() => reader.GetString(0))).ShouldThrow<NotSupportedException>();
        ((Action)(() => reader.GetDecimal(0))).ShouldThrow<NotSupportedException>();
        ((Action)(() => reader.GetDateTime(0))).ShouldThrow<NotSupportedException>();
        ((Action)(() => reader.GetData(0))).ShouldThrow<NotSupportedException>();
        ((Action)(() => reader.GetSchemaTable())).ShouldThrow<NotSupportedException>();
        ((Action)(() => reader.IsDBNull(0))).ShouldThrow<NotSupportedException>();
        ((Action)(() => reader.NextResult())).ShouldThrow<NotSupportedException>();
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