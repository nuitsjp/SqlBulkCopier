using System.Text;
using FixedLengthHelper;
using FluentAssertions;
using SqlBulkCopier.FixedLength;

// ReSharper disable UnusedMember.Global

namespace SqlBulkCopier.Test.FixedLength;

public class FixedLengthDataReaderTest
{
#if NET8_0_OR_GREATER
        static FixedLengthDataReaderTest()
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        }
#endif

    private static FixedLengthColumn CreateColumn(int ordinal, string name, int offset, int length)
    {
        return new FixedLengthColumn(ordinal, name, offset, length);
    }

    private static Predicate<IFixedLengthReader> DefaultPredicate => _ => true;

    [Fact]
    public void FieldCount()
    {
        // Arrange
        var stream = new MemoryStream(""u8.ToArray());
        using var reader = new FixedLengthDataReader(
            new FixedLengthReader(stream, Encoding.UTF8),
            [
                CreateColumn(0, "CustomerId", 0, 5),
                CreateColumn(1, "Name", 5, 21),
                CreateColumn(2, "Balance", 26, 15)
            ],
            DefaultPredicate);

        // Act & Assert
        reader.FieldCount.Should().Be(3);
    }

    [Fact]
    public void Depth()
    {
        // Arrange
        var stream = new MemoryStream(""u8.ToArray());
        using var reader = new FixedLengthDataReader(new FixedLengthReader(stream, Encoding.UTF8), [], DefaultPredicate);

        // Act & Assert
        reader.Depth.Should().Be(0);
    }

    [Fact]
    public void IsClosed()
    {
        // Arrange
        var stream = new MemoryStream(""u8.ToArray());
        var reader = new FixedLengthDataReader(new FixedLengthReader(stream, Encoding.UTF8), [], DefaultPredicate);

        // Act & Assert
        reader.IsClosed.Should().BeFalse();
        reader.Dispose();
        reader.IsClosed.Should().BeTrue();
    }

    [Fact]
    public void RecordsAffected()
    {
        // Arrange
        var stream = new MemoryStream(""u8.ToArray());
        using var reader = new FixedLengthDataReader(new FixedLengthReader(stream, Encoding.UTF8), [], DefaultPredicate);

        // Act & Assert
        reader.RecordsAffected.Should().Be(0);
    }

    [Fact]
    public void This_ByOrdinal()
    {
        // Arrange
        var stream = new MemoryStream(
            """
                00554Pedro Gomez          123423006022004
                01732中村 充志        004350011052002
                00112Ramiro Politti       000000001022000
                00924Pablo Ramirez        033213024112002
                """u8.ToArray());

        // Act
        using var reader = new FixedLengthDataReader(
            new FixedLengthReader(stream, new UTF8Encoding(false)),
            [
                CreateColumn(0, "CustomerId", 0, 5),
                CreateColumn(1, "Name", 5, 21),
                CreateColumn(2, "Balance", 26, 15)
            ],
            DefaultPredicate);

        // Assert
        reader.Read().Should().BeTrue();
        reader[0].Should().Be("00554");
        reader[1].Should().Be("Pedro Gomez          ");
        reader[2].Should().Be("123423006022004");

        reader.Read().Should().BeTrue();
        reader[0].Should().Be("01732");
        reader[1].Should().Be("中村 充志        ");
        reader[2].Should().Be("004350011052002");

        reader.Read().Should().BeTrue();
        reader.Read().Should().BeTrue();
        reader.Read().Should().BeFalse();
    }

    [Fact]
    public void GetFieldType()
    {
        // Arrange
        var stream = new MemoryStream(""u8.ToArray());
        using var reader = new FixedLengthDataReader(new FixedLengthReader(stream, Encoding.UTF8), [], DefaultPredicate);

        // Act & Assert
        reader.GetFieldType(0).Should().Be(typeof(string));
    }

    public class GetValue
    {
        [Fact]
        public void Normality()
        {
            // Arrange
            var stream = new MemoryStream(
                """
                    00554Pedro Gomez          123423006022004
                    01732中村 充志        004350011052002
                    00112Ramiro Politti       000000001022000
                    00924Pablo Ramirez        033213024112002
                    """u8.ToArray());

            // Act
            using var reader = new FixedLengthDataReader(
                new FixedLengthReader(stream, new UTF8Encoding(false)),
                [
                    CreateColumn(0, "CustomerId", 0, 5),
                    CreateColumn(1, "Name", 5, 21),
                    CreateColumn(2, "Balance", 26, 15)
                ],
                DefaultPredicate);

            // Assert
            reader.Read().Should().BeTrue();
            reader.GetValue(0).Should().Be("00554");
            reader.GetValue(1).Should().Be("Pedro Gomez          ");
            reader.GetValue(2).Should().Be("123423006022004");

            reader.Read().Should().BeTrue();
            reader.GetValue(0).Should().Be("01732");
            reader.GetValue(1).Should().Be("中村 充志        ");
            reader.GetValue(2).Should().Be("004350011052002");

            reader.Read().Should().BeTrue();
            reader.Read().Should().BeTrue();
            reader.Read().Should().BeFalse();
        }

        [Fact]
        public void WithTrim()
        {
            // Arrange
            var stream = new MemoryStream(
                """
                    00554 Pedro Gomez         123423006022004
                    01732中村 充志        004350011052002
                    00112Ramiro Politti       000000001022000
                    00924Pablo Ramirez        033213024112002
                    """u8.ToArray());

            // Act
            using var reader = new FixedLengthDataReader(
                new FixedLengthReader(stream, new UTF8Encoding(false)),
                [
                    (FixedLengthColumn)new FixedLengthColumnContext(0, "CustomerId", 0, 5).TrimStart(['0', '2', '4']).Build(),
                    (FixedLengthColumn)new FixedLengthColumnContext(1, "Name", 5, 21).TrimEnd().Build(),
                    (FixedLengthColumn)new FixedLengthColumnContext(2, "Balance", 26, 15).Trim(['0', '1', '2', '4']).Build()
                ],
                DefaultPredicate);

            // Assert
            reader.Read().Should().BeTrue();
            reader.GetValue(0).Should().Be("554");
            reader.GetValue(1).Should().Be(" Pedro Gomez");
            reader.GetValue(2).Should().Be("3423006");

            reader.Read().Should().BeTrue();
            reader.GetValue(0).Should().Be("1732");
            reader.GetValue(1).Should().Be("中村 充志");
            reader.GetValue(2).Should().Be("35001105");

            reader.Read().Should().BeTrue();
            reader.Read().Should().BeTrue();
            reader.Read().Should().BeFalse();
        }

        [Fact]
        public void WhenEmpty()
        {
            // Arrange
            var stream = new MemoryStream(
                "                          123423006022004"u8.ToArray());

            // Act
            using var reader = new FixedLengthDataReader(
                new FixedLengthReader(stream, new UTF8Encoding(false)),
                [
                    (FixedLengthColumn)new FixedLengthColumnContext(0, "CustomerId", 0, 5)
                        .Trim()
                        .TreatEmptyStringAsNull()
                        .Build(),
                    (FixedLengthColumn)new FixedLengthColumnContext(1, "Name", 5, 21)
                        .Trim()
                        .Build(),
                    (FixedLengthColumn)new FixedLengthColumnContext(2, "Balance", 26, 10)
                        .Convert(_ => DBNull.Value)
                        .Build(),
                    (FixedLengthColumn)new FixedLengthColumnContext(3, "Foo", 36, 5)
                        .Convert(_ => DBNull.Value)
                        .Build()
                ],
                DefaultPredicate);

            // Assert
            reader.Read().Should().BeTrue();
            reader.GetValue(0).Should().Be(DBNull.Value);
            reader.GetValue(1).Should().Be(string.Empty);
            reader.GetValue(2).Should().Be(DBNull.Value);
            reader.GetValue(3).Should().Be(DBNull.Value);
        }

        [Fact]
        public void WhenNotExist()
        {
            // Arrange
            var stream = new MemoryStream(
                "00554Pedro Gomez          123423006022004"u8.ToArray());
            using var reader = new FixedLengthDataReader(new FixedLengthReader(stream, Encoding.UTF8), [], DefaultPredicate);

            // Act
            reader.Read().Should().BeTrue();
            // ReSharper disable once AccessToDisposedClosure
            var act = () => reader.GetValue(0);

            // Assert
            act.Should().Throw<IndexOutOfRangeException>();
        }
    }

    public class GetOrdinal
    {
        [Fact]
        public void Normality()
        {
            // Arrange
            var stream = new MemoryStream(""u8.ToArray());
            using var reader = new FixedLengthDataReader(
                new FixedLengthReader(stream, Encoding.UTF8),
                [
                    CreateColumn(0, "CustomerId", 0, 5),
                    CreateColumn(1, "Name", 5, 21),
                    CreateColumn(2, "Balance", 26, 15)
                ],
                DefaultPredicate);

            // Act & Assert
            reader.GetOrdinal("CustomerId").Should().Be(0);
            reader.GetOrdinal("Name").Should().Be(1);
            reader.GetOrdinal("Balance").Should().Be(2);
        }

        [Fact]
        public void WhenNotExist()
        {
            // Arrange
            var stream = new MemoryStream(""u8.ToArray());
            using var reader = new FixedLengthDataReader(new FixedLengthReader(stream, Encoding.UTF8), [], DefaultPredicate);

            // Act
            // ReSharper disable once AccessToDisposedClosure
            var act = () => reader.GetOrdinal("CustomerId");

            // Assert
            act.Should().Throw<IndexOutOfRangeException>();
        }
    }

    [Fact]
    public void Close()
    {
        // Arrange
        var stream = new MemoryStream(""u8.ToArray());
        var reader = new FixedLengthDataReader(new FixedLengthReader(stream, Encoding.UTF8), [], DefaultPredicate);
        // Act & Assert
        reader.IsClosed.Should().BeFalse();
        reader.Close();
        reader.IsClosed.Should().BeTrue();
        reader.Close();
    }

    [Fact]
    public void NotSupported()
    {
        // Arrange
        var stream = new MemoryStream(""u8.ToArray());
        using var reader = new FixedLengthDataReader(new FixedLengthReader(stream, Encoding.UTF8), [], DefaultPredicate);

        // Act & Assert
        // ReSharper disable AccessToDisposedClosure
        ((Action)(() => { _ = reader["name"]; })).Should().Throw<NotSupportedException>();
        ((Action)(() => reader.GetName(0))).Should().Throw<NotSupportedException>();
        ((Action)(() => reader.GetDataTypeName(0))).Should().Throw<NotSupportedException>();
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
}