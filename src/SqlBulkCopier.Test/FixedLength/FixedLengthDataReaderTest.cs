using System.Text;
using FixedLengthHelper;
using Shouldly;
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
        reader.FieldCount.ShouldBe(3);
    }

    [Fact]
    public void Depth()
    {
        // Arrange
        var stream = new MemoryStream(""u8.ToArray());
        using var reader = new FixedLengthDataReader(new FixedLengthReader(stream, Encoding.UTF8), [], DefaultPredicate);

        // Act & Assert
        reader.Depth.ShouldBe(0);
    }

    [Fact]
    public void IsClosed()
    {
        // Arrange
        var stream = new MemoryStream(""u8.ToArray());
        var reader = new FixedLengthDataReader(new FixedLengthReader(stream, Encoding.UTF8), [], DefaultPredicate);

        // Act & Assert
        reader.IsClosed.ShouldBeFalse();
        reader.Dispose();
        reader.IsClosed.ShouldBeTrue();
    }

    [Fact]
    public void RecordsAffected()
    {
        // Arrange
        var stream = new MemoryStream(""u8.ToArray());
        using var reader = new FixedLengthDataReader(new FixedLengthReader(stream, Encoding.UTF8), [], DefaultPredicate);

        // Act & Assert
        reader.RecordsAffected.ShouldBe(0);
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
        reader.Read().ShouldBeTrue();
        reader[0].ShouldBe("00554");
        reader[1].ShouldBe("Pedro Gomez          ");
        reader[2].ShouldBe("123423006022004");

        reader.Read().ShouldBeTrue();
        reader[0].ShouldBe("01732");
        reader[1].ShouldBe("中村 充志        ");
        reader[2].ShouldBe("004350011052002");

        reader.Read().ShouldBeTrue();
        reader.Read().ShouldBeTrue();
        reader.Read().ShouldBeFalse();
    }

    [Fact]
    public void GetFieldType()
    {
        // Arrange
        var stream = new MemoryStream(""u8.ToArray());
        using var reader = new FixedLengthDataReader(new FixedLengthReader(stream, Encoding.UTF8), [], DefaultPredicate);

        // Act & Assert
        reader.GetFieldType(0).ShouldBe(typeof(string));
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
            reader.Read().ShouldBeTrue();
            reader.GetValue(0).ShouldBe("00554");
            reader.GetValue(1).ShouldBe("Pedro Gomez          ");
            reader.GetValue(2).ShouldBe("123423006022004");

            reader.Read().ShouldBeTrue();
            reader.GetValue(0).ShouldBe("01732");
            reader.GetValue(1).ShouldBe("中村 充志        ");
            reader.GetValue(2).ShouldBe("004350011052002");

            reader.Read().ShouldBeTrue();
            reader.Read().ShouldBeTrue();
            reader.Read().ShouldBeFalse();
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
                    (FixedLengthColumn)new FixedLengthColumnContext(0, "CustomerId", 0, 5).TrimStart(['0', '2', '4']).Build(_ => { }),
                    (FixedLengthColumn)new FixedLengthColumnContext(1, "Name", 5, 21).TrimEnd().Build(_ => { }),
                    (FixedLengthColumn)new FixedLengthColumnContext(2, "Balance", 26, 15).Trim(['0', '1', '2', '4']).Build(_ => { })
                ],
                DefaultPredicate);

            // Assert
            reader.Read().ShouldBeTrue();
            reader.GetValue(0).ShouldBe("554");
            reader.GetValue(1).ShouldBe(" Pedro Gomez");
            reader.GetValue(2).ShouldBe("3423006");

            reader.Read().ShouldBeTrue();
            reader.GetValue(0).ShouldBe("1732");
            reader.GetValue(1).ShouldBe("中村 充志");
            reader.GetValue(2).ShouldBe("35001105");

            reader.Read().ShouldBeTrue();
            reader.Read().ShouldBeTrue();
            reader.Read().ShouldBeFalse();
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
                        .Build(_ => { }),
                    (FixedLengthColumn)new FixedLengthColumnContext(1, "Name", 5, 21)
                        .Trim()
                        .Build(_ => { }),
                    (FixedLengthColumn)new FixedLengthColumnContext(2, "Balance", 26, 10)
                        .Convert(_ => DBNull.Value)
                        .Build(_ => { }),
                    (FixedLengthColumn)new FixedLengthColumnContext(3, "Foo", 36, 5)
                        .Convert(_ => DBNull.Value)
                        .Build(_ => { })
                ],
                DefaultPredicate);

            // Assert
            reader.Read().ShouldBeTrue();
            reader.GetValue(0).ShouldBe(DBNull.Value);
            reader.GetValue(1).ShouldBe(string.Empty);
            reader.GetValue(2).ShouldBe(DBNull.Value);
            reader.GetValue(3).ShouldBe(DBNull.Value);
        }

        [Fact]
        public void WhenNotExist()
        {
            // Arrange
            var stream = new MemoryStream(
                "00554Pedro Gomez          123423006022004"u8.ToArray());
            using var reader = new FixedLengthDataReader(new FixedLengthReader(stream, Encoding.UTF8), [], DefaultPredicate);

            // Act
            reader.Read().ShouldBeTrue();
            // ReSharper disable once AccessToDisposedClosure
            var act = () => reader.GetValue(0);

            // Assert
            act.ShouldThrow<IndexOutOfRangeException>();
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
            reader.GetOrdinal("CustomerId").ShouldBe(0);
            reader.GetOrdinal("Name").ShouldBe(1);
            reader.GetOrdinal("Balance").ShouldBe(2);
        }

        [Fact]
        public void WhenNotExist()
        {
            // Arrange
            var stream = new MemoryStream(""u8.ToArray());
            using var reader = new FixedLengthDataReader(new FixedLengthReader(stream, Encoding.UTF8), [], DefaultPredicate);

            // Act
            // ReSharper disable once AccessToDisposedClosure
            Func<object?> act = () => reader.GetOrdinal("CustomerId");

            // Assert
            act.ShouldThrow<IndexOutOfRangeException>();
        }
    }

    [Fact]
    public void Close()
    {
        // Arrange
        var stream = new MemoryStream(""u8.ToArray());
        var reader = new FixedLengthDataReader(new FixedLengthReader(stream, Encoding.UTF8), [], DefaultPredicate);
        // Act & Assert
        reader.IsClosed.ShouldBeFalse();
        reader.Close();
        reader.IsClosed.ShouldBeTrue();
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
        ((Action)(() => { _ = reader["name"]; })).ShouldThrow<NotSupportedException>();
        ((Action)(() => reader.GetName(0))).ShouldThrow<NotSupportedException>();
        ((Action)(() => reader.GetDataTypeName(0))).ShouldThrow<NotSupportedException>();
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
}