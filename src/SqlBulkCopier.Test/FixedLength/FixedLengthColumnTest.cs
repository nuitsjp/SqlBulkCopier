using FluentAssertions;
using SqlBulkCopier.FixedLength;

namespace SqlBulkCopier.Test.FixedLength
{
    public class FixedLengthColumnTest : ColumnTest
    {
        protected override IColumnContext CreateColumnContext()
            => new FixedLengthColumnContext(0, "column", 1, 2);

        [Fact]
        public void Basic()
        {
            // Arrange
            var options = CreateColumnContext();

            // Act
            var column = (FixedLengthColumn)options.Build();

            // Assert
            column.Ordinal.Should().Be(0);
            column.Name.Should().Be("column");
            column.OffsetBytes.Should().Be(1);
            column.LengthBytes.Should().Be(2);

            column.Convert("Hello").Should().Be("Hello");
        }

    }
}