using Shouldly;
using SqlBulkCopier.FixedLength;

namespace SqlBulkCopier.Test.FixedLength;

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
        column.Ordinal.ShouldBe(0);
        column.Name.ShouldBe("column");
        column.OffsetBytes.ShouldBe(1);
        column.LengthBytes.ShouldBe(2);

        column.Convert("Hello").ShouldBe("Hello");
    }

}