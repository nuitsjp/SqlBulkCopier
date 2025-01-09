using FluentAssertions;
using SqlBulkCopier.CsvHelper;

namespace SqlBulkCopier.Test.CsvHelper;

public class CsvColumnTest : ColumnTest
{
    protected override IColumnContext CreateColumnContext()
        => new CsvColumnContext(0, "column");

    [Fact]
    public void Basic()
    {
        // Arrange
        var options = CreateColumnContext();

        // Act
        var column = (CsvColumn)options.Build();

        // Assert
        column.Ordinal.Should().Be(0);
        column.Name.Should().Be("column");

        column.Convert("Hello").Should().Be("Hello");
    }

}