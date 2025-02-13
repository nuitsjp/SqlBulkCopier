using Shouldly;
using SqlBulkCopier.CsvHelper;

namespace SqlBulkCopier.Test.CsvHelper;

public class CsvColumnTest : ColumnTest
{
    protected override IColumnContext CreateColumnContext()
        => new CsvColumnContext(0, "column", null, _ => { });

    [Fact]
    public void Basic()
    {
        // Arrange
        var options = CreateColumnContext();

        // Act
        var column = (CsvColumn)options.Build(_ => { });

        // Assert
        column.Ordinal.ShouldBe(0);
        column.Name.ShouldBe("column");

        column.Convert("Hello").ShouldBe("Hello");
    }

}