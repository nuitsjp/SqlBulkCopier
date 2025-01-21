using System.Globalization;
using Shouldly;

// ReSharper disable UnusedMember.Global

namespace SqlBulkCopier.Test;

public abstract class ColumnTest
{
    protected abstract IColumnContext CreateColumnContext();

    [Fact]
    public void Trim()
    {
        // Arrange
        var options = CreateColumnContext()
            .Trim();
        var column = options.Build(_ => { });

        // Act
        var actual = column.Convert("  Hello  ");

        // Assert
        actual.ShouldBe("Hello");
    }

    [Fact]
    public void TrimWithChars()
    {
        // Arrange
        var options = CreateColumnContext()
            .Trim("012".ToCharArray());
        var column = options.Build(_ => { });

        // Act
        var actual = column.Convert("012Hello012");

        // Assert
        actual.ShouldBe("Hello");
    }

    [Fact]
    public void TrimStart()
    {
        // Arrange
        var options = CreateColumnContext()
            .TrimStart();
        var column = options.Build(_ => { });

        // Act
        var actual = column.Convert("  Hello  ");

        // Assert
        actual.ShouldBe("Hello  ");
    }

    [Fact]
    public void TrimStartWithChars()
    {
        // Arrange
        var options = CreateColumnContext()
            .TrimStart("012".ToCharArray());
        var column = options.Build(_ => { });
        // Act
        var actual = column.Convert("012Hello012");

        // Assert
        actual.ShouldBe("Hello012");
    }

    [Fact]
    public void TrimEnd()
    {
        // Arrange
        var options = CreateColumnContext()
            .TrimEnd();
        var column = options.Build(_ => { });
        // Act
        var actual = column.Convert("  Hello  ");
        // Assert
        actual.ShouldBe("  Hello");
    }

    [Fact]
    public void TrimEndWithChars()
    {
        // Arrange
        var options = CreateColumnContext()
            .TrimEnd("012".ToCharArray());
        var column = options.Build(_ => { });
        // Act
        var actual = column.Convert("012Hello012");
        // Assert
        actual.ShouldBe("012Hello");
    }

    [Fact]
    public void TreatEmptyStringAsNull()
    {
        // Arrange
        var options = CreateColumnContext()
            .TreatEmptyStringAsNull();
        var column = options.Build(_ => { });

        // Act
        var actual = column.Convert(string.Empty);

        // Assert
        actual.ShouldBe(DBNull.Value);
    }

    [Fact]
    public void AsBigInt()
    {
        // Arrange
        var column = CreateColumnContext()
            .AsBigInt()
            .Build(_ => { });
        // Act
        var actual = column.Convert(long.MaxValue.ToString());
        // Assert
        actual.ShouldBe(long.MaxValue);
    }

    [Fact]
    public void AsBigInt_WithParameters()
    {
        // Arrange
        var cultureInfo = CultureInfo.GetCultureInfo("de-DE");
        var column = CreateColumnContext()
            .AsBigInt(NumberStyles.AllowThousands, cultureInfo)
            .Build(_ => { });
        // Act
        var actual = column.Convert("1.234.567");
        // Assert
        actual.ShouldBe((long)1234567);
    }

    [Fact]
    public void AsBinary()
    {
        // Arrange
        var options = CreateColumnContext()
            .AsBinary();
        var column = options.Build(_ => { });
        // Act
        var actual = column.Convert("6qj7n+AIUvDxNw==");
        // Assert
        actual.ShouldBeEquivalentTo(System.Convert.FromBase64String("6qj7n+AIUvDxNw=="));
    }

    [Fact]
    public void AsBit()
    {
        // Arrange
        var options = CreateColumnContext().AsBit();
        var column = options.Build(_ => { });

        // Act & Assert
        column.Convert("1").ShouldBe(true);
        column.Convert("0").ShouldBe(false);
        column.Convert("true").ShouldBe(true);
        column.Convert("false").ShouldBe(false);
        column.Convert("True").ShouldBe(true);
        column.Convert("False").ShouldBe(false);
        column.Convert("TRUE").ShouldBe(true);
        column.Convert("FALSE").ShouldBe(false);
        column.Convert("").ShouldBe(DBNull.Value);
        var act = () => column.Convert("Hello");
        act.ShouldThrow<InvalidCastException>();
    }


    [Fact]
    public void AsDateTime_WithFormat()
    {
        // Arrange
        var options = CreateColumnContext()
            .AsDateTime("yyyyMMdd", CultureInfo.InvariantCulture);
        var column = options.Build(_ => { });
        // Act
        var actual = column.Convert("20210101");
        // Assert
        actual.ShouldBe(new DateTime(2021, 1, 1));
    }

    [Fact]
    public void AsDateTime_NotWithFormat()
    {
        // Arrange
        var options = CreateColumnContext()
            .AsDateTime();
        var column = options.Build(_ => { });
        // Act
        var actual = column.Convert("2021-01-01");
        // Assert
        actual.ShouldBe(new DateTime(2021, 1, 1));
    }

    [Fact]
    public void AsDecimal()
    {
        // Arrange
        const string numberString = "1234567.89";
        const decimal number = 1234567.89m;
        var column = CreateColumnContext()
            .AsDecimal()
            .Build(_ => { });
        // Act
        var actual = column.Convert(numberString);
        // Assert
        actual.ShouldBe(number);
    }

    [Fact]
    public void AsDecimal_WithParameters()
    {
        // Arrange
        const string numberString = "1.234.567,89";
        const decimal number = 1234567.89m;
        const NumberStyles numberStyle = NumberStyles.AllowThousands | NumberStyles.AllowDecimalPoint;
        var cultureInfo = CultureInfo.GetCultureInfo("de-DE");
        var column = CreateColumnContext()
            .AsDecimal(numberStyle, cultureInfo)
            .Build(_ => { });

        // Act
        var actual = column.Convert(numberString);

        // Assert
        actual.ShouldBe(number);
    }
    
    [Fact]
    public void AsFloat()
    {
        // Arrange
        const string numberString = "1234567.89";
        const float expectedNumber = 1234567.89f;
        var column = CreateColumnContext()
            .AsFloat()
            .Build(_ => { });
        
        // Act
        var actual = (float)column.Convert(numberString);
        
        // Assert
        actual.ShouldBe(expectedNumber, 0.0001f);
    }

    [Fact]
    public void AsFloat_WithParameters()
    {
        // Arrange
        const string numberString = "1.234.567,89";
        const float expectedNumber = 1234567.89f;
        const NumberStyles numberStyle = NumberStyles.AllowThousands | NumberStyles.AllowDecimalPoint;
        var cultureInfo = CultureInfo.GetCultureInfo("de-DE");

        var column = CreateColumnContext()
            .AsFloat(numberStyle, cultureInfo)
            .Build(_ => { });

        // Act
        var actual = (float)column.Convert(numberString);

        // Assert
        actual.ShouldBe(expectedNumber, 0.0001f);
    }
    
    [Fact]
    public void AsInt()
    {
        // Arrange
        const string numberString = "1234567";
        const int expectedNumber = 1234567;
        var column = CreateColumnContext()
            .AsInt()
            .Build(_ => { });
        
        // Act
        var actual = column.Convert(numberString);
        
        // Assert
        actual.ShouldBe(expectedNumber);
    }

    [Fact]
    public void AsInt_WithParameters()
    {
        // Arrange
        const string numberString = "1.234.567";
        const int expectedNumber = 1234567;
        const NumberStyles numberStyle = NumberStyles.AllowThousands;
        var cultureInfo = CultureInfo.GetCultureInfo("de-DE");

        var column = CreateColumnContext()
            .AsInt(numberStyle, cultureInfo)
            .Build(_ => { });

        // Act
        var actual = column.Convert(numberString);

        // Assert
        actual.ShouldBe(expectedNumber);
    }
    
    [Fact]
    public void AsMoney()
    {
        // Arrange
        const string numberString = "1234567.89";
        const decimal expectedNumber = 1234567.89m;
        var column = CreateColumnContext()
            .AsMoney()
            .Build(_ => { });
        
        // Act
        var actual = column.Convert(numberString);
        
        // Assert
        actual.ShouldBe(expectedNumber);
    }

    [Fact]
    public void AsMoney_WithParameters()
    {
        // Arrange
        const string numberString = "1.234.567,89";
        const decimal expectedNumber = 1234567.89m;
        const NumberStyles numberStyle = NumberStyles.Currency | NumberStyles.AllowThousands | NumberStyles.AllowDecimalPoint;
        var cultureInfo = CultureInfo.GetCultureInfo("de-DE");

        var column = CreateColumnContext()
            .AsMoney(numberStyle, cultureInfo)
            .Build(_ => { });

        // Act
        var actual = column.Convert(numberString);

        // Assert
        actual.ShouldBe(expectedNumber);
    }
    
    [Fact]
    public void AsReal()
    {
        // Arrange
        const string numberString = "1234567.89";
        const double expectedNumber = 1234567.89;
        var column = CreateColumnContext()
            .AsReal()
            .Build(_ => { });
        
        // Act
        var actual = (double)column.Convert(numberString);
        
        // Assert
        actual.ShouldBe(expectedNumber, 0.0001);
    }

    [Fact]
    public void AsReal_WithParameters()
    {
        // Arrange
        const string numberString = "1.234.567,89";
        const double expectedNumber = 1234567.89;
        const NumberStyles numberStyle = NumberStyles.Float | NumberStyles.AllowThousands | NumberStyles.AllowDecimalPoint;
        var cultureInfo = CultureInfo.GetCultureInfo("de-DE");

        var column = CreateColumnContext()
            .AsReal(numberStyle, cultureInfo)
            .Build(_ => { });

        // Act
        var actual = (double)column.Convert(numberString);

        // Assert
        actual.ShouldBe(expectedNumber, 0.0001);
    }

    [Fact]
    public void AsUniqueIdentifier()
    {
        // Arrange
        var options = CreateColumnContext()
            .AsUniqueIdentifier();
        var column = options.Build(_ => { });
        // Act
        var actual = column.Convert("12345678-1234-1234-1234-123456789012");
        // Assert
        actual.ShouldBe(new Guid("12345678-1234-1234-1234-123456789012"));
    }

    [Fact]
    public void AsSmallDateTime_WithFormat()
    {
        // Arrange
        var options = CreateColumnContext()
            .AsSmallDateTime("yyyyMMdd", CultureInfo.InvariantCulture);
        var column = options.Build(_ => { });
        // Act
        var actual = column.Convert("20210101");
        // Assert
        actual.ShouldBe(new DateTime(2021, 1, 1));
    }

    [Fact]
    public void AsSmallDateTime_NotWithFormat()
    {
        // Arrange
        var options = CreateColumnContext()
            .AsSmallDateTime();
        var column = options.Build(_ => { });
        // Act
        var actual = column.Convert("2021-01-01");
        // Assert
        actual.ShouldBe(new DateTime(2021, 1, 1));
    }
    
    [Fact]
    public void AsSmallInt()
    {
        // Arrange
        const string numberString = "12345";
        const short expectedNumber = 12345;
        var column = CreateColumnContext()
            .AsSmallInt()
            .Build(_ => { });
        
        // Act
        var actual = column.Convert(numberString);
        
        // Assert
        actual.ShouldBe(expectedNumber);
    }

    [Fact]
    public void AsSmallInt_WithParameters()
    {
        // Arrange
        const string numberString = "12.345";
        const short expectedNumber = 12345;
        const NumberStyles numberStyle = NumberStyles.AllowThousands;
        var cultureInfo = CultureInfo.GetCultureInfo("de-DE");

        var column = CreateColumnContext()
            .AsSmallInt(numberStyle, cultureInfo)
            .Build(_ => { });

        // Act
        var actual = column.Convert(numberString);

        // Assert
        actual.ShouldBe(expectedNumber);
    }

    [Fact]
    public void AsSmallMoney()
    {
        // Arrange
        const string numberString = "214748.3647";
        const decimal expectedNumber = 214748.3647m;
        var column = CreateColumnContext()
            .AsSmallMoney()
            .Build(_ => { });
        
        // Act
        var actual = column.Convert(numberString);
        
        // Assert
        actual.ShouldBe(expectedNumber);
    }

    [Fact]
    public void AsSmallMoney_WithParameters()
    {
        // Arrange
        const string numberString = "214.748,3647";
        const decimal expectedNumber = 214748.3647m;
        const NumberStyles numberStyle = NumberStyles.Currency | NumberStyles.AllowThousands | NumberStyles.AllowDecimalPoint;
        var cultureInfo = CultureInfo.GetCultureInfo("de-DE");

        var column = CreateColumnContext()
            .AsSmallMoney(numberStyle, cultureInfo)
            .Build(_ => { });

        // Act
        var actual = column.Convert(numberString);

        // Assert
        actual.ShouldBe(expectedNumber);
    }
    
    [Fact]
    public void AsTimestamp()
    {
        // Arrange
        const string timestampString = "2024-12-27 01:52:13";
        var expectedDateTime = new DateTime(2024, 12, 27, 1, 52, 13, DateTimeKind.Utc);
        var column = CreateColumnContext()
            .AsTimestamp()
            .Build(_ => { });
        
        // Act
        var actual = column.Convert(timestampString);
        
        // Assert
        actual.ShouldBe(expectedDateTime);
    }

    [Fact]
    public void AsTimestamp_WithParameters()
    {
        // Arrange
        const string timestampString = "27.12.2024 01:52:13";
        var expectedDateTime = new DateTime(2024, 12, 27, 1, 52, 13, DateTimeKind.Utc);
        const DateTimeStyles dateTimeStyle = DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal;
        var cultureInfo = CultureInfo.GetCultureInfo("de-DE");

        var column = CreateColumnContext()
            .AsTimestamp(cultureInfo: cultureInfo, style: dateTimeStyle)
            .Build(_ => { });

        // Act
        var actual = column.Convert(timestampString);

        // Assert
        actual.ShouldBe(expectedDateTime);
    }
    
    [Fact]
    public void AsTinyInt()
    {
        // Arrange
        const string numberString = "255";
        const byte expectedNumber = 255;
        var column = CreateColumnContext()
            .AsTinyInt()
            .Build(_ => { });
        
        // Act
        var actual = column.Convert(numberString);
        
        // Assert
        actual.ShouldBe(expectedNumber);
    }

    [Fact]
    public void AsTinyInt_WithParameters()
    {
        // Arrange
        const string numberString = "123";
        const byte expectedNumber = 123;
        const NumberStyles numberStyle = NumberStyles.Integer;
        var cultureInfo = CultureInfo.GetCultureInfo("de-DE");

        var column = CreateColumnContext()
            .AsTinyInt(numberStyle, cultureInfo)
            .Build(_ => { });

        // Act
        var actual = column.Convert(numberString);

        // Assert
        actual.ShouldBe(expectedNumber);
    }

    [Fact]
    public void AsVarBinary()
    {
        // Arrange
        var options = CreateColumnContext()
            .AsVarBinary();
        var column = options.Build(_ => { });
        // Act
        var actual = column.Convert("6qj7n+AIUvDxNw==");
        // Assert
        actual.ShouldBeEquivalentTo(System.Convert.FromBase64String("6qj7n+AIUvDxNw=="));
    }


    [Fact]
    public void AsDate_WithFormat()
    {
        // Arrange
        var options = CreateColumnContext()
            .AsDate("yyyyMMdd", CultureInfo.InvariantCulture);
        var column = options.Build(_ => { });
        // Act
        var actual = column.Convert("20210101");
        // Assert
        actual.ShouldBe(new DateTime(2021, 1, 1));
    }

    [Fact]
    public void AsDate_NotWithFormat()
    {
        // Arrange
        var options = CreateColumnContext()
            .AsDate();
        var column = options.Build(_ => { });

        // Act
        var actual = column.Convert("2021-01-01");

        // Assert
        actual.ShouldBe(new DateTime(2021, 1, 1));
    }

    [Fact]
    public void AsTime_WithFormat()
    {
        // Arrange
        var options = CreateColumnContext()
            .AsTime("hhmmss", CultureInfo.InvariantCulture);
        var column = options.Build(_ => { });
        // Act
        var actual = column.Convert("123456");
        // Assert
        actual.ShouldBe(new TimeSpan(12, 34, 56));
    }

    [Fact]
    public void AsTime_NotWithFormat()
    {
        // Arrange
        var options = CreateColumnContext()
            .AsTime();
        var column = options.Build(_ => { });
        // Act
        var actual = column.Convert("12:34:56");
        // Assert
        actual.ShouldBe(new TimeSpan(12, 34, 56));
    }

    [Fact]
    public void AsDateTime2_WithFormat()
    {
        // Arrange
        var options = CreateColumnContext()
            .AsDateTime2("yyyyMMdd", CultureInfo.InvariantCulture);
        var column = options.Build(_ => { });
        // Act
        var actual = column.Convert("20210101");
        // Assert
        actual.ShouldBe(new DateTime(2021, 1, 1));
    }

    [Fact]
    public void AsDateTime2_NotWithFormat()
    {
        // Arrange
        var options = CreateColumnContext()
            .AsDateTime2();
        var column = options.Build(_ => { });
        // Act
        var actual = column.Convert("2021-01-01");
        // Assert
        actual.ShouldBe(new DateTime(2021, 1, 1));
    }

    [Fact]
    public void AsDateTimeOffset_WithFormat()
    {
        // Arrange
        var options = CreateColumnContext()
            .AsDateTimeOffset("yyyyMMddHHmmK", CultureInfo.InvariantCulture);
        var column = options.Build(_ => { });
        // Act
        var actual = column.Convert("202412210904+09:00");
        // Assert
        actual.ShouldBe(DateTimeOffset.ParseExact("202412210904+09:00", "yyyyMMddHHmmK", null));
    }

    [Fact]
    public void AsDateTimeOffset_NotWithFormat()
    {
        // Arrange
        var options = CreateColumnContext()
            .AsDateTimeOffset();
        var column = options.Build(_ => { });
        // Act
        var actual = column.Convert("2021-01-01+09:00");
        // Assert
        actual.ShouldBe(DateTimeOffset.Parse("2021-01-01+09:00"));
    }

    [Fact]
    public void Convert()
    {
        // Arrange
        var options = CreateColumnContext()
            .Convert(s => s.ToUpper());
        var column = options.Build(_ => { });

        // Act
        var actual = column.Convert("hello");

        // Assert
        actual.ShouldBe("HELLO");
    }

    [Fact]
    public void ConvertHasPriorityOverAsType()
    {
        // Arrange
        var options = CreateColumnContext()
            .Trim()
            .TreatEmptyStringAsNull()
            .AsBit()
            .Convert(s => s.ToUpper());
        var column = options.Build(_ => { });

        // Act
        var actual = column.Convert(" a ");

        // Assert
        actual.ShouldBe("A");
    }
}