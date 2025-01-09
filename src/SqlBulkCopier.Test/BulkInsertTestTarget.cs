namespace SqlBulkCopier.Test;

public class BulkInsertTestTarget
{
    // デフォルトコンストラクター（Bogusが利用）

    // PK (IDENTITY) → int?
    public int? Id { get; set; }

    // TINYINT → byte?
    public byte? TinyInt { get; set; }

    // SMALLINT → short?
    public short? SmallInt { get; set; }

    // INT → int?
    public int? IntValue { get; set; }

    // BIGINT → long?
    public long? BigInt { get; set; }

    // BIT → bool?
    public bool? BitValue { get; set; }

    // DECIMAL(10,2), NUMERIC(10,2), MONEY, SMALLMONEY → decimal?
    public decimal? DecimalValue { get; set; }
    public decimal? NumericValue { get; set; }
    public decimal? MoneyValue { get; set; }
    public decimal? SmallMoneyValue { get; set; }

    // FLOAT → double?
    public double? FloatValue { get; set; }

    // REAL → float?
    public float? RealValue { get; set; }

    // DATE, DATETIME, SMALLDATETIME, DATETIME2 → DateTime?
    public DateTime? DateValue { get; set; }
    public DateTime? DateTimeValue { get; set; }
    public DateTime? SmallDateTimeValue { get; set; }
    public DateTime? DateTime2Value { get; set; }

    // TIME(7) → TimeSpan?
    public TimeSpan? TimeValue { get; set; }

    // DATETIMEOFFSET(7) → DateTimeOffset?
    public DateTimeOffset? DateTimeOffsetValue { get; set; }

    // CHAR, VARCHAR, NCHAR, NVARCHAR → string?
    public string? CharValue { get; set; }
    public string? VarCharValue { get; set; }
    public string? NCharValue { get; set; }
    public string? NVarCharValue { get; set; }

    // BINARY, VARBINARY → byte[]?
    public byte[]? BinaryValue { get; set; }
    public byte[]? VarBinaryValue { get; set; }

    // UNIQUEIDENTIFIER → Guid?
    public Guid? UniqueIdValue { get; set; }

    // XML → string?（必要に応じてXDocument等を使用）
    public string? XmlValue { get; set; }
}