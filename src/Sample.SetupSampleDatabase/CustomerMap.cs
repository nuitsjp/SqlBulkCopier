using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.TypeConversion;

namespace Sample.SetupSampleDatabase;

public sealed class CustomerMap : ClassMap<Customer>
{
    public CustomerMap()
    {
        Map(m => m.CustomerId);
        Map(m => m.FirstName);
        Map(m => m.LastName);
        Map(m => m.Email);
        Map(m => m.PhoneNumber);
        Map(m => m.AddressLine1);
        Map(m => m.AddressLine2);
        Map(m => m.City);
        Map(m => m.State);
        Map(m => m.PostalCode);
        Map(m => m.Country);
        Map(m => m.BirthDate).TypeConverterOption.Format("yyyy-MM-dd");
        Map(m => m.Gender);
        Map(m => m.Occupation);
        Map(m => m.Income).TypeConverterOption.Format("F2");  // 小数点2桁で出力
        Map(m => m.RegistrationDate).TypeConverterOption.Format("yyyy-MM-dd HH:mm:ss.fff");
        Map(m => m.LastLogin).TypeConverterOption.Format("yyyy-MM-dd HH:mm:ss.fff");
        Map(m => m.IsActive).TypeConverter<BoolToIntConverter>();
        Map(m => m.Notes);
        Map(m => m.CreatedAt).TypeConverterOption.Format("yyyy-MM-dd HH:mm:ss.fff");
        Map(m => m.UpdatedAt).TypeConverterOption.Format("yyyy-MM-dd HH:mm:ss.fff");
    }
}

public class BoolToIntConverter : DefaultTypeConverter
{
    public override string ConvertToString(object? value, IWriterRow row, MemberMapData memberMapData)
    {
        if (value == null) return "0";
        return (bool)value ? "1" : "0";
    }

    public override object ConvertFromString(string? text, IReaderRow row, MemberMapData memberMapData)
    {
        if (string.IsNullOrEmpty(text)) return false;
        return text.Trim() == "1";
    }
}
