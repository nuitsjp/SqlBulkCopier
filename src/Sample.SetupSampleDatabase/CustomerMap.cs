using CsvHelper.Configuration;

namespace Sample.SetupSampleDatabase;

public class CustomerMap : ClassMap<Customer>
{
    public CustomerMap()
    {
        // 数値系
        Map(m => m.CustomerId).Index(0);
        Map(m => m.Income).Index(1).TypeConverterOption.Format("F2");  // 小数点2桁で出力

        // 名前・連絡先情報
        Map(m => m.FirstName).Index(2);
        Map(m => m.LastName).Index(3);
        Map(m => m.Email).Index(4);
        Map(m => m.PhoneNumber).Index(5);

        // 住所情報
        Map(m => m.AddressLine1).Index(6);
        Map(m => m.AddressLine2).Index(7);
        Map(m => m.City).Index(8);
        Map(m => m.State).Index(9);
        Map(m => m.PostalCode).Index(10);
        Map(m => m.Country).Index(11);

        // 日付系
        Map(m => m.BirthDate).Index(12).TypeConverterOption.Format("yyyyMMdd");
        Map(m => m.RegistrationDate).Index(13).TypeConverterOption.Format("yyyyMMddHHmmss");
        Map(m => m.LastLogin).Index(14).TypeConverterOption.Format("yyyyMMddHHmmss");
        Map(m => m.CreatedAt).Index(15).TypeConverterOption.Format("yyyyMMddHHmmss");
        Map(m => m.UpdatedAt).Index(16).TypeConverterOption.Format("yyyyMMddHHmmss");

        // その他の情報
        Map(m => m.Gender).Index(17);
        Map(m => m.Occupation).Index(18);
        Map(m => m.IsActive).Index(19);
        Map(m => m.Notes).Index(20);
    }
}