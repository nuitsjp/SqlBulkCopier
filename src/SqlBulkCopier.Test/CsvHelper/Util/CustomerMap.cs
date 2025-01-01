using System.Globalization;
using CsvHelper.Configuration;

namespace SqlBulkCopier.Test.CsvHelper.Util
{
    public sealed class CustomerMap : ClassMap<Customer>
    {
        public CustomerMap()
        {
            // 例として、一通りのカラムをマッピングします

            Map(m => m.CustomerId).Name("CustomerId");
            Map(m => m.FirstName).Name("FirstName");
            Map(m => m.LastName).Name("LastName");
            Map(m => m.Email).Name("Email");
            Map(m => m.PhoneNumber).Name("PhoneNumber");
            Map(m => m.AddressLine1).Name("AddressLine1");
            Map(m => m.AddressLine2).Name("AddressLine2");
            Map(m => m.City).Name("City");
            Map(m => m.State).Name("State");
            Map(m => m.PostalCode).Name("PostalCode");
            Map(m => m.Country).Name("Country");

            // 日付: yyyyMMdd のようなフォーマットで書き出したい場合は、Format を設定
            Map(m => m.BirthDate)
                .Name("BirthDate")
                .TypeConverterOption.Format("yyyyMMdd")
                .TypeConverterOption.CultureInfo(CultureInfo.InvariantCulture);

            Map(m => m.Gender).Name("Gender");
            Map(m => m.Occupation).Name("Occupation");

            // decimal
            Map(m => m.Income)
                .Name("Income")
                .TypeConverterOption.CultureInfo(CultureInfo.InvariantCulture);

            // 日付時刻: yyyyMMddHHmmss で書き出す例
            Map(m => m.RegistrationDate)
                .Name("RegistrationDate")
                .TypeConverterOption.Format("yyyyMMddHHmmss")
                .TypeConverterOption.CultureInfo(CultureInfo.InvariantCulture);

            Map(m => m.LastLogin)
                .Name("LastLogin")
                .TypeConverterOption.Format("yyyyMMddHHmmss")
                .TypeConverterOption.CultureInfo(CultureInfo.InvariantCulture);

            // bool → "true/false" あるいは "1/0" にしたい場合など
            Map(m => m.IsActive).Name("IsActive");

            // NVARCHAR(MAX) はそのまま文字列
            Map(m => m.Notes).Name("Notes");

            // CreatedAt, UpdatedAt はデフォルト値があるので基本必須ではないが一応用意
            Map(m => m.CreatedAt)
                .Name("CreatedAt")
                .TypeConverterOption.Format("yyyyMMddHHmmss")
                .TypeConverterOption.CultureInfo(CultureInfo.InvariantCulture);

            Map(m => m.UpdatedAt)
                .Name("UpdatedAt")
                .TypeConverterOption.Format("yyyyMMddHHmmss")
                .TypeConverterOption.CultureInfo(CultureInfo.InvariantCulture);
        }
    }
}