﻿using System.Globalization;
using System.Text;
using Bogus;
using CsvHelper.Configuration;
using CsvHelper;
using System.ComponentModel.DataAnnotations.Schema;

namespace Sample.SetupSampleDatabase;

[Table("Customer")]
public class Customer
{
    public int? CustomerId { get; set; }
    public string? FirstName { get; set; }
    public string? LastName { get; set; }
    public string? Email { get; set; }
    public string? PhoneNumber { get; set; }
    public string? AddressLine1 { get; set; }
    public string? AddressLine2 { get; set; }
    public string? City { get; set; }
    public string? State { get; set; }
    public string? PostalCode { get; set; }
    public string? Country { get; set; }
    public DateTime? BirthDate { get; set; }
    public string? Gender { get; set; }
    public string? Occupation { get; set; }
    public decimal? Income { get; set; }
    public DateTime? RegistrationDate { get; set; }
    public DateTime? LastLogin { get; set; }
    public bool? IsActive { get; set; }
    public string? Notes { get; set; }
    public DateTime? CreatedAt { get; set; }
    public DateTime? UpdatedAt { get; set; }

    public static async Task WriteFixedLengthAsync(string path, int count)
    {
        var customers = GenerateCustomers(count);
        await CreateFixedLengthAsync(customers, path);
    }

    public static async Task WriteCsvAsync(string path, int count)
    {
        var customers = GenerateCustomers(count);
        await CreateCsvAsync(customers, path);
    }

    public static async Task CreateCsvAsync(IEnumerable<Customer> customers, string path)
    {
        var encoding = new UTF8Encoding(false);
        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = true,
            Delimiter = ",",
            NewLine = "\r\n",
            Encoding = encoding,
            ShouldQuote = _ => false
        };

        if (File.Exists(path))
        {
            File.Delete(path);
        }

        await using var memoryStream = File.OpenWrite(path);
        await using var writer = new StreamWriter(memoryStream, encoding, leaveOpen: true);
        await using var csv = new CsvWriter(writer, config);

        csv.Context.RegisterClassMap<CustomerMap>();
        await csv.WriteRecordsAsync(customers);
        await writer.FlushAsync();
    }

    static IEnumerable<Customer> GenerateCustomers(int count)
    {
        var idSeed = 0;
        var faker = new Faker<Customer>("en_US")
            .RuleFor(x => x.CustomerId, _ => ++idSeed)
            .RuleFor(x => x.FirstName, f => f.Name.FirstName())
            .RuleFor(x => x.LastName, f => f.Name.LastName())
            .RuleFor(x => x.Email, f => f.Internet.Email())
            .RuleFor(x => x.PhoneNumber, f => f.Phone.PhoneNumber().Truncate(20))
            .RuleFor(x => x.AddressLine1, f => f.Address.StreetAddress())
            .RuleFor(x => x.AddressLine2, f => f.Address.SecondaryAddress())
            .RuleFor(x => x.City, f => f.Address.City())
            .RuleFor(x => x.State, f => f.Address.StateAbbr())
            .RuleFor(x => x.PostalCode, f => f.Address.ZipCode())
            .RuleFor(x => x.Country, f => f.Address.Country().Truncate(50).Replace(",", string.Empty))
            .RuleFor(x => x.BirthDate, f => f.Date.Past(40, DateTime.Now.AddYears(-20))) // 20〜60歳ぐらい
            .RuleFor(x => x.Gender, f => f.PickRandom("Male", "Female", "Other"))
            .RuleFor(x => x.Occupation, f => f.Name.JobTitle())
            .RuleFor(x => x.Income, f => f.Finance.Amount(1000, 1000000))  // 年収を大まかに
            .RuleFor(x => x.RegistrationDate, f => f.Date.Past(3))         // 過去3年ぐらい
            .RuleFor(x => x.LastLogin, f => f.Date.Recent(30))             // 過去1ヶ月
            .RuleFor(x => x.IsActive, f => f.Random.Bool(0.8f))            // 80% を true
            .RuleFor(x => x.Notes, f => f.Lorem.Sentence())

            // テーブル上は DEFAULT GETDATE() のため本来必須ではない
            // 例として現在時刻を設定しておく
            .RuleFor(x => x.CreatedAt, DateTime.Now)
            .RuleFor(x => x.UpdatedAt, DateTime.Now);

        for (var i = 1; i <= count; i++)
        {
            if (i % 100 == 0)
            {
                Console.Write($"\rWriting {i:###,###,###,###} records...");
            }
            yield return faker.Generate();
        }
    }

    static async Task CreateFixedLengthAsync(IEnumerable<Customer> dataList, string path)
    {
        // UTF-8 エンコーディング (BOM なし)
        Encoding encoding = new UTF8Encoding(false);

        if (File.Exists(path))
        {
            File.Delete(path);
        }

        await using var stream = File.OpenWrite(path);
        await using var writer = new StreamWriter(stream, encoding);

        foreach (var item in dataList)
        {
            await writer.WriteAsync(PadRightBytes(item.CustomerId?.ToString() ?? "", 10, encoding));
            await writer.WriteAsync(PadRightBytes(item.FirstName ?? "", 50, encoding));
            await writer.WriteAsync(PadRightBytes(item.LastName ?? "", 50, encoding));
            await writer.WriteAsync(PadRightBytes(item.Email ?? "", 100, encoding));
            await writer.WriteAsync(PadRightBytes(item.PhoneNumber ?? "", 20, encoding));
            await writer.WriteAsync(PadRightBytes(item.AddressLine1 ?? "", 100, encoding));
            await writer.WriteAsync(PadRightBytes(item.AddressLine2 ?? "", 100, encoding));
            await writer.WriteAsync(PadRightBytes(item.City ?? "", 50, encoding));
            await writer.WriteAsync(PadRightBytes(item.State ?? "", 50, encoding));
            await writer.WriteAsync(PadRightBytes(item.PostalCode ?? "", 10, encoding));
            await writer.WriteAsync(PadRightBytes(item.Country ?? "", 50, encoding));
            await writer.WriteAsync(PadRightBytes(item.BirthDate?.ToString("yyyy-MM-dd") ?? "", 10, encoding));
            await writer.WriteAsync(PadRightBytes(item.Gender ?? "", 10, encoding));
            await writer.WriteAsync(PadRightBytes(item.Occupation ?? "", 50, encoding));
            await writer.WriteAsync(PadRightBytes(item.Income?.ToString("0.00") ?? "", 21, encoding));
            await writer.WriteAsync(PadRightBytes(item.RegistrationDate?.ToString("yyyy-MM-dd HH:mm:ss.fff") ?? "", 23, encoding));
            await writer.WriteAsync(PadRightBytes(item.LastLogin?.ToString("yyyy-MM-dd HH:mm:ss.fff") ?? "", 23, encoding));
            await writer.WriteAsync(PadRightBytes(item.IsActive == true ? "1" : "0", 1, encoding));
            await writer.WriteAsync(PadRightBytes(item.Notes ?? "", 500, encoding));
            await writer.WriteAsync(PadRightBytes(item.CreatedAt?.ToString("yyyy-MM-dd HH:mm:ss.fff") ?? "", 23, encoding));
            await writer.WriteAsync(PadRightBytes(item.UpdatedAt?.ToString("yyyy-MM-dd HH:mm:ss.fff") ?? "", 23, encoding));
            await writer.WriteLineAsync(); // 行区切り
        }

        await writer.FlushAsync();
    }

    // 参考コードと同じPadRightBytesメソッドが必要です
    static string PadRightBytes(string input, int byteCount, Encoding encoding)
    {
        var bytes = encoding.GetBytes(input);
        if (bytes.Length >= byteCount)
        {
            return encoding.GetString(bytes.Take(byteCount).ToArray());
        }

        var paddedBytes = new byte[byteCount];
        Array.Copy(bytes, paddedBytes, bytes.Length);
        for (var i = bytes.Length; i < byteCount; i++)
        {
            paddedBytes[i] = (byte)' ';
        }

        return encoding.GetString(paddedBytes);
    }

}