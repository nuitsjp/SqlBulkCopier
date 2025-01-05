using Sample.SetupSampleDatabase;

// ReSharper disable StringLiteralTypo

await Database.SetupAsync();
//await Customer.WriteFixedLengthAsync(@"..\..\..\Asserts\Customer.dat", 100);
//await Customer.WriteCsvAsync(@"..\..\..\Asserts\Customer.csv", 100);
await Customer.WriteCsvAsync(@"..\..\..\Asserts\Customer_10_000_000.csv", 10_000_000);

Console.WriteLine("Setup Success.");
return;