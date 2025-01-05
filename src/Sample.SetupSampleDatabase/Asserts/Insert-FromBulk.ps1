# bcp SqlBulkCopier.dbo.Customer in .\Customer.csv -f Customer.fmt -T -S . -F 2 -e Customer.error
# bcp SqlBulkCopier.dbo.Customer in .\Customer_10_000_000.csv -f Customer.fmt -T -S . -F 2 -e Customer.error
bcp SqlBulkCopier.dbo.Customer in D:\SqlBulkCopier\src\Sample.SetupSampleDatabase\Asserts\Customer_10_000_000.csv -f D:\SqlBulkCopier\src\Sample.SetupSampleDatabase\Asserts\Customer.fmt -T -S . -F 2
