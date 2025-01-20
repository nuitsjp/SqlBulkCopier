using System.Globalization;
using System.Text;
using CsvHelper;
using CsvHelper.Configuration;
using SqlBulkCopier.Test.CsvHelper.Util;

namespace SqlBulkCopier.Test.CsvHelper;

public abstract class CsvBulkCopierBuilderTest<TBuilder>(bool hasHeader) : WriteToServerAsync<TBuilder>
    where TBuilder : IBulkCopierBuilder<TBuilder>
{
    /// <summary>
    /// 生成したデータを固定長でファイル出力(バイト数でパディング)する
    /// </summary>
    // ReSharper disable once UnusedMember.Local
    protected override async Task<Stream> CreateBulkInsertStreamAsync(List<BulkInsertTestTarget> dataList, bool withHeaderAndFooter = false)
    {
        var encoding = new UTF8Encoding(false);
        using var stream = new MemoryStream();
        // ReSharper disable once UseAwaitUsing
        using var writer = new StreamWriter(stream, encoding);
        var csvWriter = new CsvWriter(
            writer,
            new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = hasHeader
            });
        csvWriter.Context.RegisterClassMap<BulkInsertTestTargetMap>();
        await csvWriter.WriteRecordsAsync(Targets, CancellationToken.None);
        await csvWriter.FlushAsync();

        if (withHeaderAndFooter == false)
        {
            // csvWriterなどを閉じると、streamも閉じられるため作り直す
            return new MemoryStream(stream.ToArray());
        }

        return new MemoryStream(
            encoding.GetBytes(
                $"""
                 Header
                 {encoding.GetString(stream.ToArray())}
                 Footer
                 """));
    }
}