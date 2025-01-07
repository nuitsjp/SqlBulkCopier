namespace Sample.SetupSampleDatabase;

public static class StringExtensions
{
    /// <summary>
    /// 文字列を指定の長さに切り詰めます。
    /// </summary>
    /// <param name="str">対象の文字列</param>
    /// <param name="maxLength">最大長</param>
    /// <returns>切り詰められた文字列</returns>
    public static string Truncate(this string str, int maxLength)
    {
        if (string.IsNullOrEmpty(str)) return str;

        return str.Length <= maxLength
            ? str
            : str.Substring(0, maxLength);
    }
}