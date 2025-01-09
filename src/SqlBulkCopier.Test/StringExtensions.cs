namespace SqlBulkCopier.Test;

public static class StringExtensions
{
    /// <summary>
    /// •¶š—ñ‚ğw’è‚Ì’·‚³‚ÉØ‚è‹l‚ß‚Ü‚·B
    /// </summary>
    /// <param name="str">‘ÎÛ‚Ì•¶š—ñ</param>
    /// <param name="maxLength">Å‘å’·</param>
    /// <returns>Ø‚è‹l‚ß‚ç‚ê‚½•¶š—ñ</returns>
    public static string Truncate(this string str, int maxLength)
    {
        if (string.IsNullOrEmpty(str)) return str;

        return str.Length <= maxLength
            ? str
            : str.Substring(0, maxLength);
    }
}