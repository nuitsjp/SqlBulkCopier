namespace SqlBulkCopier.Test;

public static class StringExtensions
{
    /// <summary>
    /// ��������w��̒����ɐ؂�l�߂܂��B
    /// </summary>
    /// <param name="str">�Ώۂ̕�����</param>
    /// <param name="maxLength">�ő咷</param>
    /// <returns>�؂�l�߂�ꂽ������</returns>
    public static string Truncate(this string str, int maxLength)
    {
        if (string.IsNullOrEmpty(str)) return str;

        return str.Length <= maxLength
            ? str
            : str.Substring(0, maxLength);
    }
}