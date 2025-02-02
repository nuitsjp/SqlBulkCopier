using System.Globalization;
using Microsoft.Extensions.Configuration;

namespace SqlBulkCopier.FixedLength.Hosting;

public static class ConfigurationSectionExtensions
{
    public static T? GetEnum<T>(this IConfigurationSection section, string key) where T : struct
    {
        var value = section[key];
        if (value is null)
        {
            return null;
        }
        return Enum.TryParse<T>(value, true, out var result) ? result : null;
    }

    public static T GetEnum<T>(this IConfigurationSection section, string key, T defaultValue) where T : struct
    {
        var value = section[key];
        if (value is null)
        {
            return defaultValue;
        }
        return Enum.TryParse<T>(value, true, out var result) ? result : defaultValue;
    }

    public static CultureInfo? GetCultureInfo(this IConfigurationSection section, string key)
    {
        var value = section[key];
        if (value is null)
        {
            return null;
        }
        return CultureInfo.GetCultureInfo(value);
    }
}