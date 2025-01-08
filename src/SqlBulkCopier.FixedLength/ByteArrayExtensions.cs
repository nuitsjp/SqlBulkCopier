using System.Buffers;

namespace SqlBulkCopier.FixedLength;

public static class ByteArrayExtensions
{
    /// <summary>
    /// Determines whether the beginning of this byte array matches the specified pattern.
    /// </summary>
    /// <param name="source">The source byte array to search within.</param>
    /// <param name="pattern">The pattern to search for.</param>
    /// <returns>true if the start of the array matches the pattern; otherwise, false.</returns>
    public static bool StartsWith(this byte[] source, byte[] pattern)
    {
        if (source == null) throw new ArgumentNullException(nameof(source));
        if (pattern == null) throw new ArgumentNullException(nameof(pattern));
        if (pattern.Length > source.Length) return false;

        // Use Span to avoid memory allocation
        return source.AsSpan(0, pattern.Length).SequenceEqual(pattern);
    }

    /// <summary>
    /// Determines whether the end of this byte array matches the specified pattern.
    /// </summary>
    /// <param name="source">The source byte array to search within.</param>
    /// <param name="pattern">The pattern to search for.</param>
    /// <returns>true if the end of the array matches the pattern; otherwise, false.</returns>
    public static bool EndsWith(this byte[] source, byte[] pattern)
    {
        if (source == null) throw new ArgumentNullException(nameof(source));
        if (pattern == null) throw new ArgumentNullException(nameof(pattern));
        if (pattern.Length > source.Length) return false;

        return source.AsSpan(source.Length - pattern.Length).SequenceEqual(pattern);
    }

    /// <summary>
    /// Determines whether this byte array exactly matches another byte array.
    /// </summary>
    /// <param name="source">The source byte array to compare.</param>
    /// <param name="other">The byte array to compare against.</param>
    /// <returns>true if the arrays are equal; otherwise, false.</returns>
    public static bool SequenceEqualsOptimized(this byte[] source, byte[] other)
    {
        if (source == null) throw new ArgumentNullException(nameof(source));
        if (other == null) throw new ArgumentNullException(nameof(other));
        if (source.Length != other.Length) return false;

        return source.AsSpan().SequenceEqual(other);
    }
}