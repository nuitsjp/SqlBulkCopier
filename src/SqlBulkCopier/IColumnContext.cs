﻿using System.Data;
using System.Globalization;

namespace SqlBulkCopier;

/// <summary>
/// Defines the context for configuring column mappings and data conversions in bulk copy operations.
/// This interface provides a fluent API for specifying how source data should be mapped and converted
/// to SQL Server column types.
/// </summary>
/// <remarks>
/// The column context allows for:
/// - Specifying the SQL Server data type for the destination column
/// - Configuring data format strings and culture-specific parsing
/// - Setting up string trimming behaviors
/// - Defining custom data conversions
/// </remarks>
public interface IColumnContext
{
    /// <summary>
    /// Gets the name of the column in the destination table.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Gets the zero-based ordinal position of the column in the source data.
    /// </summary>
    int Ordinal { get; }

    /// <summary>
    /// Gets the SQL Server data type of the destination column.
    /// May be null if not explicitly specified.
    /// </summary>
    SqlDbType? SqlDbType { get; }

    /// <summary>
    /// Gets the format string used for parsing date/time or numeric values.
    /// May be null if no specific format is required.
    /// </summary>
    string? Format { get; }

    /// <summary>
    /// Configures the column to be mapped as a SQL BIGINT.
    /// </summary>
    /// <param name="numberStyles">The number styles to use when parsing the source string.</param>
    /// <param name="cultureInfo">The culture-specific formatting information. If null, the current culture is used.</param>
    /// <returns>The column context for method chaining.</returns>
    IColumnContext AsBigInt(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null);

    /// <summary>
    /// Configures the column to be mapped as a SQL BIT.
    /// </summary>
    /// <returns>The column context for method chaining.</returns>
    IColumnContext AsBit();

    /// <summary>
    /// Configures the column to be mapped as a SQL UNIQUEIDENTIFIER.
    /// </summary>
    /// <returns>The column context for method chaining.</returns>
    IColumnContext AsUniqueIdentifier();

    /// <summary>
    /// Configures the column to be mapped as a SQL DATE.
    /// </summary>
    /// <param name="format">The format string to use when parsing the date string. If null, standard date formats are tried.</param>
    /// <param name="cultureInfo">The culture-specific formatting information. If null, the current culture is used.</param>
    /// <param name="dateTimeStyles">The date time styles to use when parsing.</param>
    /// <returns>The column context for method chaining.</returns>
    IColumnContext AsDate(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles dateTimeStyles = DateTimeStyles.None);

    IColumnContext AsDateTime(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles dateTimeStyles = DateTimeStyles.None);
    IColumnContext AsDecimal(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null);
    IColumnContext AsFloat(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null);
    IColumnContext AsInt(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null);
    IColumnContext AsMoney(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null);
    IColumnContext AsReal(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null);
    IColumnContext AsSmallDateTime(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles dateTimeStyles = DateTimeStyles.None);
    IColumnContext AsSmallInt(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null);
    IColumnContext AsSmallMoney(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null);
    IColumnContext AsTimestamp(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles style = DateTimeStyles.None);
    IColumnContext AsTinyInt(NumberStyles numberStyles = NumberStyles.None, CultureInfo? cultureInfo = null);
    IColumnContext AsDateTime2(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles dateTimeStyles = DateTimeStyles.None);
    IColumnContext AsTime(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles dateTimeStyles = DateTimeStyles.None);
    IColumnContext AsDateTimeOffset(string? format = null, CultureInfo? cultureInfo = null, DateTimeStyles dateTimeStyles = DateTimeStyles.None);
    IColumnContext AsBinary();
    IColumnContext AsVarBinary();
    IColumnContext AsImage();

    /// <summary>
    /// Applies trimming to the source string value before conversion.
    /// </summary>
    /// <param name="trimChars">An array of characters to remove. If null, white-space characters are removed.</param>
    /// <returns>The column context for method chaining.</returns>
    IColumnContext Trim(char[]? trimChars = null);

    /// <summary>
    /// Applies trimming from the start of the source string value before conversion.
    /// </summary>
    /// <param name="trimChars">An array of characters to remove. If null, white-space characters are removed.</param>
    /// <returns>The column context for method chaining.</returns>
    IColumnContext TrimStart(char[]? trimChars = null);

    /// <summary>
    /// Applies trimming from the end of the source string value before conversion.
    /// </summary>
    /// <param name="trimChars">An array of characters to remove. If null, white-space characters are removed.</param>
    /// <returns>The column context for method chaining.</returns>
    IColumnContext TrimEnd(char[]? trimChars = null);

    /// <summary>
    /// Configures empty strings to be treated as NULL values when inserted into the database.
    /// </summary>
    /// <returns>The column context for method chaining.</returns>
    IColumnContext TreatEmptyStringAsNull();

    /// <summary>
    /// Specifies a custom conversion function for the column's values.
    /// </summary>
    /// <param name="convert">A function that takes a string input and returns the converted object.</param>
    /// <returns>The column context for method chaining.</returns>
    /// <remarks>
    /// The conversion function should handle null or empty inputs appropriately.
    /// Any exceptions thrown by the conversion function will cause the bulk copy operation to fail.
    /// </remarks>
    IColumnContext Convert(Func<string, object> convert);

    /// <summary>
    /// Builds the final column configuration using the specified default context.
    /// </summary>
    /// <param name="setDefaultContext">An action that configures the default context for the column.</param>
    /// <returns>A Column instance configured with all the specified settings.</returns>
    Column Build(Action<IColumnContext> setDefaultContext);
}