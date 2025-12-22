using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RxDatasets.Abstractions;
using System.Text;
using System.Text.RegularExpressions;

namespace RxDatasets.Aliasing;

/// <summary>
/// Dependency injection configuration for alias-based dataset readers.
/// Allows querying entities by their aliases in addition to their primary IDs.
/// </summary>
public static class DI
{
    /// <summary>
    /// Registers an alias-based dataset reader for type T.
    /// </summary>
    /// <typeparam name="T">The DTO type (must be a protobuf message)</typeparam>
    /// <param name="services">The service collection</param>
    /// <param name="aliasName">A unique name for this alias reader (used to retrieve it later)</param>
    /// <param name="aliasFactory">A function that converts entity bytes to a list of aliases for that entity.
    /// More than one alias name can exist per entity type, and multiple alias readers can be registered.</param>
    /// <returns>The service collection for chaining</returns>
    public static IServiceCollection AddDatasetAlias<T>(
        this IServiceCollection services,
        string aliasName,
        Func<ReadOnlyMemory<byte>, IEnumerable<string>> aliasFactory)
        where T : class, IMessage<T>, new()
    {
        if (string.IsNullOrWhiteSpace(aliasName))
            throw new ArgumentException("Alias name cannot be null or empty", nameof(aliasName));

        if (aliasFactory == null)
            throw new ArgumentNullException(nameof(aliasFactory));

        // Register a factory that creates the alias reader
        services.AddKeyedSingleton<IDatasetReader<T>>(
            aliasName,
            (serviceProvider, key) =>
            {
                var sourceReader = serviceProvider.GetRequiredService<IDatasetReader<T>>();
                var logger = serviceProvider.GetRequiredService<ILogger<AliasDatasetReader<T>>>();
                var appLifetime = serviceProvider.GetRequiredService<IHostApplicationLifetime>();

                var aliasReader = new AliasDatasetReader<T>(
                    sourceReader,
                    aliasFactory,
                    aliasName,
                    logger,
                    appLifetime);

                // Start initialization in background
                aliasReader.StartInitialization();

                return aliasReader;
            });

        return services;
    }

    /// <summary>
    /// Retrieves an alias-based dataset reader by its alias name.
    /// </summary>
    /// <typeparam name="T">The DTO type</typeparam>
    /// <param name="serviceProvider">The service provider</param>
    /// <param name="aliasName">The alias name used during registration</param>
    /// <returns>The alias reader, or null if not found</returns>
    public static IDatasetReader<T>? GetDatasetReader<T>(
        this IServiceProvider serviceProvider,
        string aliasName)
        where T : class, IMessage<T>, new()
    {
        return serviceProvider.GetKeyedService<IDatasetReader<T>>(aliasName);
    }

    /// <summary>
    /// Retrieves a GrpcReaderServiceImpl by its key.
    /// </summary>
    /// <typeparam name="T">The DTO type</typeparam>
    /// <param name="serviceProvider">The service provider</param>
    /// <param name="serviceName">The service key used during registration</param>
    /// <returns>The gRPC service, or null if not found</returns>
    public static GrpcReaderServiceImpl<T>? GetGrpcReaderService<T>(
        this IServiceProvider serviceProvider,
        string serviceName)
        where T : class, IMessage<T>, new()
    {
        return serviceProvider.GetKeyedService<GrpcReaderServiceImpl<T>>(serviceName);
    }

    /// <summary>
    /// Registers multiple alias-based dataset readers for type T.
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    /// <param name="services"></param>
    /// <param name="protoParser">The MessageParser for the protobuf message type (e.g., Link.Parser)</param>
    /// <param name="idPattern"></param>
    /// <param name="aliasConfigs"></param>
    public static void AddDatasetAliases<TEntity>(
    this IServiceCollection services,
    MessageParser<TEntity> protoParser,
    string idPattern,
    params (string AliasName, string AliasPattern)[] aliasConfigs)
    where TEntity : class, IMessage<TEntity>, new()
    {
        foreach (var aliasNameValues in aliasConfigs.GroupBy(ac => ac.AliasName))
        {
            var aliasName = aliasNameValues.Key;
            var aliasPatterns = aliasNameValues.Select(av => av.AliasPattern).ToList();

            // Create alias factory
            var aliasFactory = PatternsAndExpressions.CreateAliasFactoryDelegate(idPattern, aliasPatterns, protoParser);
            services.AddDatasetAlias<TEntity>(aliasName, aliasFactory);
        }
    }
}

public static class PatternsAndExpressions
{
    private static readonly Regex PlaceholderRegex = new(@"\{([^}]+)\}", RegexOptions.Compiled);

    public static Func<ReadOnlyMemory<byte>, IEnumerable<string>> CreateAliasFactoryDelegate<TEntity>(
        string idPattern,
        List<string> aliasPatterns,
        MessageParser<TEntity> protoParser)
    where TEntity : class, IMessage<TEntity>, new()
    {
        // Extract variables from the ID pattern: e.g., "t/{tenantid}/s/{storeid}/links/{linkid}"
        // This produces: [("tenantid", 0), ("storeid", 1), ("linkid", 2)] with their positions in the split result
        var idVariables = ExtractPatternVariables(idPattern);

        return (ReadOnlyMemory<byte> entityBytes) =>
        {
            try
            {
                // Parse the proto message using the typed parser
                var entity = protoParser.ParseFrom(entityBytes.Span);
                if (entity == null)
                    return Enumerable.Empty<string>();

                // Generate aliases from each alias pattern and flatten, then deduplicate
                var allAliases = aliasPatterns
                    .SelectMany(aliasPattern => EvaluateAliasPattern(aliasPattern, idVariables, entity))
                    .Distinct()
                    .ToList();

                return allAliases;
            }
            catch
            {
                // Silently fail and return empty aliases
                return Enumerable.Empty<string>();
            }
        };
    }

    /// <summary>
    /// Extracts variable names and their positions from an ID pattern.
    /// For example, "t/{tenantid}/s/{storeid}/links/{linkid}" produces:
    /// {tenantid: 1, storeid: 3, linkid: 5} (positions in the split result)
    /// </summary>
    public static Dictionary<string, int> ExtractPatternVariables(string idPattern)
    {
        var variables = new Dictionary<string, int>();
        var parts = idPattern.Split('/');
        int position = 0;

        foreach (var part in parts)
        {
            if (part.StartsWith('{') && part.EndsWith('}'))
            {
                var varName = part.Substring(1, part.Length - 2);
                variables[varName] = position;
            }

            position++;
        }

        return variables;
    }

    /// <summary>
    /// Evaluates an alias pattern by:
    /// 1. Extracting the entity's ID and parsing it according to the ID pattern
    /// 2. For each placeholder in the alias pattern:
    ///    - If {.field} syntax: use JSON-style field access on the proto message (may yield multiple values for arrays)
    ///    - If {variable}: use the value extracted from the ID
    /// 3. Joining the parts to form the final alias(es)
    /// 
    /// If any placeholder yields multiple values (from array iteration), this will yield
    /// one alias for each distinct combination of values.
    /// 
    /// Examples:
    ///   aliasPattern: "{.id/[..4]}/item/{.single.linked_item.item_id}/links/{linkid}"
    ///   For Link with id="t/100/s/200/links/abc", single.linked_item.item_id="xyz":
    ///     Result: ["100/s/200/item/xyz/links/abc"]
    ///
    ///   aliasPattern: "item/{.multi_item.linked_items[].item_id}"
    ///   For Link with multi_item.linked_items having ["a", "b", "c"] (3 distinct items):
    ///     Result: ["item/a", "item/b", "item/c"]
    /// </summary>
    public static IEnumerable<string> EvaluateAliasPattern(
        string aliasPattern,
        Dictionary<string, int> idVariables,
        object entity)
    {
        try
        {
            // First, extract the ID from the entity and parse it according to the ID pattern
            var idProperty = entity.GetType().GetProperty("Id");
            if (idProperty == null)
                return Enumerable.Empty<string>();

            var entityId = idProperty.GetValue(entity) as string;
            if (string.IsNullOrEmpty(entityId))
                return Enumerable.Empty<string>();

            // Parse the ID according to the ID pattern
            var idParts = entityId.Split('/');
            var idValues = new Dictionary<string, string>();

            foreach (var (varName, position) in idVariables)
            {
                if (position < idParts.Length)
                {
                    idValues[varName] = idParts[position];
                }
            }

            // Parse the pattern into alternating literal and placeholder segments
            var matches = PlaceholderRegex.Matches(aliasPattern);
            var segments = new List<(bool isPlaceholder, string content)>();
            int lastIndex = 0;

            foreach (Match match in matches)
            {
                // Add literal text before this placeholder
                if (match.Index > lastIndex)
                {
                    segments.Add((false, aliasPattern.Substring(lastIndex, match.Index - lastIndex)));
                }

                // Add placeholder
                var placeholder = match.Groups[1].Value;
                segments.Add((true, placeholder));
                lastIndex = match.Index + match.Length;
            }

            // Add any remaining literal text
            if (lastIndex < aliasPattern.Length)
            {
                segments.Add((false, aliasPattern.Substring(lastIndex)));
            }

            // Evaluate each placeholder to get all possible values
            var allValues = new List<List<string>>();
            foreach (var (isPlaceholder, content) in segments)
            {
                if (!isPlaceholder)
                    continue;

                if (content.StartsWith('.'))
                {
                    // JSON-style field access - may return multiple values for array iteration
                    var values = EvaluateJsonPath(content, entity).ToList();
                    if (values.Count == 0)
                    {
                        // If field doesn't exist or is empty, skip this alias pattern entirely
                        return Enumerable.Empty<string>();
                    }
                    allValues.Add(values);
                }
                else
                {
                    // Variable reference from ID pattern
                    if (idValues.TryGetValue(content, out var value))
                    {
                        allValues.Add(new List<string> { value });
                    }
                    else
                    {
                        // Variable not found in ID, skip this alias
                        return Enumerable.Empty<string>();
                    }
                }
            }

            // Generate all combinations of values
            var results = new List<string>();
            GenerateAliasCombinations(segments, allValues, 0, "", results);

            return results;
        }
        catch
        {
            return Enumerable.Empty<string>();
        }
    }

    /// <summary>
    /// Evaluates an expression that references ID placeholders (e.g. {tenantid}, {storeid})
    /// using the provided ID pattern and DTO entity ID.
    /// </summary>
    public static string? EvaluateIdPatternExpression(string expression, string idPattern, string entityId)
    {
        if (string.IsNullOrWhiteSpace(expression) || string.IsNullOrWhiteSpace(idPattern) || string.IsNullOrWhiteSpace(entityId))
            return null;

        var idVariables = ExtractPatternVariables(idPattern);
        if (idVariables.Count == 0)
            return null;

        var idParts = entityId.Split('/');
        var idValues = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (variableName, position) in idVariables)
        {
            if (position < idParts.Length)
            {
                idValues[variableName] = idParts[position];
            }
        }

        var builder = new StringBuilder();
        int lastIndex = 0;
        var matches = PlaceholderRegex.Matches(expression);
        foreach (Match match in matches)
        {
            if (match.Index > lastIndex)
            {
                builder.Append(expression.Substring(lastIndex, match.Index - lastIndex));
            }

            var placeholder = match.Groups[1].Value;
            if (string.IsNullOrEmpty(placeholder) || placeholder.StartsWith(".", StringComparison.Ordinal))
                return null;

            if (!idValues.TryGetValue(placeholder, out var value))
                return null;

            builder.Append(value);
            lastIndex = match.Index + match.Length;
        }

        if (lastIndex < expression.Length)
        {
            builder.Append(expression.Substring(lastIndex));
        }

        var result = builder.ToString();
        return string.IsNullOrEmpty(result) ? null : result;
    }

    /// <summary>
    /// Recursively generates all combinations of alias values.
    /// Iterates through segments, adding literals directly and iterating through value lists for placeholders.
    /// </summary>
    private static void GenerateAliasCombinations(
        List<(bool isPlaceholder, string content)> segments,
        List<List<string>> valueLists,
        int segmentIndex,
        string currentAlias,
        List<string> results)
    {
        if (segmentIndex >= segments.Count)
        {
            results.Add(currentAlias);
            return;
        }

        var (isPlaceholder, content) = segments[segmentIndex];

        if (!isPlaceholder)
        {
            // Literal text - just append and move to next segment
            GenerateAliasCombinations(segments, valueLists, segmentIndex + 1, currentAlias + content, results);
        }
        else
        {
            // Placeholder - find which value list this corresponds to
            // Count how many placeholders we've seen so far
            int placeholderIndex = 0;
            for (int i = 0; i < segmentIndex; i++)
            {
                if (segments[i].isPlaceholder)
                    placeholderIndex++;
            }

            // For each value in this placeholder's value list, recurse
            var valueList = valueLists[placeholderIndex];
            foreach (var value in valueList)
            {
                GenerateAliasCombinations(segments, valueLists, segmentIndex + 1, currentAlias + value, results);
            }
        }
    }

    /// <summary>
    /// Splits a path into segments, respecting bracket notation.
    /// E.g., "multi_item.linked_items[..2].item_id" -> ["multi_item", "linked_items[..2]", "item_id"]
    /// Does not include empty segments (from leading/trailing dots).
    /// </summary>
    private static string[] SplitPathIntoSegments(string path)
    {
        var segments = new List<string>();
        var currentSegment = new System.Text.StringBuilder();
        int bracketDepth = 0;

        foreach (var ch in path)
        {
            if (ch == '[')
            {
                bracketDepth++;
                currentSegment.Append(ch);
            }
            else if (ch == ']')
            {
                bracketDepth--;
                currentSegment.Append(ch);
            }
            else if (ch == '.' && bracketDepth == 0)
            {
                // This dot is a separator
                var segment = currentSegment.ToString();
                if (!string.IsNullOrEmpty(segment))
                {
                    segments.Add(segment);
                }
                currentSegment.Clear();
            }
            else
            {
                currentSegment.Append(ch);
            }
        }

        // Add the last segment
        var lastSegment = currentSegment.ToString();
        if (!string.IsNullOrEmpty(lastSegment))
        {
            segments.Add(lastSegment);
        }

        return segments.ToArray();
    }

    /// <summary>
    /// Extracts value slicing operators from the end of a path.
    /// Patterns:
    ///   /[..N] - from start to N
    ///   /[M..N] - from M to N
    ///   /[M..] - from M to end
    ///   /[N] - single index N
    /// 
    /// Returns: (pathWithoutSlicing, sliceStart, sliceEnd)
    /// If no slicing operator is found, returns (path, null, null)
    /// </summary>
    private static (string path, int? sliceStart, int? sliceEnd) ExtractValueSlicing(string path)
    {
        int? valueSliceStart = null;
        int? valueSliceEnd = null;

        // Try /[..N] pattern
        var valueSliceMatchLeading = Regex.Match(path, @"^(.+)/\[\.\.\d+\]$");
        if (valueSliceMatchLeading.Success)
        {
            var sliceStr = Regex.Match(path, @"/\[\.\.\d+\]$").Value;
            valueSliceEnd = int.Parse(sliceStr.Substring(4, sliceStr.Length - 5)); // Extract N from /[..N]
            path = valueSliceMatchLeading.Groups[1].Value;
            valueSliceStart = 0;
            return (path, valueSliceStart, valueSliceEnd);
        }

        // Try /[M..N], /[M..], or /[N] patterns
        var valueSliceMatchRange = Regex.Match(path, @"^(.+)/\[(\d+)\.\.\d*\]$");
        if (valueSliceMatchRange.Success)
        {
            var sliceStr = Regex.Match(path, @"/\[(\d+)\.\.\d*\]$").Value;
            var parts = sliceStr.Substring(2, sliceStr.Length - 3).Split(new[] { ".." }, StringSplitOptions.None);
            valueSliceStart = int.Parse(parts[0]);
            if (parts[1].Length > 0)
                valueSliceEnd = int.Parse(parts[1]);
            path = valueSliceMatchRange.Groups[1].Value;
            return (path, valueSliceStart, valueSliceEnd);
        }

        // Try /[N] pattern (single index on string value)
        var valueSliceMatchSingle = Regex.Match(path, @"^(.+)/\[(\d+)\]$");
        if (valueSliceMatchSingle.Success)
        {
            valueSliceStart = int.Parse(valueSliceMatchSingle.Groups[2].Value);
            valueSliceEnd = valueSliceStart + 1;
            path = valueSliceMatchSingle.Groups[1].Value;
            return (path, valueSliceStart, valueSliceEnd);
        }

        // No slicing operator found
        return (path, null, null);
    }

    /// <summary>
    /// Evaluates a JSON-style path expression on a protobuf message.
    /// Returns a list of string values (usually one, but multiple for array iteration with []).
    /// Examples:
    ///   ".id" -> gets the id field -> ["value"]
    ///   ".id/[..4]" -> gets the id field, splits by "/", takes first 4 parts -> ["t/100/s/200"]
    ///   ".id/[1..3]" -> gets the id field, splits by "/", takes parts from index 1 to 3 -> ["100/s"]
    ///   ".id/[4]" -> gets the id field, splits by "/", takes part at index 4 -> ["value"]
    ///   ".single.linked_item.item_id" -> navigates through nested messages -> ["itemid"]
    ///   ".multi_item.linked_items[].item_id" -> iterates through repeated field, deduplicates -> ["item1", "item2"]
    ///   ".multi_item.linked_items[0].item_id" -> gets first item -> ["item1"]
    ///   ".multi_item.linked_items[1..3].item_id" -> slices array and iterates -> ["item2", "item3"]
    /// 
    /// Returns null if:
    ///   - The path is invalid
    ///   - A required field is missing
    ///   - An array iteration is attempted on an empty array
    /// </summary>
    private static IEnumerable<string> EvaluateJsonPath(string path, object entity)
    {
        if (!path.StartsWith('.'))
            return Enumerable.Empty<string>();

        path = path.Substring(1); // Remove leading dot

        // Extract value slicing operators (on "/" separated string values)
        var (pathWithoutSlicing, valueSliceStart, valueSliceEnd) = ExtractValueSlicing(path);
        path = pathWithoutSlicing;

        // Split path into segments, but be careful about dots inside brackets
        // We need to split on dots that are NOT inside [...] brackets
        var segments = SplitPathIntoSegments(path);

        // Find the first segment with array indexing/iteration operator
        int arrayOpSegmentIndex = -1;
        for (int i = 0; i < segments.Length; i++)
        {
            if (segments[i].Contains("["))
            {
                arrayOpSegmentIndex = i;
                break; // Only one array op supported per path
            }
        }

        // If we found array operator, process up to it specially
        if (arrayOpSegmentIndex >= 0)
        {
            // First, navigate to the array field
            object? current = entity;

            for (int i = 0; i < arrayOpSegmentIndex; i++)
            {
                var segment = segments[i];

                if (current == null)
                    return Enumerable.Empty<string>();

                // Regular field access - convert snake_case to PascalCase
                var pascalSegment = ConvertSnakeCaseToPascalCase(segment);
                var property = current.GetType().GetProperty(pascalSegment);
                if (property == null)
                    return Enumerable.Empty<string>();

                current = property.GetValue(current);
                if (current == null)
                    return Enumerable.Empty<string>();
            }

            // Now process the array segment
            var arraySegment = segments[arrayOpSegmentIndex];
            var bracketIndex = arraySegment.IndexOf('[');
            var fieldName = arraySegment.Substring(0, bracketIndex);
            var indexSpec = arraySegment.Substring(bracketIndex); // e.g., "[0]", "[]", "[1..3]"

            // Convert snake_case to PascalCase for protobuf property lookup
            var pascalFieldName = ConvertSnakeCaseToPascalCase(fieldName);
            var arrayFieldProperty = current.GetType().GetProperty(pascalFieldName);
            if (arrayFieldProperty == null)
                return Enumerable.Empty<string>();

            var fieldValue = arrayFieldProperty.GetValue(current);
            if (fieldValue == null)
                return Enumerable.Empty<string>();

            // Get the enumerable
            if (!(fieldValue is System.Collections.IEnumerable enumerable))
                return Enumerable.Empty<string>();

            // Parse the index specification
            int? arrayIndexStart = null;
            int? arrayIndexEnd = null;

            if (indexSpec == "[]")
            {
                arrayIndexStart = 0;
                arrayIndexEnd = int.MaxValue; // All items
            }
            else if (indexSpec.StartsWith("[", StringComparison.Ordinal) && indexSpec.EndsWith("]", StringComparison.Ordinal))
            {
                var inner = indexSpec.Substring(1, indexSpec.Length - 2);
                if (inner.Contains(".."))
                {
                    var parts = inner.Split(new[] { ".." }, StringSplitOptions.None);
                    if (parts[0].Length > 0)
                        arrayIndexStart = int.Parse(parts[0]);
                    else
                        arrayIndexStart = 0;
                    if (parts[1].Length > 0)
                        arrayIndexEnd = int.Parse(parts[1]);
                    else
                        arrayIndexEnd = int.MaxValue; // To the end
                }
                else
                {
                    // Single index [N]
                    arrayIndexStart = int.Parse(inner);
                    arrayIndexEnd = arrayIndexStart + 1;
                }
            }

            // Collect items from the enumerable based on index spec
            var items = new List<object>();
            int itemIndex = 0;
            foreach (var item in enumerable)
            {
                if (item != null && arrayIndexStart.HasValue)
                {
                    if (itemIndex >= arrayIndexStart && itemIndex < arrayIndexEnd)
                    {
                        items.Add(item);
                    }
                }
                itemIndex++;
            }

            if (items.Count == 0)
                return Enumerable.Empty<string>();

            // Now process remaining path segments on each collected item
            var results = new List<string>();

            foreach (var item in items)
            {
                var remainingSegments = segments.Skip(arrayOpSegmentIndex + 1).ToList();
                IEnumerable<string>? itemResults = null;

                if (remainingSegments.Count == 0)
                {
                    // No more segments - just convert item to string
                    var itemStr = item?.ToString();
                    if (!string.IsNullOrEmpty(itemStr))
                    {
                        itemResults = new List<string> { itemStr };
                    }
                }
                else
                {
                    // Recursively process remaining path on this item
                    var remainingPath = string.Join(".", remainingSegments);
                    itemResults = EvaluateJsonPath("." + remainingPath, item);
                }

                if (itemResults != null)
                {
                    results.AddRange(itemResults);
                }
            }

            if (results.Count == 0)
                return Enumerable.Empty<string>();

            // Apply value slicing if specified
            if (valueSliceStart.HasValue || valueSliceEnd.HasValue)
            {
                var slicedResults = new List<string>();
                foreach (var result in results)
                {
                    var partsList = result.Split('/');
                    var start = valueSliceStart ?? 0;
                    var end = valueSliceEnd ?? partsList.Length;

                    if (start >= partsList.Length)
                        continue;

                    end = Math.Min(end, partsList.Length);

                    if (start >= end)
                        continue; // Empty slice - skip

                    var sliced = string.Join("/", partsList.Skip(start).Take(end - start));
                    if (!string.IsNullOrEmpty(sliced))
                    {
                        slicedResults.Add(sliced);
                    }
                }
                return slicedResults;
            }

            return results;
        }

        // No array operator - normal path navigation
        object? currentObj = entity;
        for (int i = 0; i < segments.Length; i++)
        {
            var segment = segments[i];

            if (currentObj == null)
                return Enumerable.Empty<string>();

            // Regular field access - convert snake_case to PascalCase
            var pascalSegment = ConvertSnakeCaseToPascalCase(segment);
            var prop = currentObj.GetType().GetProperty(pascalSegment);
            if (prop == null)
                return Enumerable.Empty<string>();

            currentObj = prop.GetValue(currentObj);
            if (currentObj == null)
                return Enumerable.Empty<string>();
        }

        // Single value - convert to string and apply value slicing
        var singleResult = currentObj?.ToString();
        if (string.IsNullOrEmpty(singleResult))
            return Enumerable.Empty<string>();

        // Apply value slicing if specified
        if (valueSliceStart.HasValue || valueSliceEnd.HasValue)
        {
            var partsList = singleResult.Split('/');
            var start = valueSliceStart ?? 0;
            var end = valueSliceEnd ?? partsList.Length;

            // Handle out-of-bounds: if start is beyond the array, skip this alias pattern
            if (start >= partsList.Length)
                return Enumerable.Empty<string>();

            // Clamp end to array length
            end = Math.Min(end, partsList.Length);

            // Handle invalid range - if start >= end, return empty
            if (start >= end)
                return Enumerable.Empty<string>();
            
            singleResult = string.Join("/", partsList.Skip(start).Take(end - start));
        }

        return string.IsNullOrEmpty(singleResult) ? Enumerable.Empty<string>() : new[] { singleResult };
    }

    /// <summary>
    /// Converts snake_case to PascalCase.
    /// Examples: "item_id" -> "ItemId", "esl" -> "Esl", "esls" -> "Esls"
    /// </summary>
    private static string ConvertSnakeCaseToPascalCase(string snakeCase)
    {
        var parts = snakeCase.Split('_');
        return string.Concat(parts.Select(p => char.ToUpperInvariant(p[0]) + p.Substring(1)));
    }

    /// <summary>
    /// TEST HELPER: Exposes ExtractValueSlicing for unit testing.
    /// </summary>
    internal static (string path, int? sliceStart, int? sliceEnd) TestExtractValueSlicing(string path)
    {
        return ExtractValueSlicing(path);
    }
}
