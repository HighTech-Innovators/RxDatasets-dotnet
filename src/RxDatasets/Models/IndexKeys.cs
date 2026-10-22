using System.Collections.Concurrent;

namespace RxDatasets.Models;

/// <summary>
/// The one place that knows how an entity name maps onto the index tree, and back.
/// </summary>
/// <remarks>
/// A name is split on '/' into n segments. The last segments form the leaf: all n when n &lt;= 3, otherwise the
/// last 2 when n is even and the last 3 when n is odd. The leading segments are <c>collection/id</c> pairs, one tree
/// node each. A leaf is <c>V</c> (only n = 1), <c>C/V</c> or <c>C/V/L</c>; it is stored under its template
/// (<c>{}</c>, <c>C/{}</c> or <c>C/{}/L</c>) and its variable <c>V</c>, so the fixed parts are stored once per node.
/// <example>
/// <c>t/T/s/S/items/I/values</c> → nodes <c>(t, T)</c>, <c>(s, S)</c>; template <c>items/{}/values</c>; variable <c>I</c>.
/// </example>
/// No id patterns are registered: the rule is pattern-agnostic, and the template is opaque to everything else.
/// </remarks>
public static class IndexKeys
{
    public const string Placeholder = "{}";

    // ponytail: one process-wide pool, capped so that unusual names can't grow it without bound; past a cap, nothing
    // new of that kind is shared. Templates and variables have their own caps, so names with a unique trailing literal
    // can't fill the pool with templates and stop the page numbers being shared. A per-dataset pool if datasets with
    // odd names ever crowd out the real templates.
    private const int MaxSharedTemplates = 1_000;
    private const int MaxSharedVariables = 10_000;
    private const int MaxSharedVariableLength = 3;
    private static readonly ConcurrentDictionary<string, string> Shared = new(StringComparer.Ordinal);
    private static readonly ConcurrentDictionary<string, string>.AlternateLookup<ReadOnlySpan<char>> SharedBySpan =
        Shared.GetAlternateLookup<ReadOnlySpan<char>>();
    private static int _sharedTemplates; // ConcurrentDictionary.Count takes all its locks
    private static int _sharedVariables;

    /// <summary>The shared instance of a template, so the nodes that use it don't each hold a copy.</summary>
    internal static string ShareTemplate(string template) =>
        Shared.TryGetValue(template, out var shared) ? shared : AddShared(template, ref _sharedTemplates, MaxSharedTemplates);

    /// <summary>
    /// The shared instance of a short variable, like a page number, which repeats under many nodes. Longer variables
    /// are ids, mostly unique, and are returned as they are.
    /// </summary>
    internal static ReadOnlyMemory<char> ShareVariable(ReadOnlyMemory<char> variable)
    {
        if (variable.Length > MaxSharedVariableLength)
            return variable;
        if (SharedBySpan.TryGetValue(variable.Span, out var shared))
            return shared.AsMemory();
        return Volatile.Read(ref _sharedVariables) < MaxSharedVariables
            ? AddShared(variable.ToString(), ref _sharedVariables, MaxSharedVariables).AsMemory()
            : variable;
    }

    private static string AddShared(string text, ref int count, int max)
    {
        if (Volatile.Read(ref count) >= max)
            return text;
        if (!Shared.TryAdd(text, text))
            return Shared[text];

        Interlocked.Increment(ref count);
        return text;
    }

    /// <summary>A name split into its node path (collection, id, collection, id, ...), template and variable.</summary>
    public readonly record struct Key(string[] Path, string Template, string Variable);

    public static Key Split(string name)
    {
        var segments = name.Split('/');
        int leafSegments = LeafSegmentCount(segments.Length);
        int nodeSegments = segments.Length - leafSegments;
        string template = leafSegments switch
        {
            1 => Placeholder,
            2 => segments[nodeSegments] + "/" + Placeholder,
            _ => segments[nodeSegments] + "/" + Placeholder + "/" + segments[^1]
        };
        string variable = leafSegments == 1 ? segments[0] : segments[nodeSegments + 1];
        return new Key(segments[..nodeSegments], template, variable);
    }

    /// <summary>
    /// The number of trailing segments that form the leaf of a name with <paramref name="segmentCount"/> segments.
    /// </summary>
    public static int LeafSegmentCount(int segmentCount) =>
        segmentCount <= 3 ? segmentCount : segmentCount % 2 == 0 ? 2 : 3;

    /// <summary>The leaf part of a name: the template with its placeholder replaced by <paramref name="variable"/>.</summary>
    public static string LeafName(string template, ReadOnlySpan<char> variable)
    {
        int slash = template.IndexOf('/');
        return slash < 0
            ? variable.ToString()
            : string.Concat(template.AsSpan(0, slash + 1), variable, template.AsSpan(slash + 1 + Placeholder.Length));
    }

    /// <summary>
    /// Splits a template into its collection and trailing literal, and returns its segment count: 1 for <c>{}</c>,
    /// 2 for <c>C/{}</c>, 3 for <c>C/{}/L</c>. A collection never contains '/', so the first '/' ends it.
    /// </summary>
    public static int ParseTemplate(string template, out ReadOnlySpan<char> collection, out ReadOnlySpan<char> literal)
    {
        int slash = template.IndexOf('/');
        if (slash < 0)
        {
            collection = literal = default;
            return 1;
        }

        collection = template.AsSpan(0, slash);
        int literalStart = slash + 1 + Placeholder.Length + 1;
        if (literalStart > template.Length)
        {
            literal = default;
            return 2;
        }

        literal = template.AsSpan(literalStart);
        return 3;
    }

    /// <summary>Whether <paramref name="template"/> is the template of a leaf with these parts, without building it.</summary>
    internal static bool TemplateMatches(string template, int leafSegments, ReadOnlySpan<char> collection,
        ReadOnlySpan<char> literal)
    {
        int segments = ParseTemplate(template, out var templateCollection, out var templateLiteral);
        return segments == leafSegments
               && templateCollection.SequenceEqual(collection)
               && templateLiteral.SequenceEqual(literal);
    }

    /// <summary>Joins a node path and a leaf name, without a leading '/' at the root.</summary>
    public static string Join(string path, string leafName) =>
        path.Length == 0 ? leafName : string.Concat(path, "/", leafName);

    /// <summary>Joins a node path and a child's collection and id, without a leading '/' at the root.</summary>
    public static string Join(string path, ReadOnlySpan<char> collection, ReadOnlySpan<char> id) =>
        path.Length == 0 ? string.Concat(collection, "/", id) : $"{path}/{collection}/{id}";

    /// <summary>Counts the '/'-separated segments of <paramref name="name"/>.</summary>
    internal static int CountSegments(ReadOnlySpan<char> name) => name.Count('/') + 1;

    /// <summary>
    /// Returns the segment that starts at <paramref name="position"/>, and moves the position past it and its '/'.
    /// </summary>
    public static ReadOnlyMemory<char> NextSegment(ReadOnlyMemory<char> name, ref int position)
    {
        int length = name.Span[position..].IndexOf('/');
        if (length < 0)
            length = name.Length - position;

        var segment = name.Slice(position, length);
        position += length + 1;
        return segment;
    }
}

/// <summary>Compares <see cref="ReadOnlyMemory{T}"/> keys by content, so a slice of a name finds a stored key.</summary>
public sealed class MemoryKeyComparer : IEqualityComparer<ReadOnlyMemory<char>>
{
    public static readonly MemoryKeyComparer Instance = new();

    public bool Equals(ReadOnlyMemory<char> x, ReadOnlyMemory<char> y) => x.Span.SequenceEqual(y.Span);

    public int GetHashCode(ReadOnlyMemory<char> obj) => string.GetHashCode(obj.Span);
}
