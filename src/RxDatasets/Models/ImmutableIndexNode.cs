using System.Collections.Immutable;

namespace RxDatasets.Models;

/// <summary>
/// A value stored in an <see cref="ImmutableIndexNode{TValue}"/> that can be present or not. Values that don't
/// implement it always count as present.
/// </summary>
public interface IIndexValue
{
    /// <summary>Whether the value counts as present, for <see cref="ImmutableIndexNode{TValue}.LiveCount"/>.</summary>
    bool IsLive { get; }
}

/// <summary>
/// An immutable node of the hierarchical index. See <see cref="IndexKeys"/> for how names map onto it.
/// </summary>
/// <remarks>
/// <see cref="Children"/>: collection → (id → child node). <see cref="Leaves"/>: template → (variable → value).
/// Keys are <see cref="ReadOnlyMemory{T}"/> over whole strings, so a lookup can pass a slice of the name it was
/// asked for, without allocating. Updates go through a <see cref="Builder"/>, which copies only the touched path;
/// readers keep the nodes they hold.
/// </remarks>
public sealed class ImmutableIndexNode<TValue> where TValue : class
{
    private static readonly
        ImmutableDictionary<ReadOnlyMemory<char>, ImmutableDictionary<ReadOnlyMemory<char>, ImmutableIndexNode<TValue>>>
        EmptyChildren =
        ImmutableDictionary<ReadOnlyMemory<char>, ImmutableDictionary<ReadOnlyMemory<char>, ImmutableIndexNode<TValue>>>.Empty
            .WithComparers(MemoryKeyComparer.Instance);

    public static readonly ImmutableIndexNode<TValue> Empty = new(EmptyChildren, [], 0);

    private ImmutableIndexNode(
        ImmutableDictionary<ReadOnlyMemory<char>, ImmutableDictionary<ReadOnlyMemory<char>, ImmutableIndexNode<TValue>>> children,
        ImmutableArray<KeyValuePair<string, ImmutableDictionary<ReadOnlyMemory<char>, TValue>>> leaves,
        int liveCount)
    {
        Children = children;
        Leaves = leaves;
        LiveCount = liveCount;
    }

    /// <summary>Child nodes, by collection and then by id.</summary>
    public ImmutableDictionary<ReadOnlyMemory<char>, ImmutableDictionary<ReadOnlyMemory<char>, ImmutableIndexNode<TValue>>>
        Children { get; }

    /// <summary>
    /// Values stored at this node, by template and then by variable. An array, not a dictionary: a node has a handful
    /// of templates, and most nodes hold only a few values, so a dictionary per node would cost more than it saves.
    /// </summary>
    public ImmutableArray<KeyValuePair<string, ImmutableDictionary<ReadOnlyMemory<char>, TValue>>> Leaves { get; }

    /// <summary>The number of live values in this node and all its descendants.</summary>
    public int LiveCount { get; }

    public Builder ToBuilder() => new(this);

    private static bool IsLive(TValue value) => value is not IIndexValue { IsLive: false };

    /// <summary>Looks up a value by its full name, without allocating.</summary>
    public bool TryGetValue(string name, out TValue value)
    {
        value = null!;
        var memory = name.AsMemory();
        int segments = IndexKeys.CountSegments(memory.Span);
        int leafSegments = IndexKeys.LeafSegmentCount(segments);
        int position = 0;
        var node = this;
        for (int pair = 0; pair < (segments - leafSegments) / 2; pair++)
        {
            var collection = IndexKeys.NextSegment(memory, ref position);
            var id = IndexKeys.NextSegment(memory, ref position);
            if (!node.Children.TryGetValue(collection, out var group) || !group.TryGetValue(id, out node))
                return false;
        }

        ReadOnlyMemory<char> leafCollection = default, variable, literal = default;
        if (leafSegments == 1)
        {
            variable = IndexKeys.NextSegment(memory, ref position);
        }
        else
        {
            leafCollection = IndexKeys.NextSegment(memory, ref position);
            variable = IndexKeys.NextSegment(memory, ref position);
            if (leafSegments == 3)
                literal = IndexKeys.NextSegment(memory, ref position);
        }

        var leafGroup = node.FindLeafGroup(leafSegments, leafCollection.Span, literal.Span);
        return leafGroup != null && leafGroup.TryGetValue(variable, out value!);
    }

    // ponytail: linear scan over this node's templates; real id patterns give a handful per node.
    private int IndexOfTemplate(string template)
    {
        for (int i = 0; i < Leaves.Length; i++)
        {
            if (Leaves[i].Key == template)
                return i;
        }

        return -1;
    }

    private ImmutableDictionary<ReadOnlyMemory<char>, TValue>? FindLeafGroup(int leafSegments, ReadOnlySpan<char> collection,
        ReadOnlySpan<char> literal)
    {
        foreach (var (template, group) in Leaves)
        {
            if (IndexKeys.TemplateMatches(template, leafSegments, collection, literal))
                return group;
        }

        return null;
    }

    /// <summary>
    /// Enumerates every value whose name equals <paramref name="prefix"/> or starts with <paramref name="prefix"/>
    /// followed by '/', with its full name. A null or empty prefix, or "/", enumerates everything. Only the names of values
    /// that pass <paramref name="where"/> (default: all, deleted ones included) are built.
    /// </summary>
    public IEnumerable<KeyValuePair<string, TValue>> EnumerateAll(string? prefix = "", Func<TValue, bool>? where = null)
    {
        foreach (var match in Match(prefix))
        {
            if (match.Node == null)
            {
                if (where == null || where(match.Value!))
                {
                    string leafName = IndexKeys.LeafName(match.Template!, match.Variable.Span);
                    yield return new(IndexKeys.Join(match.Path, leafName), match.Value!);
                }
                continue;
            }

            foreach (var entry in match.Node.EnumerateSubtree(match.Path, where))
                yield return entry;
        }
    }

    /// <summary>Counts the live values that <see cref="EnumerateAll"/> would return for <paramref name="prefix"/>.</summary>
    public int CountLive(string? prefix = "")
    {
        int count = 0;
        foreach (var match in Match(prefix))
            count += match.Node?.LiveCount ?? (IsLive(match.Value!) ? 1 : 0);
        return count;
    }

    private IEnumerable<KeyValuePair<string, TValue>> EnumerateSubtree(string path, Func<TValue, bool>? where)
    {
        foreach (var (template, group) in Leaves)
        {
            foreach (var (variable, value) in group)
            {
                if (where == null || where(value))
                    yield return new(IndexKeys.Join(path, IndexKeys.LeafName(template, variable.Span)), value);
            }
        }

        foreach (var (collection, group) in Children)
        {
            foreach (var (id, child) in group)
            {
                foreach (var entry in child.EnumerateSubtree(IndexKeys.Join(path, collection.Span, id.Span), where))
                    yield return entry;
            }
        }
    }

    /// <summary>A whole subtree (<see cref="Node"/> set), or a single value, under a prefix.</summary>
    private readonly record struct PrefixMatch(
        string Path,
        ImmutableIndexNode<TValue>? Node,
        string? Template,
        ReadOnlyMemory<char> Variable,
        TValue? Value);

    private IEnumerable<PrefixMatch> Match(string? prefix)
    {
        prefix = prefix?.TrimEnd('/') ?? "";
        return prefix.Length == 0
            ? [new PrefixMatch("", this, null, default, null)]
            : Match("", prefix.AsMemory());
    }

    /// <summary>
    /// The matches for the <paramref name="rest"/> of a prefix below this node. Every key is an exact lookup: a leaf
    /// matches when the rest covers its first segments, a child group when the rest is its collection, and a child
    /// when the rest starts with its collection and id.
    /// </summary>
    private IEnumerable<PrefixMatch> Match(string path, ReadOnlyMemory<char> rest)
    {
        int segments = IndexKeys.CountSegments(rest.Span);
        int position = 0;
        var first = IndexKeys.NextSegment(rest, ref position);
        var second = segments > 1 ? IndexKeys.NextSegment(rest, ref position) : default;
        int afterSecond = position;

        if (segments <= 3)
        {
            var third = segments > 2 ? IndexKeys.NextSegment(rest, ref position) : default;
            foreach (var (template, group) in Leaves)
            {
                int leafSegments = IndexKeys.ParseTemplate(template, out var collection, out var literal);
                if (segments > leafSegments)
                    continue;

                if (leafSegments == 1)
                {
                    if (group.TryGetValue(first, out var value))
                        yield return new PrefixMatch(path, null, template, first, value);
                    continue;
                }

                if (!collection.SequenceEqual(first.Span))
                    continue;

                if (segments == 1)
                {
                    foreach (var (variable, value) in group)
                        yield return new PrefixMatch(path, null, template, variable, value);
                }
                else if ((segments == 2 || literal.SequenceEqual(third.Span)) && group.TryGetValue(second, out var value))
                {
                    yield return new PrefixMatch(path, null, template, second, value);
                }
            }
        }

        if (!Children.TryGetValue(first, out var children))
            yield break;

        if (segments == 1)
        {
            foreach (var (id, child) in children)
                yield return new PrefixMatch(IndexKeys.Join(path, first.Span, id.Span), child, null, default, null);
            yield break;
        }

        if (!children.TryGetValue(second, out var node))
            yield break;

        string childPath = IndexKeys.Join(path, first.Span, second.Span);
        if (segments == 2)
        {
            yield return new PrefixMatch(childPath, node, null, default, null);
            yield break;
        }

        foreach (var match in node.Match(childPath, rest[afterSecond..]))
            yield return match;
    }

    /// <summary>
    /// A mutable view of a node, frozen with <see cref="ToImmutable"/>. Only touched groups and children are
    /// copied; the rest is shared with the original node.
    /// </summary>
    /// <remarks>Keys passed in are stored, so they must be whole strings, never slices of a longer name.</remarks>
    public sealed class Builder
    {
        private readonly ImmutableIndexNode<TValue> _original;
        private Dictionary<string, ImmutableDictionary<ReadOnlyMemory<char>, TValue>.Builder>? _leaves;
        private Dictionary<ReadOnlyMemory<char>, Dictionary<ReadOnlyMemory<char>, Builder>>? _children;
        private int _leafLiveCountChange;

        internal Builder(ImmutableIndexNode<TValue> original)
        {
            _original = original;
        }

        public Builder Child(ReadOnlyMemory<char> collection, ReadOnlyMemory<char> id)
        {
            if (!(_children ??= new(MemoryKeyComparer.Instance)).TryGetValue(collection, out var group))
                _children[collection] = group = new Dictionary<ReadOnlyMemory<char>, Builder>(MemoryKeyComparer.Instance);

            if (!group.TryGetValue(id, out var child))
            {
                var original = _original.Children.TryGetValue(collection, out var originalGroup)
                               && originalGroup.TryGetValue(id, out var originalChild)
                    ? originalChild
                    : Empty;
                group[id] = child = new Builder(original);
            }

            return child;
        }

        public TValue? GetLeaf(string template, ReadOnlyMemory<char> variable)
        {
            if (_leaves != null && _leaves.TryGetValue(template, out var group))
                return group.GetValueOrDefault(variable);

            int index = _original.IndexOfTemplate(template);
            return index < 0 ? null : _original.Leaves[index].Value.GetValueOrDefault(variable);
        }

        public void SetLeaf(string template, ReadOnlyMemory<char> variable, TValue value)
        {
            var group = GetLeafGroupBuilder(template);
            if (!group.TryGetValue(variable, out var previous))
                variable = IndexKeys.ShareVariable(variable);
            else if (IsLive(previous))
                _leafLiveCountChange--;
            if (IsLive(value))
                _leafLiveCountChange++;
            group[variable] = value;
        }

        /// <summary>Removes a value; an emptied group or node stays, empty.</summary>
        public void RemoveLeaf(string template, ReadOnlyMemory<char> variable)
        {
            if (GetLeaf(template, variable) is not { } previous)
                return;

            GetLeafGroupBuilder(template).Remove(variable);
            if (IsLive(previous))
                _leafLiveCountChange--;
        }

        private ImmutableDictionary<ReadOnlyMemory<char>, TValue>.Builder GetLeafGroupBuilder(string template)
        {
            if (!(_leaves ??= new(StringComparer.Ordinal)).TryGetValue(template, out var group))
            {
                int index = _original.IndexOfTemplate(template);
                group = index >= 0
                    ? _original.Leaves[index].Value.ToBuilder()
                    : ImmutableDictionary.CreateBuilder<ReadOnlyMemory<char>, TValue>(MemoryKeyComparer.Instance);
                _leaves[index >= 0 ? _original.Leaves[index].Key : IndexKeys.ShareTemplate(template)] = group;
            }

            return group;
        }

        /// <summary>Sets the value of a full name, creating the nodes on its path.</summary>
        public void Set(string name, TValue value)
        {
            var (node, template, variable) = Locate(name);
            node.SetLeaf(template, variable, value);
        }

        /// <summary>The value of a full name, or null.</summary>
        public TValue? Get(string name)
        {
            var (node, template, variable) = Locate(name);
            return node.GetLeaf(template, variable);
        }

        /// <summary>Removes the value of a full name, if it is there.</summary>
        public void Remove(string name)
        {
            var (node, template, variable) = Locate(name);
            node.RemoveLeaf(template, variable);
        }

        // ponytail: walks the path through Child, which marks it as touched even for a lookup that finds nothing;
        // ToImmutable then copies those nodes unchanged. A read-only walk if that ever shows up in a profile.
        private (Builder Node, string Template, ReadOnlyMemory<char> Variable) Locate(string name)
        {
            var key = IndexKeys.Split(name);
            var node = this;
            for (int i = 0; i < key.Path.Length; i += 2)
                node = node.Child(key.Path[i].AsMemory(), key.Path[i + 1].AsMemory());
            return (node, key.Template, key.Variable.AsMemory());
        }

        public ImmutableIndexNode<TValue> ToImmutable()
        {
            if (_leaves == null && _children == null)
                return _original;

            int liveCount = _original.LiveCount + _leafLiveCountChange;

            var leaves = _original.Leaves;
            if (_leaves != null)
            {
                var leavesBuilder = leaves.ToBuilder();
                foreach (var (template, group) in _leaves)
                {
                    int index = _original.IndexOfTemplate(template);
                    if (index >= 0)
                        leavesBuilder[index] = new(template, group.ToImmutable());
                    else
                        leavesBuilder.Add(new(template, group.ToImmutable()));
                }

                leaves = leavesBuilder.ToImmutable();
            }

            var children = _original.Children;
            if (_children != null)
            {
                var childrenBuilder = children.ToBuilder();
                foreach (var (collection, group) in _children)
                {
                    var groupBuilder = childrenBuilder.TryGetValue(collection, out var originalGroup)
                        ? originalGroup.ToBuilder()
                        : ImmutableDictionary.CreateBuilder<ReadOnlyMemory<char>, ImmutableIndexNode<TValue>>(
                            MemoryKeyComparer.Instance);
                    foreach (var (id, childBuilder) in group)
                    {
                        var child = childBuilder.ToImmutable();
                        liveCount += child.LiveCount - childBuilder._original.LiveCount;
                        groupBuilder[id] = child;
                    }

                    childrenBuilder[collection] = groupBuilder.ToImmutable();
                }

                children = childrenBuilder.ToImmutable();
            }

            return new ImmutableIndexNode<TValue>(children, leaves, liveCount);
        }
    }
}
