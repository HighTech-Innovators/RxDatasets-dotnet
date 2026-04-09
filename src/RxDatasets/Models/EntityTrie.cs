using System.Collections;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;

namespace RxDatasets.Models;

/// <summary>
/// An immutable trie (prefix tree) that stores entities keyed by '/'-separated hierarchical names.
/// Uses <see cref="IndexEntity"/> — a slim record without version-history arrays.
/// Deleted entities are NOT stored in the trie; presence means alive.
/// </summary>
public sealed class EntityTrie : IReadOnlyDictionary<string, IndexEntity>
{
    internal static readonly EntityTrie Empty = new(TrieNode.EmptyNode, 0);

    private readonly TrieNode _root;

    public int Count { get; }

    private EntityTrie(TrieNode root, int count)
    {
        _root = root;
        Count = count;
    }

    public IndexEntity this[string key]
    {
        get
        {
            if (TryGetValue(key, out var value))
                return value;
            throw new KeyNotFoundException($"Entity '{key}' not found");
        }
    }

    public bool TryGetValue(string name, [MaybeNullWhen(false)] out IndexEntity value)
    {
        var node = _root;
        int start = 0;

        while (start <= name.Length)
        {
            int end = name.IndexOf('/', start);
            if (end == -1) end = name.Length;

            var segment = name.Substring(start, end - start);
            if (!node.Children.TryGetValue(segment, out var child))
            {
                value = default;
                return false;
            }
            node = child;
            start = end + 1;
        }

        if (node.Value is not null)
        {
            value = node.Value;
            return true;
        }

        value = default;
        return false;
    }

    public bool ContainsKey(string key) => TryGetValue(key, out _);

    /// <summary>
    /// Returns a new trie with the given entity set (added or replaced).
    /// </summary>
    public EntityTrie SetItem(string name, IndexEntity value)
    {
        var segments = name.Split('/');
        bool wasNew = !ContainsKey(name);
        var newRoot = SetItemRecursive(_root, segments, 0, value);
        return new EntityTrie(newRoot, wasNew ? Count + 1 : Count);
    }

    private static TrieNode SetItemRecursive(TrieNode node, string[] segments, int depth, IndexEntity value)
    {
        if (depth == segments.Length)
        {
            return new TrieNode(node.Children, value);
        }

        var segment = segments[depth];
        var child = node.Children.GetValueOrDefault(segment, TrieNode.EmptyNode);
        var newChild = SetItemRecursive(child, segments, depth + 1, value);
        return new TrieNode(node.Children.SetItem(segment, newChild), node.Value);
    }

    /// <summary>
    /// Returns a new trie with the given entity removed.
    /// If the entity is not found, the same trie is returned.
    /// Prunes empty intermediate nodes that have no value and no children.
    /// </summary>
    public EntityTrie RemoveItem(string name)
    {
        if (!ContainsKey(name))
            return this;

        var segments = name.Split('/');
        var newRoot = RemoveItemRecursive(_root, segments, 0);
        return new EntityTrie(newRoot ?? TrieNode.EmptyNode, Count - 1);
    }

    private static TrieNode? RemoveItemRecursive(TrieNode node, string[] segments, int depth)
    {
        if (depth == segments.Length)
        {
            // This is the target node — remove value
            if (node.Children.Count == 0)
                return null; // Prune: no children, no value
            return new TrieNode(node.Children, null);
        }

        var segment = segments[depth];
        if (!node.Children.TryGetValue(segment, out var child))
            return node;

        var newChild = RemoveItemRecursive(child, segments, depth + 1);
        ImmutableDictionary<string, TrieNode> newChildren;
        if (newChild == null)
            newChildren = node.Children.Remove(segment);
        else
            newChildren = node.Children.SetItem(segment, newChild);

        if (newChildren.Count == 0 && node.Value is null)
            return null; // Prune
        return new TrieNode(newChildren, node.Value);
    }

    /// <summary>
    /// Enumerates all entity names that are descendants of the given parent prefix.
    /// Parent should NOT end with '/'.
    /// All entities in the trie are alive (no deleted-entity filter needed).
    /// </summary>
    public IEnumerable<string> ListDescendants(string parent)
    {
        var node = NavigateTo(parent);
        if (node is null)
            yield break;

        var prefix = parent.Length > 0 ? parent + "/" : "";
        foreach (var (name, _) in EnumerateSubtree(node, prefix))
        {
            yield return name;
        }
    }

    /// <summary>
    /// Counts descendants under the given parent.
    /// All entities in the trie are alive (no deleted-entity filter needed).
    /// </summary>
    public int CountDescendants(string parent)
    {
        var node = NavigateTo(parent);
        if (node is null)
            return 0;

        return CountLeaves(node);
    }

    private static int CountLeaves(TrieNode node)
    {
        int count = node.Value is not null ? 1 : 0;

        foreach (var child in node.Children.Values)
            count += CountLeaves(child);

        return count;
    }

    private TrieNode? NavigateTo(string path)
    {
        if (string.IsNullOrEmpty(path))
            return _root;

        var node = _root;
        int start = 0;

        while (start <= path.Length)
        {
            int end = path.IndexOf('/', start);
            if (end == -1) end = path.Length;

            var segment = path.Substring(start, end - start);
            if (!node.Children.TryGetValue(segment, out var child))
                return null;
            node = child;
            start = end + 1;
        }

        return node;
    }

    private static IEnumerable<(string Name, IndexEntity Entity)> EnumerateSubtree(TrieNode node, string prefix)
    {
        foreach (var (segment, child) in node.Children)
        {
            var childPath = prefix + segment;

            if (child.Value is not null)
                yield return (childPath, child.Value);

            foreach (var descendant in EnumerateSubtree(child, childPath + "/"))
                yield return descendant;
        }
    }

    #region IReadOnlyDictionary / IEnumerable

    public IEnumerable<string> Keys
    {
        get
        {
            foreach (var kvp in this)
                yield return kvp.Key;
        }
    }

    public IEnumerable<IndexEntity> Values
    {
        get
        {
            foreach (var kvp in this)
                yield return kvp.Value;
        }
    }

    public IEnumerator<KeyValuePair<string, IndexEntity>> GetEnumerator()
    {
        return EnumerateAll(_root, "").GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    private static IEnumerable<KeyValuePair<string, IndexEntity>> EnumerateAll(TrieNode node, string prefix)
    {
        if (node.Value is not null)
            yield return new KeyValuePair<string, IndexEntity>(prefix, node.Value);

        foreach (var (segment, child) in node.Children)
        {
            var childPath = prefix.Length == 0 ? segment : prefix + "/" + segment;
            foreach (var kvp in EnumerateAll(child, childPath))
                yield return kvp;
        }
    }

    #endregion

    /// <summary>
    /// Creates an EntityTrie from a dictionary of entity names to index entities.
    /// </summary>
    public static EntityTrie FromDictionary(IEnumerable<KeyValuePair<string, IndexEntity>> entries)
    {
        var builder = new Builder();
        foreach (var kvp in entries)
            builder.Add(kvp.Key, kvp.Value);
        return builder.Build();
    }

    #region Builder (for bulk construction from proto)

    public sealed class Builder
    {
        private readonly MutableTrieNode _root = new();
        private int _count;

        public void Add(string name, IndexEntity value)
        {
            var segments = name.Split('/');
            var node = _root;

            for (int i = 0; i < segments.Length; i++)
            {
                if (!node.Children.TryGetValue(segments[i], out var child))
                {
                    child = new MutableTrieNode();
                    node.Children[segments[i]] = child;
                }
                node = child;
            }

            node.Value = value;
            _count++;
        }

        public EntityTrie Build()
        {
            return new EntityTrie(Freeze(_root), _count);
        }

        private static TrieNode Freeze(MutableTrieNode mutable)
        {
            if (mutable.Children.Count == 0)
                return new TrieNode(ImmutableDictionary<string, TrieNode>.Empty, mutable.Value);

            var builder = ImmutableDictionary.CreateBuilder<string, TrieNode>();
            foreach (var (key, child) in mutable.Children)
                builder.Add(key, Freeze(child));

            return new TrieNode(builder.ToImmutable(), mutable.Value);
        }

        private sealed class MutableTrieNode
        {
            public Dictionary<string, MutableTrieNode> Children { get; } = new();
            public IndexEntity? Value { get; set; }
        }
    }

    #endregion
}

internal sealed class TrieNode
{
    public static readonly TrieNode EmptyNode = new(ImmutableDictionary<string, TrieNode>.Empty, null);

    public ImmutableDictionary<string, TrieNode> Children { get; }
    public IndexEntity? Value { get; }

    public TrieNode(ImmutableDictionary<string, TrieNode> children, IndexEntity? value)
    {
        Children = children;
        Value = value;
    }
}
