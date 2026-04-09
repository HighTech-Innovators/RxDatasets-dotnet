using System.Collections;
using System.Collections.Frozen;
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
    internal static readonly EntityTrie Empty = new(TrieNode.EmptyNode, 0, null);

    private readonly TrieNode _root;
    internal NameCompressor? Compressor { get; }

    public int Count { get; }

    private EntityTrie(TrieNode root, int count, NameCompressor? compressor)
    {
        _root = root;
        Count = count;
        Compressor = compressor;
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
        var key = Compressor?.Compress(name) ?? name;
        var node = _root;
        int start = 0;

        while (start <= key.Length)
        {
            int end = key.IndexOf('/', start);
            if (end == -1) end = key.Length;

            var segment = key.Substring(start, end - start);
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
        var key = Compressor?.Compress(name) ?? name;
        var segments = key.Split('/');
        bool wasNew = !ContainsKey(name);
        var newRoot = SetItemRecursive(_root, segments, 0, value);
        return new EntityTrie(newRoot, wasNew ? Count + 1 : Count, Compressor);
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
        var dict = new Dictionary<string, TrieNode>(node.Children) { [segment] = newChild };
        return new TrieNode(dict.ToFrozenDictionary(), node.Value);
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

        var key = Compressor?.Compress(name) ?? name;
        var segments = key.Split('/');
        var newRoot = RemoveItemRecursive(_root, segments, 0);
        return new EntityTrie(newRoot ?? TrieNode.EmptyNode, Count - 1, Compressor);
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
        Dictionary<string, TrieNode> newChildren;
        if (newChild == null)
        {
            newChildren = new Dictionary<string, TrieNode>(node.Children);
            newChildren.Remove(segment);
        }
        else
        {
            newChildren = new Dictionary<string, TrieNode>(node.Children) { [segment] = newChild };
        }

        if (newChildren.Count == 0 && node.Value is null)
            return null; // Prune
        return new TrieNode(newChildren.ToFrozenDictionary(), node.Value);
    }

    /// <summary>
    /// Enumerates all entity names that are descendants of the given parent prefix.
    /// Parent should NOT end with '/'.
    /// All entities in the trie are alive (no deleted-entity filter needed).
    /// </summary>
    public IEnumerable<string> ListDescendants(string parent)
    {
        var compressedParent = Compressor?.CompressPrefix(parent) ?? parent;
        var node = NavigateTo(compressedParent);
        if (node is null)
            yield break;

        var prefix = compressedParent.Length > 0 ? compressedParent + "/" : "";
        foreach (var (name, _) in EnumerateSubtree(node, prefix))
        {
            yield return Compressor?.Expand(name) ?? name;
        }
    }

    /// <summary>
    /// Counts descendants under the given parent.
    /// All entities in the trie are alive (no deleted-entity filter needed).
    /// </summary>
    public int CountDescendants(string parent)
    {
        var compressedParent = Compressor?.CompressPrefix(parent) ?? parent;
        var node = NavigateTo(compressedParent);
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
        return EnumerateAll(_root, "", Compressor).GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    private static IEnumerable<KeyValuePair<string, IndexEntity>> EnumerateAll(
        TrieNode node, string prefix, NameCompressor? compressor)
    {
        if (node.Value is not null)
        {
            var key = compressor?.Expand(prefix) ?? prefix;
            yield return new KeyValuePair<string, IndexEntity>(key, node.Value);
        }

        foreach (var (segment, child) in node.Children)
        {
            var childPath = prefix.Length == 0 ? segment : prefix + "/" + segment;
            foreach (var kvp in EnumerateAll(child, childPath, compressor))
                yield return kvp;
        }
    }

    #endregion

    /// <summary>
    /// Creates a mutable Builder from this immutable trie.
    /// The Builder shares no state with the original trie, so mutations are safe.
    /// Use this for bulk delta application: thaw once, apply all changes, freeze once.
    /// </summary>
    public Builder ToBuilder()
    {
        var mutableRoot = ThawRecursive(_root);
        return new Builder(mutableRoot, Count, Compressor);
    }

    private static Builder.MutableTrieNode ThawRecursive(TrieNode node)
    {
        var mutable = new Builder.MutableTrieNode { Value = node.Value };

        foreach (var (segment, child) in node.Children)
        {
            mutable.Children[segment] = ThawRecursive(child);
        }

        return mutable;
    }

    /// <summary>
    /// Creates an EntityTrie from a dictionary of entity names to index entities.
    /// </summary>
    public static EntityTrie FromDictionary(
        IEnumerable<KeyValuePair<string, IndexEntity>> entries, NameCompressor? compressor = null)
    {
        var builder = new Builder(compressor);
        foreach (var kvp in entries)
            builder.Add(kvp.Key, kvp.Value);
        return builder.Build();
    }

    /// <summary>
    /// Builds an EntityTrie directly from a proto <see cref="SnapshotBucket"/> tree
    /// without reconstructing any entity name strings. Maps proto nodes 1:1 to trie nodes,
    /// pre-allocating dictionaries from proto sizes.
    /// </summary>
    internal static EntityTrie FromSnapshotBucket(SnapshotBucket bucket, NameCompressor? compressor)
    {
        var root = FreezeFromBucket(bucket, out int count);
        return new EntityTrie(root, count, compressor);
    }

    private static TrieNode FreezeFromBucket(SnapshotBucket bucket, out int count)
    {
        count = 0;

        if (bucket.Children.Count == 0 && bucket.Leaves.Count == 0)
            return TrieNode.EmptyNode;

        var dict = new Dictionary<string, TrieNode>(bucket.Children.Count + bucket.Leaves.Count);

        foreach (var (segment, childBucket) in bucket.Children)
        {
            var childNode = FreezeFromBucket(childBucket, out int childCount);
            count += childCount;
            if (childNode != TrieNode.EmptyNode)
                dict[segment] = childNode;
        }

        foreach (var (segment, entity) in bucket.Leaves)
        {
            if (IndexEntity.IsDeletedInProto(entity))
                continue;

            var value = IndexEntity.FromProto(entity);

            if (dict.TryGetValue(segment, out var existingNode))
            {
                // Segment is both an intermediate node and a leaf — merge value
                dict[segment] = new TrieNode(existingNode.Children, value);
            }
            else
            {
                dict[segment] = new TrieNode(FrozenDictionary<string, TrieNode>.Empty, value);
            }

            count++;
        }

        if (dict.Count == 0)
            return TrieNode.EmptyNode;

        return new TrieNode(dict.ToFrozenDictionary(), null);
    }

    #region Builder (for bulk construction and mutation)

    public sealed class Builder
    {
        private readonly MutableTrieNode _root;
        private readonly NameCompressor? _compressor;
        private int _count;

        public Builder(NameCompressor? compressor = null)
        {
            _root = new MutableTrieNode();
            _compressor = compressor;
            _count = 0;
        }

        internal Builder(MutableTrieNode root, int count, NameCompressor? compressor)
        {
            _root = root;
            _count = count;
            _compressor = compressor;
        }

        /// <summary>
        /// Adds a new entity. Assumes the entity does not already exist.
        /// Use <see cref="SetItem"/> when overwriting is possible.
        /// </summary>
        public void Add(string name, IndexEntity value)
        {
            var key = _compressor?.Compress(name) ?? name;
            var node = NavigateOrCreate(key);
            node.Value = value;
            _count++;
        }

        /// <summary>
        /// Sets an entity, overwriting any existing value. Correctly tracks count.
        /// </summary>
        public void SetItem(string name, IndexEntity value)
        {
            var key = _compressor?.Compress(name) ?? name;
            var node = NavigateOrCreate(key);
            if (node.Value is null)
                _count++;
            node.Value = value;
        }

        /// <summary>
        /// Removes an entity. Returns true if the entity was found and removed.
        /// Does not prune empty intermediate nodes (they are pruned during <see cref="Build"/>).
        /// </summary>
        public bool Remove(string name)
        {
            var key = _compressor?.Compress(name) ?? name;
            var segments = key.Split('/');
            var node = _root;

            for (int i = 0; i < segments.Length; i++)
            {
                if (!node.Children.TryGetValue(segments[i], out var child))
                    return false;
                node = child;
            }

            if (node.Value is null)
                return false;

            node.Value = null;
            _count--;
            return true;
        }

        public EntityTrie Build()
        {
            return new EntityTrie(Freeze(_root), _count, _compressor);
        }

        private MutableTrieNode NavigateOrCreate(string key)
        {
            var segments = key.Split('/');
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

            return node;
        }

        private static TrieNode Freeze(MutableTrieNode mutable)
        {
            // Prune: if no children and no value, this node is empty
            if (mutable.Children.Count == 0 && mutable.Value is null)
                return TrieNode.EmptyNode;

            if (mutable.Children.Count == 0)
                return new TrieNode(FrozenDictionary<string, TrieNode>.Empty, mutable.Value);

            var dict = new Dictionary<string, TrieNode>(mutable.Children.Count);
            foreach (var (key, child) in mutable.Children)
            {
                var frozen = Freeze(child);
                if (frozen != TrieNode.EmptyNode || frozen.Value is not null)
                    dict.Add(key, frozen);
            }

            if (dict.Count == 0 && mutable.Value is null)
                return TrieNode.EmptyNode;

            return new TrieNode(dict.ToFrozenDictionary(), mutable.Value);
        }

        internal sealed class MutableTrieNode
        {
            public Dictionary<string, MutableTrieNode> Children { get; } = new();
            public IndexEntity? Value { get; set; }
        }
    }

    #endregion
}

internal sealed class TrieNode
{
    public static readonly TrieNode EmptyNode = new(FrozenDictionary<string, TrieNode>.Empty, null);

    public FrozenDictionary<string, TrieNode> Children { get; }
    public IndexEntity? Value { get; }

    public TrieNode(FrozenDictionary<string, TrieNode> children, IndexEntity? value)
    {
        Children = children;
        Value = value;
    }
}
