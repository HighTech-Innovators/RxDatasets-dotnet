using System.Collections.Immutable;

namespace RxDatasets.Models;

/// <summary>
/// Represents a hierarchical node in the dataset index structure.
/// This is the in-memory representation of SnapshotNode from the Protobuf schema.
/// Each node can contain entities (leaves) and child nodes (sub-directories).
/// </summary>
public sealed record ImmutableIndexNode(
    ImmutableDictionary<string, ImmutableSnapshotEntity> Entities,
    ImmutableDictionary<string, ImmutableIndexNode> Children)
{
    /// <summary>
    /// Creates an empty index node with no entities or children.
    /// </summary>
    public static ImmutableIndexNode Empty => new(
        ImmutableDictionary<string, ImmutableSnapshotEntity>.Empty,
        ImmutableDictionary<string, ImmutableIndexNode>.Empty);

    /// <summary>
    /// Adds or updates an entity at this node level.
    /// Path segments are expected to be already split by the caller.
    /// </summary>
    public ImmutableIndexNode AddEntity(string key, ImmutableSnapshotEntity entity)
    {
        var updatedEntities = Entities.SetItem(key, entity);
        return new ImmutableIndexNode(updatedEntities, Children);
    }

    /// <summary>
    /// Removes an entity from this node level.
    /// </summary>
    public ImmutableIndexNode RemoveEntity(string key)
    {
        var updatedEntities = Entities.Remove(key);
        return new ImmutableIndexNode(updatedEntities, Children);
    }

    /// <summary>
    /// Gets an entity from this node level, or null if not found.
    /// </summary>
    public ImmutableSnapshotEntity? GetEntity(string key)
    {
        return Entities.TryGetValue(key, out var entity) ? entity : null;
    }

    /// <summary>
    /// Gets or creates a child node. Returns the existing child if present,
    /// or an empty new node if not.
    /// </summary>
    public ImmutableIndexNode GetOrCreateChild(string key)
    {
        if (Children.TryGetValue(key, out var child))
        {
            return child;
        }
        return Empty;
    }

    /// <summary>
    /// Sets a child node at the specified key.
    /// </summary>
    public ImmutableIndexNode SetChild(string key, ImmutableIndexNode child)
    {
        var updatedChildren = Children.SetItem(key, child);
        return new ImmutableIndexNode(Entities, updatedChildren);
    }

    /// <summary>
    /// Removes a child node at the specified key.
    /// </summary>
    public ImmutableIndexNode RemoveChild(string key)
    {
        var updatedChildren = Children.Remove(key);
        return new ImmutableIndexNode(Entities, updatedChildren);
    }

    /// <summary>
    /// Gets a child node, or null if not found.
    /// </summary>
    public ImmutableIndexNode? GetChild(string key)
    {
        return Children.TryGetValue(key, out var child) ? child : null;
    }

    /// <summary>
    /// Navigates to an entity using a path. Path segments should be split by '/'.
    /// Returns null if the path doesn't exist.
    /// </summary>
    public ImmutableSnapshotEntity? Navigate(IEnumerable<string> pathSegments)
    {
        var segments = pathSegments.ToList();
        if (segments.Count == 0)
            return null;

        var current = this;
        for (int i = 0; i < segments.Count - 1; i++)
        {
            var child = current.GetChild(segments[i]);
            if (child == null)
                return null;
            current = child;
        }

        return current.GetEntity(segments[^1]);
    }

    /// <summary>
    /// Converts the Protobuf SnapshotNode to an ImmutableIndexNode.
    /// Recursively converts the entire tree structure.
    /// </summary>
    public static ImmutableIndexNode FromProto(SnapshotNode protoNode)
    {
        if (protoNode == null)
        {
            return Empty;
        }

        // Convert entities at this level
        var entities = new Dictionary<string, ImmutableSnapshotEntity>();
        foreach (var kvp in protoNode.Entities)
        {
            entities[kvp.Key] = ImmutableSnapshotEntity.FromProto(kvp.Value);
        }

        // Recursively convert children
        var children = new Dictionary<string, ImmutableIndexNode>();
        foreach (var kvp in protoNode.Children)
        {
            children[kvp.Key] = FromProto(kvp.Value);
        }

        return new ImmutableIndexNode(
            entities.ToImmutableDictionary(),
            children.ToImmutableDictionary());
    }

    /// <summary>
    /// Converts the ImmutableIndexNode to a Protobuf SnapshotNode.
    /// Recursively converts the entire tree structure.
    /// </summary>
    public SnapshotNode ToProto()
    {
        var protoNode = new SnapshotNode();

        // Add entities at this level
        foreach (var kvp in Entities)
        {
            protoNode.Entities[kvp.Key] = kvp.Value.ToProto();
        }

        // Recursively add children
        foreach (var kvp in Children)
        {
            protoNode.Children[kvp.Key] = kvp.Value.ToProto();
        }

        return protoNode;
    }
}
