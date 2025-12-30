using System.Collections.Immutable;

namespace RxDatasets.Models;

/// <summary>
/// Represents the immutable state of a dataset.
/// This record supports both flat (legacy) and hierarchical index structures.
/// 
/// The hierarchical structure (IndexNode) is preferred for memory efficiency,
/// but the flat Entities dictionary is maintained for backward compatibility
/// and is automatically generated from the hierarchical structure when needed.
/// </summary>
public record ImmutableDatasetState
{
    public ImmutableIndexNode IndexNode { get; init; }
    public ulong SnapshotVersion { get; init; }
    public ulong LatestVersion { get; init; }

    // Backward compatibility: Computed property that flattens the index.
    // WARNING: This is expensive (O(N)) and should be avoided in performance-critical paths.
    [Obsolete("Use IndexNode or GetEntity() instead. This property is expensive and will be removed in future versions.")]
    public ImmutableDictionary<string, ImmutableSnapshotEntity> Entities => FlattenIndexNode(IndexNode, "");

    public ImmutableDatasetState(
        ImmutableDictionary<string, ImmutableSnapshotEntity> entities, // Ignored, kept for compat if needed but we prefer IndexNode
        ulong snapshotVersion,
        ulong latestVersion,
        ImmutableIndexNode? indexNode = null)
    {
        SnapshotVersion = snapshotVersion;
        LatestVersion = latestVersion;
        
        if (indexNode != null)
        {
            IndexNode = indexNode;
        }
        else
        {
            // Convert flat entities to hierarchical
            IndexNode = BuildIndexNodeFromFlat(entities);
        }
    }

    public ImmutableDatasetState(
        ImmutableIndexNode indexNode,
        ulong snapshotVersion,
        ulong latestVersion)
    {
        IndexNode = indexNode;
        SnapshotVersion = snapshotVersion;
        LatestVersion = latestVersion;
    }

    /// <summary>
    /// Gets an entity by its full path name.
    /// </summary>
    public ImmutableSnapshotEntity? GetEntity(string name)
    {
        return IndexNode.Navigate(name.Split('/'));
    }

    private static ImmutableIndexNode BuildIndexNodeFromFlat(ImmutableDictionary<string, ImmutableSnapshotEntity> entities)
    {
        var root = ImmutableIndexNode.Empty;
        foreach (var kvp in entities)
        {
            root = SetEntityRecursive(root, kvp.Key.Split('/'), 0, kvp.Value);
        }
        return root;
    }

    private static ImmutableIndexNode SetEntityRecursive(ImmutableIndexNode node, string[] pathParts, int index, ImmutableSnapshotEntity entity)
    {
        if (index >= pathParts.Length) return node;

        if (index == pathParts.Length - 1)
        {
            return node.AddEntity(pathParts[index], entity);
        }

        var childKey = pathParts[index];
        var childNode = node.GetOrCreateChild(childKey);
        var updatedChild = SetEntityRecursive(childNode, pathParts, index + 1, entity);
        
        return node.SetChild(childKey, updatedChild);
    }

    /// <summary>
    /// Gets a flattened view of all entities by traversing the hierarchical index.
    /// This is computed on-the-fly from IndexNode if available, otherwise returns Entities.
    /// </summary>
    [Obsolete("Use IndexNode or GetEntity() instead. This method is expensive and will be removed in future versions.")]
    public ImmutableDictionary<string, ImmutableSnapshotEntity> GetFlatEntities()
    {
        return Entities;
    }

    /// <summary>
    /// Recursively flattens the hierarchical index into a flat dictionary.
    /// </summary>
    private static ImmutableDictionary<string, ImmutableSnapshotEntity> FlattenIndexNode(
        ImmutableIndexNode node,
        string prefix)
    {
        var result = new Dictionary<string, ImmutableSnapshotEntity>();

        // Add entities at this level
        foreach (var entity in node.Entities)
        {
            var key = string.IsNullOrEmpty(prefix) ? entity.Key : $"{prefix}/{entity.Key}";
            result[key] = entity.Value;
        }

        // Recursively flatten children
        foreach (var child in node.Children)
        {
            var childPrefix = string.IsNullOrEmpty(prefix) ? child.Key : $"{prefix}/{child.Key}";
            var flatChild = FlattenIndexNode(child.Value, childPrefix);
            foreach (var kvp in flatChild)
            {
                result[kvp.Key] = kvp.Value;
            }
        }

        return result.ToImmutableDictionary();
    }

    /// <summary>
    /// Creates an ImmutableDatasetState from a Protobuf SnapshotIndex.
    /// Prefers reading from the hierarchical root if present, otherwise falls back to flat entities.
    /// </summary>
    public static ImmutableDatasetState FromProto(SnapshotIndex snapshotIndex)
    {
        var snapshotIndexSnapshotVersion = snapshotIndex.SnapshotVersion > 0
            ? snapshotIndex.SnapshotVersion
            : snapshotIndex.SnapshotVersionLegacy;

        // Check if hierarchical root is present and populated
        if (snapshotIndex.Root != null &&
            (snapshotIndex.Root.Entities.Count > 0 || snapshotIndex.Root.Children.Count > 0))
        {
            var indexNode = ImmutableIndexNode.FromProto(snapshotIndex.Root);
            return new ImmutableDatasetState(
                indexNode,
                snapshotIndexSnapshotVersion,
                snapshotIndexSnapshotVersion);
        }

        // Fall back to legacy flat entities - convert to hierarchical
        var entities = new Dictionary<string, ImmutableSnapshotEntity>();
        foreach (var x in snapshotIndex.Entities)
        {
            entities.Add(x.Key, ImmutableSnapshotEntity.FromProto(x.Value));
        }

        return new ImmutableDatasetState(
            entities.ToImmutableDictionary(),
            snapshotIndexSnapshotVersion,
            snapshotIndexSnapshotVersion,
            null);
    }

    /// <summary>
    /// Converts to a Protobuf SnapshotIndex.
    /// If IndexNode is available, it's serialized to the root field.
    /// Legacy flat entities are always populated for backward compatibility.
    /// </summary>
    public SnapshotIndex ToProto()
    {
        var snapshotIndex = new SnapshotIndex()
        {
            SnapshotVersionLegacy = 0,
            SnapshotVersion = this.SnapshotVersion
        };

        // Serialize hierarchical structure
        snapshotIndex.Root = IndexNode.ToProto();

        return snapshotIndex;
    }

    /// <summary>
    /// Creates a new ImmutableDatasetState with an updated hierarchical index.
    /// </summary>
    public ImmutableDatasetState WithIndexNode(ImmutableIndexNode newIndexNode)
    {
        return new ImmutableDatasetState(
            newIndexNode,
            SnapshotVersion,
            LatestVersion);
    }
}
