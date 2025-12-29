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
public record ImmutableDatasetState(
    ImmutableDictionary<string, ImmutableSnapshotEntity> Entities, 
    ulong SnapshotVersion, 
    ulong LatestVersion,
    ImmutableIndexNode? IndexNode = null)
{
    /// <summary>
    /// Gets a flattened view of all entities by traversing the hierarchical index.
    /// This is computed on-the-fly from IndexNode if available, otherwise returns Entities.
    /// </summary>
    public ImmutableDictionary<string, ImmutableSnapshotEntity> GetFlatEntities()
    {
        if (IndexNode != null && !IndexNode.Entities.IsEmpty)
        {
            return FlattenIndexNode(IndexNode, "");
        }
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
            var flatEntities = FlattenIndexNode(indexNode, "");
            return new ImmutableDatasetState(
                flatEntities, 
                snapshotIndexSnapshotVersion, 
                snapshotIndexSnapshotVersion,
                indexNode);
        }

        // Fall back to legacy flat entities
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

        // Always populate legacy flat entities for backward compatibility
        var entitiesToSerialize = IndexNode != null ? GetFlatEntities() : Entities;
        foreach (var entity in entitiesToSerialize)
        {
            snapshotIndex.Entities.Add(entity.Key, entity.Value.ToProto());
        }

        // Serialize hierarchical structure if available
        if (IndexNode != null)
        {
            snapshotIndex.Root = IndexNode.ToProto();
        }

        return snapshotIndex;
    }

    /// <summary>
    /// Creates a new ImmutableDatasetState with an updated hierarchical index.
    /// </summary>
    public ImmutableDatasetState WithIndexNode(ImmutableIndexNode newIndexNode)
    {
        return new ImmutableDatasetState(
            GetFlatEntities(),
            SnapshotVersion,
            LatestVersion,
            newIndexNode);
    }
}
