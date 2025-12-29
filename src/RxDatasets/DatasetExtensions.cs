using RxDatasets.Abstractions;
using RxDatasets.Models;

namespace RxDatasets;

public static class DatasetExtensions
{
    public static ulong GetLatestEntityVersion(this ImmutableSnapshotEntity snapshotEntity)
    {
        return Math.Max(snapshotEntity.LastUpdatedVersion, Math.Max(snapshotEntity.LastDeletedVersion.LastOrDefault(), snapshotEntity.LastInsertedVersion.LastOrDefault()));
    }

    public static ChangeType GetLatestChangeType(this ImmutableSnapshotEntity snapshotEntity)
    {
        ulong latestVersion = snapshotEntity.GetLatestEntityVersion();

        if (latestVersion == snapshotEntity.LastInsertedVersion.LastOrDefault())
            return ChangeType.New;

        if (latestVersion == snapshotEntity.LastUpdatedVersion)
            return ChangeType.Changed;

        return ChangeType.Deleted;
    }

    public static ulong GetLatestEntityVersion(this SnapshotEntity entity)
    {
        return Math.Max(entity.LastUpdatedVersion, Math.Max(entity.LastDeletedVersion.LastOrDefault(), entity.LastInsertedVersion.LastOrDefault()));
    }

    public static ChangeType GetLatestChangeType(this SnapshotEntity entity)
    {
        ulong latestVersion = entity.GetLatestEntityVersion();

        if (latestVersion == entity.LastInsertedVersion.LastOrDefault())
            return ChangeType.New;

        if (latestVersion == entity.LastUpdatedVersion)
            return ChangeType.Changed;

        return ChangeType.Deleted;
    }

    /// <summary>
    /// Applies the provided DeltaIndex to the provided SnapshotIndex
    /// </summary>
    /// <param name="index"></param>
    /// <param name="delta"></param>
    public static ImmutableDatasetState ApplyDelta(this ImmutableDatasetState index, DeltaIndex delta)
    {
        var tempSnapshotEntities = index.Entities;
        var deltaVersion = delta.DeltaVersion > 0 ? delta.DeltaVersion : delta.DeltaVersionLegacy;
        foreach (var newEntity in delta.NewEntities)
        {
            if (!index.Entities.TryGetValue(newEntity.Name, out var existingEntity))
            {
                existingEntity = ImmutableSnapshotEntity.FromProto(new SnapshotEntity());
            }

            var updatedEntity = new ImmutableSnapshotEntity(
                existingEntity.LastDeletedVersion,
                existingEntity.LastInsertedVersion.Add(deltaVersion),
                existingEntity.LastUpdatedVersion,
                newEntity.Position,
                newEntity.Length,
                newEntity.Crc32);

            tempSnapshotEntities = tempSnapshotEntities.SetItem(newEntity.Name, updatedEntity);
        }

        foreach (var changedEntity in delta.ChangedEntities)
        {
            var existingEntity = index.Entities[changedEntity.Name];

            var updatedEntity = new ImmutableSnapshotEntity(
                existingEntity.LastDeletedVersion,
                existingEntity.LastInsertedVersion,
                deltaVersion,
                changedEntity.Position,
                changedEntity.Length,
                changedEntity.Crc32);

            tempSnapshotEntities = tempSnapshotEntities.SetItem(changedEntity.Name, updatedEntity);
        }

        foreach (var deletedName in delta.DeletedNames)
        {
            var existingEntity = index.Entities[deletedName];

            var updatedEntity = new ImmutableSnapshotEntity(
                existingEntity.LastDeletedVersion.Add(deltaVersion),
                existingEntity.LastInsertedVersion,
                existingEntity.LastUpdatedVersion,
                0,
                0,
                existingEntity.Crc32);

            tempSnapshotEntities = tempSnapshotEntities.SetItem(deletedName, updatedEntity);
        }

        foreach (var deletedEntity in delta.DeletedEntities)
        {
            var existingEntity = index.Entities[deletedEntity.Name];

            var updatedEntity = new ImmutableSnapshotEntity(
                existingEntity.LastDeletedVersion.Add(deltaVersion),
                existingEntity.LastInsertedVersion,
                existingEntity.LastUpdatedVersion,
                0,
                0,
                existingEntity.Crc32);

            tempSnapshotEntities = tempSnapshotEntities.SetItem(deletedEntity.Name, updatedEntity);
        }

        return new ImmutableDatasetState(tempSnapshotEntities, index.SnapshotVersion, deltaVersion);
    }

    public static ImmutableDatasetState ApplyDeltas(this ImmutableDatasetState index, IEnumerable<DeltaIndex> deltas)
    {
        var tempIndex = index;

        foreach (var delta in deltas)
        {
            tempIndex = tempIndex.ApplyDelta(delta);
        }

        return tempIndex;
    }

    /// <summary>
    /// Converts an ImmutableIndexNode to a Protobuf SnapshotNode.
    /// This is a direct pass-through to the ImmutableIndexNode.ToProto() method.
    /// </summary>
    public static SnapshotNode ToSnapshotNode(this ImmutableIndexNode node)
    {
        return node.ToProto();
    }

    /// <summary>
    /// Creates a SnapshotNode from a flat dictionary of entities.
    /// Entities are expected to have hierarchical keys (e.g., "dir1/dir2/file.txt").
    /// </summary>
    public static SnapshotNode ToSnapshotNodeFromFlat(this IReadOnlyDictionary<string, ImmutableSnapshotEntity> flatEntities)
    {
        var root = ImmutableIndexNode.Empty;

        foreach (var kvp in flatEntities)
        {
            var pathSegments = kvp.Key.Split('/');
            var entity = kvp.Value;

            // Navigate/create the path in the hierarchy
            root = InsertEntityIntoNode(root, pathSegments, entity);
        }

        return root.ToProto();
    }

    /// <summary>
    /// Helper method to insert an entity at a specific path in the hierarchical index.
    /// </summary>
    private static ImmutableIndexNode InsertEntityIntoNode(
        ImmutableIndexNode root,
        string[] pathSegments,
        ImmutableSnapshotEntity entity)
    {
        if (pathSegments.Length == 0)
            return root;

        var current = root;
        var nodePath = new Stack<(string key, ImmutableIndexNode node)>();
        nodePath.Push(("root", current));

        // Navigate/create the path
        for (int i = 0; i < pathSegments.Length - 1; i++)
        {
            var segment = pathSegments[i];
            var child = current.GetChild(segment) ?? ImmutableIndexNode.Empty;
            nodePath.Push((segment, child));
            current = child;
        }

        // Add the entity at the final level
        var fileName = pathSegments[^1];
        current = current.AddEntity(fileName, entity);

        // Rebuild the tree from bottom up
        while (nodePath.Count > 0)
        {
            var (key, _) = nodePath.Pop();
            if (key != "root")
            {
                var parent = nodePath.Count > 0 ? nodePath.Peek().node : ImmutableIndexNode.Empty;
                parent = parent.SetChild(key, current);
                current = parent;
            }
        }

        return current;
    }

    /// <summary>
    /// Converts a DeltaIndex to a hierarchical DeltaNode.
    /// Entities with hierarchical keys are distributed into the tree structure.
    /// </summary>
    public static DeltaNode ToDeltaNode(this DeltaIndex deltaIndex)
    {
        var root = new DeltaNode();

        // Distribute changed entities into the tree
        foreach (var changedEntity in deltaIndex.ChangedEntities)
        {
            DistributeDeltaEntity(root, changedEntity.Name.Split('/'), changedEntity, isNew: false, isDeleted: false);
        }

        // Distribute new entities into the tree
        foreach (var newEntity in deltaIndex.NewEntities)
        {
            DistributeDeltaEntity(root, newEntity.Name.Split('/'), newEntity, isNew: true, isDeleted: false);
        }

        // Distribute deleted names into the tree
        foreach (var deletedName in deltaIndex.DeletedNames)
        {
            DistributeDeletedName(root, deletedName.Split('/'));
        }

        // Distribute deleted entities into the tree
        foreach (var deletedEntity in deltaIndex.DeletedEntities)
        {
            DistributeDeltaEntity(root, deletedEntity.Name.Split('/'), new DeltaEntity
            {
                Name = deletedEntity.Name,
                Position = 0,
                Length = 0,
                Crc32 = 0,
                PreviousVersion = deletedEntity.PreviousVersion
            }, isNew: false, isDeleted: true);
        }

        return root;
    }

    /// <summary>
    /// Helper to distribute a delta entity into a node tree.
    /// </summary>
    private static void DistributeDeltaEntity(
        DeltaNode root,
        string[] pathSegments,
        DeltaEntity entity,
        bool isNew,
        bool isDeleted)
    {
        if (pathSegments.Length == 0)
            return;

        var current = root;

        // Navigate/create path to the parent of the entity
        for (int i = 0; i < pathSegments.Length - 1; i++)
        {
            var segment = pathSegments[i];
            if (!current.Children.ContainsKey(segment))
            {
                current.Children[segment] = new DeltaNode();
            }
            current = current.Children[segment];
        }

        // Add the entity at the final level
        var fileName = pathSegments[^1];
        if (isNew)
        {
            current.NewEntities[fileName] = entity;
        }
        else if (isDeleted)
        {
            current.ChangedEntities[fileName] = entity;
        }
        else
        {
            current.ChangedEntities[fileName] = entity;
        }
    }

    /// <summary>
    /// Helper to distribute a deleted name into a node tree.
    /// </summary>
    private static void DistributeDeletedName(DeltaNode root, string[] pathSegments)
    {
        if (pathSegments.Length == 0)
            return;

        var current = root;

        // Navigate/create path to the parent
        for (int i = 0; i < pathSegments.Length - 1; i++)
        {
            var segment = pathSegments[i];
            if (!current.Children.ContainsKey(segment))
            {
                current.Children[segment] = new DeltaNode();
            }
            current = current.Children[segment];
        }

        // Add the deleted name at the final level
        var fileName = pathSegments[^1];
        if (!current.DeletedNames.Contains(fileName))
        {
            current.DeletedNames.Add(fileName);
        }
    }
}
