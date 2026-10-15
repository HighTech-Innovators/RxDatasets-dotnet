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
        var tempIndexNode = index.IndexNode;
        var deltaVersion = delta.DeltaVersion > 0 ? delta.DeltaVersion : delta.DeltaVersionLegacy;

        if (delta.HasHierarchicalEntries())
        {
            tempIndexNode = ApplyHierarchicalDeltaRecursive(tempIndexNode, delta.Root, deltaVersion);
        }
        else
        {
            foreach (var newEntity in delta.NewEntities)
            {
                var existingEntity = index.GetEntity(newEntity.Name);
                if (existingEntity == null)
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

                tempIndexNode = SetEntityRecursive(tempIndexNode, newEntity.Name.Split('/'), 0, updatedEntity);
            }

            foreach (var changedEntity in delta.ChangedEntities)
            {
                var existingEntity = index.GetEntity(changedEntity.Name) ?? throw new KeyNotFoundException($"Entity {changedEntity.Name} not found for update");

                var updatedEntity = new ImmutableSnapshotEntity(
                    existingEntity.LastDeletedVersion,
                    existingEntity.LastInsertedVersion,
                    deltaVersion,
                    changedEntity.Position,
                    changedEntity.Length,
                    changedEntity.Crc32);

                tempIndexNode = SetEntityRecursive(tempIndexNode, changedEntity.Name.Split('/'), 0, updatedEntity);
            }

            foreach (var deletedName in delta.DeletedNames)
            {
                var existingEntity = index.GetEntity(deletedName) ?? throw new KeyNotFoundException($"Entity {deletedName} not found for delete");

                var updatedEntity = new ImmutableSnapshotEntity(
                    existingEntity.LastDeletedVersion.Add(deltaVersion),
                    existingEntity.LastInsertedVersion,
                    existingEntity.LastUpdatedVersion,
                    0,
                    0,
                    existingEntity.Crc32);

                tempIndexNode = SetEntityRecursive(tempIndexNode, deletedName.Split('/'), 0, updatedEntity);
            }

            foreach (var deletedEntity in delta.DeletedEntities)
            {
                var existingEntity = index.GetEntity(deletedEntity.Name) ?? throw new KeyNotFoundException($"Entity {deletedEntity.Name} not found for delete");

                var updatedEntity = new ImmutableSnapshotEntity(
                    existingEntity.LastDeletedVersion.Add(deltaVersion),
                    existingEntity.LastInsertedVersion,
                    existingEntity.LastUpdatedVersion,
                    0,
                    0,
                    existingEntity.Crc32);

                tempIndexNode = SetEntityRecursive(tempIndexNode, deletedEntity.Name.Split('/'), 0, updatedEntity);
            }
        }

        return new ImmutableDatasetState(tempIndexNode, index.SnapshotVersion, deltaVersion);
    }

    private static ImmutableIndexNode ApplyHierarchicalDeltaRecursive(ImmutableIndexNode indexNode, DeltaNode deltaNode, ulong deltaVersion)
    {
        var currentNode = indexNode;

        // Apply New Entities
        foreach (var kvp in deltaNode.NewEntities)
        {
            var name = kvp.Key;
            var newEntity = kvp.Value;
            
            var existingEntity = currentNode.GetEntity(name);
            ImmutableSnapshotEntity baseEntity;
            if (existingEntity != null)
            {
                baseEntity = existingEntity;
            }
            else
            {
                baseEntity = ImmutableSnapshotEntity.FromProto(new SnapshotEntity());
            }

            var updatedEntity = new ImmutableSnapshotEntity(
                baseEntity.LastDeletedVersion,
                baseEntity.LastInsertedVersion.Add(deltaVersion),
                baseEntity.LastUpdatedVersion,
                newEntity.Position,
                newEntity.Length,
                newEntity.Crc32);
            
            currentNode = currentNode.AddEntity(name, updatedEntity);
        }

        // Apply Changed Entities
        foreach (var kvp in deltaNode.ChangedEntities)
        {
            var name = kvp.Key;
            var changedEntity = kvp.Value;
            
            var existingEntity = currentNode.GetEntity(name) ?? throw new KeyNotFoundException($"Entity {name} not found for update");

            var updatedEntity = new ImmutableSnapshotEntity(
                existingEntity.LastDeletedVersion,
                existingEntity.LastInsertedVersion,
                deltaVersion,
                changedEntity.Position,
                changedEntity.Length,
                changedEntity.Crc32);

            currentNode = currentNode.AddEntity(name, updatedEntity);
        }

        // Apply Deleted Entities
        foreach (var kvp in deltaNode.DeletedEntities)
        {
            var name = kvp.Key;
            var deletedEntity = kvp.Value;
            
            var existingEntity = currentNode.GetEntity(name) ?? throw new KeyNotFoundException($"Entity {name} not found for delete");

            var updatedEntity = new ImmutableSnapshotEntity(
                existingEntity.LastDeletedVersion.Add(deltaVersion),
                existingEntity.LastInsertedVersion,
                existingEntity.LastUpdatedVersion,
                0,
                0,
                existingEntity.Crc32);

            currentNode = currentNode.AddEntity(name, updatedEntity);
        }

        // Recursively apply to children
        foreach (var childKvp in deltaNode.Children)
        {
            var childName = childKvp.Key;
            var childDelta = childKvp.Value;
            
            var childIndexNode = currentNode.GetOrCreateChild(childName);
            var updatedChild = ApplyHierarchicalDeltaRecursive(childIndexNode, childDelta, deltaVersion);
            
            currentNode = currentNode.SetChild(childName, updatedChild);
        }

        return currentNode;
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
    /// Whether the delta stores its changes in the hierarchical <see cref="DeltaIndex.Root"/>
    /// (as opposed to the legacy flat lists).
    /// </summary>
    public static bool HasHierarchicalEntries(this DeltaIndex deltaIndex)
        => deltaIndex.Root != null && (deltaIndex.Root.NewEntities.Count > 0 ||
                                       deltaIndex.Root.ChangedEntities.Count > 0 ||
                                       deltaIndex.Root.DeletedEntities.Count > 0 ||
                                       deltaIndex.Root.Children.Count > 0);

    /// <summary>
    /// Enumerates the new entities of a delta with their full names, from both the hierarchical
    /// <see cref="DeltaIndex.Root"/> and the legacy flat <see cref="DeltaIndex.NewEntities"/>.
    /// </summary>
    public static IEnumerable<(string Name, DeltaEntity Entity)> EnumerateNewEntities(this DeltaIndex deltaIndex)
        => EnumerateDeltaNode(deltaIndex.Root, "", node => node.NewEntities)
            .Concat(deltaIndex.NewEntities.Select(e => (e.Name, e)));

    /// <summary>
    /// Enumerates the changed entities of a delta with their full names, from both the hierarchical
    /// <see cref="DeltaIndex.Root"/> and the legacy flat <see cref="DeltaIndex.ChangedEntities"/>.
    /// </summary>
    public static IEnumerable<(string Name, DeltaEntity Entity)> EnumerateChangedEntities(this DeltaIndex deltaIndex)
        => EnumerateDeltaNode(deltaIndex.Root, "", node => node.ChangedEntities)
            .Concat(deltaIndex.ChangedEntities.Select(e => (e.Name, e)));

    /// <summary>
    /// Enumerates the deleted entities of a delta with their full names, from both the hierarchical
    /// <see cref="DeltaIndex.Root"/> and the legacy flat <see cref="DeltaIndex.DeletedEntities"/>.
    /// The legacy <see cref="DeltaIndex.DeletedNames"/> are not included.
    /// </summary>
    public static IEnumerable<(string Name, DeletedEntity Entity)> EnumerateDeletedEntities(this DeltaIndex deltaIndex)
        => EnumerateDeltaNode(deltaIndex.Root, "", node => node.DeletedEntities)
            .Concat(deltaIndex.DeletedEntities.Select(e => (e.Name, e)));

    /// <summary>
    /// Walks a delta node tree. Full names are built from the map keys, since the entity's own
    /// Name field may hold either the leaf name (migrated deltas) or the full name.
    /// </summary>
    private static IEnumerable<(string Name, TEntity Entity)> EnumerateDeltaNode<TEntity>(
        DeltaNode? node,
        string prefix,
        Func<DeltaNode, IDictionary<string, TEntity>> selectEntities)
    {
        if (node == null)
            yield break;

        foreach (var (key, entity) in selectEntities(node))
            yield return (JoinPath(prefix, key), entity);

        foreach (var (key, child) in node.Children)
        {
            foreach (var descendant in EnumerateDeltaNode(child, JoinPath(prefix, key), selectEntities))
                yield return descendant;
        }
    }

    private static string JoinPath(string prefix, string key) => string.IsNullOrEmpty(prefix) ? key : $"{prefix}/{key}";

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
            DistributeDeltaEntity(root, changedEntity.Name.Split('/'), changedEntity, isNew: false);
        }

        // Distribute new entities into the tree
        foreach (var newEntity in deltaIndex.NewEntities)
        {
            DistributeDeltaEntity(root, newEntity.Name.Split('/'), newEntity, isNew: true);
        }

        // Distribute deleted names into the tree
        foreach (var deletedName in deltaIndex.DeletedNames)
        {
            DistributeDeletedEntity(root, deletedName.Split('/'), new DeletedEntity { Name = deletedName, PreviousVersion = 0 });
        }

        // Distribute deleted entities into the tree
        foreach (var deletedEntity in deltaIndex.DeletedEntities)
        {
            DistributeDeletedEntity(root, deletedEntity.Name.Split('/'), deletedEntity);
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
        bool isNew)
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
        else
        {
            current.ChangedEntities[fileName] = entity;
        }
    }

    /// <summary>
    /// Helper to distribute a deleted entity into a node tree.
    /// </summary>
    private static void DistributeDeletedEntity(DeltaNode root, string[] pathSegments, DeletedEntity entity)
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

        // Add the deleted entity at the final level
        var fileName = pathSegments[^1];
        current.DeletedEntities[fileName] = entity;
    }
}
