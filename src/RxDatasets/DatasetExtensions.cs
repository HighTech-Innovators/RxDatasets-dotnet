using RxDatasets.Abstractions;
using RxDatasets.Models;

namespace RxDatasets;

public static class DatasetExtensions
{
    private static readonly ImmutableSnapshotEntity NeverSeenEntity = ImmutableSnapshotEntity.FromProto(new SnapshotEntity());

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
    /// Applies the provided DeltaIndex to the provided state; copies only the paths to the changed entities.
    /// Reads both the tree format and the legacy flat lists.
    /// </summary>
    public static ImmutableDatasetState ApplyDelta(this ImmutableDatasetState index, DeltaIndex delta)
    {
        var deltaVersion = delta.DeltaVersion > 0 ? delta.DeltaVersion : delta.DeltaVersionLegacy;
        var builder = index.Root.ToBuilder();
        if (delta.Root != null)
            ApplyDeltaNode(builder, delta.Root, deltaVersion);
        if (delta.HasFlatEntries())
            ApplyDeltaNode(builder, AddFlatEntriesToTree(delta, new DeltaNode()), deltaVersion);

        return new ImmutableDatasetState(builder.ToImmutable(), index.SnapshotVersion, deltaVersion);
    }

    private static void ApplyDeltaNode(ImmutableIndexNode<ImmutableSnapshotEntity>.Builder node, DeltaNode delta, ulong deltaVersion)
    {
        foreach (var (template, group) in delta.Leaves)
        {
            foreach (var (variable, newEntity) in group.NewEntities)
            {
                var key = variable.AsMemory();
                var existingEntity = node.GetLeaf(template, key) ?? NeverSeenEntity;
                node.SetLeaf(template, key, new ImmutableSnapshotEntity(
                    existingEntity.LastDeletedVersion,
                    existingEntity.LastInsertedVersion.Add(deltaVersion),
                    existingEntity.LastUpdatedVersion,
                    newEntity.Position,
                    newEntity.Length,
                    newEntity.Crc32));
            }

            foreach (var (variable, changedEntity) in group.ChangedEntities)
            {
                var key = variable.AsMemory();
                var existingEntity = node.GetLeaf(template, key) ?? throw NotFound(template, variable, "update");
                node.SetLeaf(template, key, new ImmutableSnapshotEntity(
                    existingEntity.LastDeletedVersion,
                    existingEntity.LastInsertedVersion,
                    deltaVersion,
                    changedEntity.Position,
                    changedEntity.Length,
                    changedEntity.Crc32));
            }

            foreach (var variable in group.DeletedEntities.Keys)
            {
                var key = variable.AsMemory();
                var existingEntity = node.GetLeaf(template, key) ?? throw NotFound(template, variable, "delete");
                node.SetLeaf(template, key, new ImmutableSnapshotEntity(
                    existingEntity.LastDeletedVersion.Add(deltaVersion),
                    existingEntity.LastInsertedVersion,
                    existingEntity.LastUpdatedVersion,
                    0,
                    0,
                    existingEntity.Crc32));
            }
        }

        foreach (var (collection, group) in delta.Children)
        {
            foreach (var (id, child) in group.Nodes)
                ApplyDeltaNode(node.Child(collection.AsMemory(), id.AsMemory()), child, deltaVersion);
        }
    }

    private static KeyNotFoundException NotFound(string template, string variable, string operation) =>
        new($"Entity {IndexKeys.LeafName(template, variable)} not found for {operation}");

    public static ImmutableDatasetState ApplyDeltas(this ImmutableDatasetState index, IEnumerable<DeltaIndex> deltas)
    {
        var tempIndex = index;

        foreach (var delta in deltas)
        {
            tempIndex = tempIndex.ApplyDelta(delta);
        }

        return tempIndex;
    }

    /// <summary>Whether the delta has entries in the legacy flat lists.</summary>
    public static bool HasFlatEntries(this DeltaIndex delta) =>
        delta.NewEntities.Count > 0 || delta.ChangedEntities.Count > 0 || delta.DeletedNames.Count > 0 || delta.DeletedEntities.Count > 0;

    /// <summary>
    /// Moves the entries of the flat lists into the tree, with their names emptied: in the tree the position is
    /// the name. Writers call this before storing a delta, so only the tree format is written.
    /// </summary>
    public static void ConvertToTree(this DeltaIndex delta)
    {
        delta.Root ??= new DeltaNode();
        AddFlatEntriesToTree(delta, delta.Root);
        foreach (var entity in delta.NewEntities.Concat(delta.ChangedEntities))
            entity.Name = "";
        foreach (var entity in delta.DeletedEntities)
            entity.Name = "";

        delta.NewEntities.Clear();
        delta.ChangedEntities.Clear();
        delta.DeletedNames.Clear();
        delta.DeletedEntities.Clear();
    }

    private static DeltaNode AddFlatEntriesToTree(DeltaIndex delta, DeltaNode root)
    {
        foreach (var entity in delta.NewEntities)
            GetOrAddLeafGroup(root, entity.Name, out var variable).NewEntities[variable] = entity;
        foreach (var entity in delta.ChangedEntities)
            GetOrAddLeafGroup(root, entity.Name, out var variable).ChangedEntities[variable] = entity;
        foreach (var name in delta.DeletedNames)
            GetOrAddLeafGroup(root, name, out var variable).DeletedEntities[variable] = new DeletedEntity();
        // After the DeletedNames, so the DeletedEntity (with its previous version) wins
        foreach (var entity in delta.DeletedEntities)
            GetOrAddLeafGroup(root, entity.Name, out var variable).DeletedEntities[variable] = entity;
        return root;
    }

    private static DeltaLeafGroup GetOrAddLeafGroup(DeltaNode root, string name, out string variable)
    {
        var key = IndexKeys.Split(name);
        var node = root;
        for (int i = 0; i < key.Path.Length; i += 2)
        {
            if (!node.Children.TryGetValue(key.Path[i], out var children))
                node.Children.Add(key.Path[i], children = new DeltaChildGroup());
            if (!children.Nodes.TryGetValue(key.Path[i + 1], out var child))
                children.Nodes.Add(key.Path[i + 1], child = new DeltaNode());
            node = child;
        }

        if (!node.Leaves.TryGetValue(key.Template, out var group))
            node.Leaves.Add(key.Template, group = new DeltaLeafGroup());

        variable = key.Variable;
        return group;
    }

    private static DeltaLeafGroup? FindLeafGroup(DeltaNode? root, string name, out string variable)
    {
        var key = IndexKeys.Split(name);
        variable = key.Variable;
        var node = root;
        for (int i = 0; node != null && i < key.Path.Length; i += 2)
        {
            node = node.Children.TryGetValue(key.Path[i], out var children) && children.Nodes.TryGetValue(key.Path[i + 1], out var child)
                ? child
                : null;
        }

        return node != null && node.Leaves.TryGetValue(key.Template, out var group) ? group : null;
    }

    /// <summary>
    /// How <paramref name="name"/> changed in this delta, in either format. <c>Entity</c> is set for new and changed
    /// entities; <c>PreviousVersion</c> is 0 for a legacy deleted name.
    /// </summary>
    public static (ChangeType Change, ulong PreviousVersion, DeltaEntity? Entity) FindChange(this DeltaIndex delta, string name)
    {
        var group = FindLeafGroup(delta.Root, name, out var variable);
        if (group != null)
        {
            if (group.NewEntities.TryGetValue(variable, out var newEntity))
                return (ChangeType.New, newEntity.PreviousVersion, newEntity);
            if (group.ChangedEntities.TryGetValue(variable, out var changedEntity))
                return (ChangeType.Changed, changedEntity.PreviousVersion, changedEntity);
            if (group.DeletedEntities.TryGetValue(variable, out var deletedEntity))
                return (ChangeType.Deleted, deletedEntity.PreviousVersion, null);
        }

        var legacyNew = delta.NewEntities.FirstOrDefault(e => e.Name == name);
        if (legacyNew != null)
            return (ChangeType.New, legacyNew.PreviousVersion, legacyNew);
        var legacyChanged = delta.ChangedEntities.FirstOrDefault(e => e.Name == name);
        if (legacyChanged != null)
            return (ChangeType.Changed, legacyChanged.PreviousVersion, legacyChanged);
        var legacyDeleted = delta.DeletedEntities.FirstOrDefault(e => e.Name == name);
        if (legacyDeleted != null)
            return (ChangeType.Deleted, legacyDeleted.PreviousVersion, null);
        if (delta.DeletedNames.Contains(name))
            return (ChangeType.Deleted, 0, null);

        return (ChangeType.None, 0, null);
    }

    /// <summary>The new entities of a delta with their full names, from the tree and the legacy flat list.</summary>
    public static IEnumerable<(string Name, DeltaEntity Entity)> EnumerateNewEntities(this DeltaIndex delta) =>
        EnumerateDeltaNode(delta.Root, "", group => group.NewEntities).Concat(delta.NewEntities.Select(e => (e.Name, e)));

    /// <summary>The changed entities of a delta with their full names, from the tree and the legacy flat list.</summary>
    public static IEnumerable<(string Name, DeltaEntity Entity)> EnumerateChangedEntities(this DeltaIndex delta) =>
        EnumerateDeltaNode(delta.Root, "", group => group.ChangedEntities).Concat(delta.ChangedEntities.Select(e => (e.Name, e)));

    /// <summary>
    /// The deleted entities of a delta with their full names, from the tree and the legacy flat list. The legacy
    /// <see cref="DeltaIndex.DeletedNames"/> are not included.
    /// </summary>
    public static IEnumerable<(string Name, DeletedEntity Entity)> EnumerateDeletedEntities(this DeltaIndex delta) =>
        EnumerateDeltaNode(delta.Root, "", group => group.DeletedEntities).Concat(delta.DeletedEntities.Select(e => (e.Name, e)));

    private static IEnumerable<(string Name, TEntity Entity)> EnumerateDeltaNode<TEntity>(DeltaNode? node, string path,
        Func<DeltaLeafGroup, IDictionary<string, TEntity>> selectEntities)
    {
        if (node == null)
            yield break;

        foreach (var (template, group) in node.Leaves)
        {
            foreach (var (variable, entity) in selectEntities(group))
                yield return (IndexKeys.Join(path, IndexKeys.LeafName(template, variable)), entity);
        }

        foreach (var (collection, group) in node.Children)
        {
            foreach (var (id, child) in group.Nodes)
            {
                foreach (var entry in EnumerateDeltaNode(child, IndexKeys.Join(path, collection + "/" + id), selectEntities))
                    yield return entry;
            }
        }
    }

    /// <summary>
    /// The history entries of one delta, for the entities whose name starts with <paramref name="parent"/>: new,
    /// changed, deleted, and then the legacy deleted names that have no deleted entity.
    /// </summary>
    public static IEnumerable<HistoryEntry> EnumerateHistoryEntries(this DeltaIndex delta, ulong version, string parent)
    {
        var timestamp = delta.Timestamp?.ToDateTimeOffset() ?? DateTimeOffset.MinValue.ToUniversalTime();
        bool InParent(string name) => string.IsNullOrEmpty(parent) || name.StartsWith(parent, StringComparison.Ordinal);

        foreach (var (name, entity) in delta.EnumerateNewEntities().Where(e => InParent(e.Name)))
            yield return new HistoryEntry(version, ChangeType.New, timestamp, entity.PreviousVersion) { Id = name };

        foreach (var (name, entity) in delta.EnumerateChangedEntities().Where(e => InParent(e.Name)))
            yield return new HistoryEntry(version, ChangeType.Changed, timestamp, entity.PreviousVersion) { Id = name };

        var deletedEntityNames = new HashSet<string>();
        foreach (var (name, entity) in delta.EnumerateDeletedEntities())
        {
            deletedEntityNames.Add(name);
            if (InParent(name))
                yield return new HistoryEntry(version, ChangeType.Deleted, timestamp, entity.PreviousVersion) { Id = name };
        }

        foreach (var name in delta.DeletedNames.Where(n => !deletedEntityNames.Contains(n) && InParent(n)))
            yield return new HistoryEntry(version, ChangeType.Deleted, timestamp, 0) { Id = name };
    }

    /// <summary>The entities of a snapshot index with their full names, from the tree and the legacy flat map.</summary>
    public static IEnumerable<(string Name, SnapshotEntity Entity)> EnumerateEntities(this SnapshotIndex index) =>
        EnumerateSnapshotNode(index.Root, "").Concat(index.Entities.Select(e => (e.Key, e.Value)));

    private static IEnumerable<(string Name, SnapshotEntity Entity)> EnumerateSnapshotNode(SnapshotNode? node, string path)
    {
        if (node == null)
            yield break;

        foreach (var (template, group) in node.Leaves)
        {
            foreach (var (variable, entity) in group.Entities)
                yield return (IndexKeys.Join(path, IndexKeys.LeafName(template, variable)), entity);
        }

        foreach (var (collection, group) in node.Children)
        {
            foreach (var (id, child) in group.Nodes)
            {
                foreach (var entry in EnumerateSnapshotNode(child, IndexKeys.Join(path, collection + "/" + id)))
                    yield return entry;
            }
        }
    }
}
