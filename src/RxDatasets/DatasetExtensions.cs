using System.Collections.Immutable;
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
    /// Applies the provided DeltaIndex to the provided state; copies only the paths to the changed entities and
    /// aliases. Reads both the tree format and the legacy flat lists.
    /// </summary>
    public static ImmutableDatasetState ApplyDelta(this ImmutableDatasetState index, DeltaIndex delta)
    {
        var deltaVersion = delta.DeltaVersion > 0 ? delta.DeltaVersion : delta.DeltaVersionLegacy;
        var builder = index.Root.ToBuilder();
        var aliases = new AliasMapsBuilder(index.Aliases);
        if (delta.Root != null)
            ApplyDeltaNode(builder, delta.Root, deltaVersion, aliases, []);
        if (delta.HasFlatEntries())
            ApplyDeltaNode(builder, AddFlatEntriesToTree(delta, new DeltaNode()), deltaVersion, aliases, []);

        foreach (var change in delta.AliasChanges)
        {
            var entity = builder.Get(change.Name)
                         ?? throw new KeyNotFoundException($"Entity {change.Name} not found for an alias change");
            builder.Set(change.Name, entity with
            {
                Aliases = ApplyAliasChanges(change.Name, entity.Aliases, change.AddedAliases.FromProto(),
                    change.RemovedAliases.FromProto(), aliases)
            });
        }

        return new ImmutableDatasetState(builder.ToImmutable(), index.SnapshotVersion, deltaVersion)
        {
            AliasDefinitionsHash = delta.AliasDefinitionsHash,
            Aliases = aliases.ToImmutable()
        };
    }

    /// <summary>The owned set after a change, with the reverse maps updated to match.</summary>
    private static ImmutableArray<(string Name, string Value)> ApplyAliasChanges(string entityName,
        ImmutableArray<(string Name, string Value)> owned, ImmutableArray<(string Name, string Value)> added,
        ImmutableArray<(string Name, string Value)> removed, AliasMapsBuilder aliases)
    {
        aliases.Remove(entityName, removed);
        aliases.Add(entityName, added);
        return [.. owned.Except(removed).Concat(added)];
    }

    private static void ApplyDeltaNode(ImmutableIndexNode<ImmutableSnapshotEntity>.Builder node, DeltaNode delta,
        ulong deltaVersion, AliasMapsBuilder aliases, List<string> path)
    {
        string NameOf(string template, string variable) =>
            IndexKeys.Join(string.Join('/', path), IndexKeys.LeafName(template, variable));

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
                    newEntity.Crc32)
                {
                    Aliases = OwnedAliases(variable, existingEntity.Aliases, newEntity.AddedAliases,
                        newEntity.RemovedAliases)
                });
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
                    changedEntity.Crc32)
                {
                    Aliases = OwnedAliases(variable, existingEntity.Aliases, changedEntity.AddedAliases,
                        changedEntity.RemovedAliases)
                });
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
                    existingEntity.Crc32)
                {
                    // A deleted entity owns nothing: release all it owned, not only what the delta lists
                    Aliases = existingEntity.Aliases.IsEmpty
                        ? []
                        : ApplyAliasChanges(NameOf(template, variable), existingEntity.Aliases, [], existingEntity.Aliases,
                            aliases)
                });
            }

            // The full name is only built for an entity whose aliases change
            ImmutableArray<(string Name, string Value)> OwnedAliases(string variable,
                ImmutableArray<(string Name, string Value)> owned, IReadOnlyCollection<AliasRef> added,
                IReadOnlyCollection<AliasRef> removed) =>
                added.Count == 0 && removed.Count == 0
                    ? owned
                    : ApplyAliasChanges(NameOf(template, variable), owned, added.FromProto(), removed.FromProto(), aliases);
        }

        foreach (var (collection, group) in delta.Children)
        {
            foreach (var (id, child) in group.Nodes)
            {
                path.Add(collection);
                path.Add(id);
                ApplyDeltaNode(node.Child(collection.AsMemory(), id.AsMemory()), child, deltaVersion, aliases, path);
                path.RemoveRange(path.Count - 2, 2);
            }
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
        delta.NewEntities.Count > 0 || delta.ChangedEntities.Count > 0
                                    || delta.DeletedNames.Count > 0 || delta.DeletedEntities.Count > 0;

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
            node = node.Children.TryGetValue(key.Path[i], out var children)
                   && children.Nodes.TryGetValue(key.Path[i + 1], out var child)
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
        var (change, entity, deleted) = Locate(delta, name);
        return (change, entity?.PreviousVersion ?? deleted?.PreviousVersion ?? 0, entity);
    }

    /// <summary>
    /// The history entry for <paramref name="name"/> in this delta, with the tracking id and SLA timestamp its write
    /// passed; <c>null</c> when the delta does not change it.
    /// </summary>
    public static HistoryEntry? FindHistoryEntry(this DeltaIndex delta, ulong version, string name)
    {
        var (change, entity, deleted) = Locate(delta, name);
        if (change == ChangeType.None)
            return null;

        var timestamp = delta.Timestamp?.ToDateTimeOffset() ?? DateTimeOffset.MinValue.ToUniversalTime();
        return entity != null
            ? ToHistoryEntry(version, change, timestamp, name, entity)
            : deleted != null
                ? ToHistoryEntry(version, timestamp, name, deleted)
                : new HistoryEntry(version, ChangeType.Deleted, timestamp, 0) { Id = name };
    }

    /// <summary>
    /// How <paramref name="name"/> changed in this delta, in either format, with the entry that records it. Both
    /// entries are <c>null</c> for a legacy deleted name.
    /// </summary>
    private static (ChangeType Change, DeltaEntity? Entity, DeletedEntity? Deleted) Locate(DeltaIndex delta, string name)
    {
        var group = FindLeafGroup(delta.Root, name, out var variable);
        if (group != null)
        {
            if (group.NewEntities.TryGetValue(variable, out var newEntity))
                return (ChangeType.New, newEntity, null);
            if (group.ChangedEntities.TryGetValue(variable, out var changedEntity))
                return (ChangeType.Changed, changedEntity, null);
            if (group.DeletedEntities.TryGetValue(variable, out var deletedEntity))
                return (ChangeType.Deleted, null, deletedEntity);
        }

        var legacyNew = delta.NewEntities.FirstOrDefault(e => e.Name == name);
        if (legacyNew != null)
            return (ChangeType.New, legacyNew, null);
        var legacyChanged = delta.ChangedEntities.FirstOrDefault(e => e.Name == name);
        if (legacyChanged != null)
            return (ChangeType.Changed, legacyChanged, null);
        var legacyDeleted = delta.DeletedEntities.FirstOrDefault(e => e.Name == name);
        if (legacyDeleted != null)
            return (ChangeType.Deleted, null, legacyDeleted);
        if (delta.DeletedNames.Contains(name))
            return (ChangeType.Deleted, null, null);

        return (ChangeType.None, null, null);
    }

    private static HistoryEntry ToHistoryEntry(ulong version, ChangeType change, DateTimeOffset timestamp, string name,
        DeltaEntity entity) =>
        new(version, change, timestamp, entity.PreviousVersion)
        {
            Id = name,
            TrackingId = entity.TrackingId,
            SlaTimestamp = entity.SlaTimestamp?.ToDateTimeOffset()
        };

    private static HistoryEntry ToHistoryEntry(ulong version, DateTimeOffset timestamp, string name, DeletedEntity entity) =>
        new(version, ChangeType.Deleted, timestamp, entity.PreviousVersion)
        {
            Id = name,
            TrackingId = entity.TrackingId,
            SlaTimestamp = entity.SlaTimestamp?.ToDateTimeOffset()
        };

    /// <summary>The new entities of a delta with their full names, from the tree and the legacy flat list.</summary>
    public static IEnumerable<(string Name, DeltaEntity Entity)> EnumerateNewEntities(this DeltaIndex delta) =>
        EnumerateDeltaNode(delta.Root, group => group.NewEntities).Concat(delta.NewEntities.Select(e => (e.Name, e)));

    /// <summary>The changed entities of a delta with their full names, from the tree and the legacy flat list.</summary>
    public static IEnumerable<(string Name, DeltaEntity Entity)> EnumerateChangedEntities(this DeltaIndex delta) =>
        EnumerateDeltaNode(delta.Root, group => group.ChangedEntities)
            .Concat(delta.ChangedEntities.Select(e => (e.Name, e)));

    /// <summary>
    /// The deleted entities of a delta with their full names, from the tree and the legacy flat list. The legacy
    /// <see cref="DeltaIndex.DeletedNames"/> are not included.
    /// </summary>
    public static IEnumerable<(string Name, DeletedEntity Entity)> EnumerateDeletedEntities(this DeltaIndex delta) =>
        EnumerateDeltaNode(delta.Root, group => group.DeletedEntities)
            .Concat(delta.DeletedEntities.Select(e => (e.Name, e)));

    private static IEnumerable<(string Name, TEntity Entity)> EnumerateDeltaNode<TEntity>(DeltaNode? root,
        Func<DeltaLeafGroup, IDictionary<string, TEntity>> selectEntities) =>
        EnumerateTree(root, node => node.Leaves.Select(leaf => (leaf.Key, selectEntities(leaf.Value))),
            node => node.Children.Select(child => (child.Key, (IDictionary<string, DeltaNode>)child.Value.Nodes)));

    /// <summary>The entities of a snapshot or delta tree with their full names.</summary>
    private static IEnumerable<(string Name, TEntity Entity)> EnumerateTree<TNode, TEntity>(TNode? node,
        Func<TNode, IEnumerable<(string Template, IDictionary<string, TEntity> Entities)>> leafGroups,
        Func<TNode, IEnumerable<(string Collection, IDictionary<string, TNode> Nodes)>> childGroups,
        string path = "") where TNode : class
    {
        if (node == null)
            yield break;

        foreach (var (template, entities) in leafGroups(node))
        {
            foreach (var (variable, entity) in entities)
                yield return (IndexKeys.Join(path, IndexKeys.LeafName(template, variable)), entity);
        }

        foreach (var (collection, nodes) in childGroups(node))
        {
            foreach (var (id, child) in nodes)
            {
                foreach (var entry in EnumerateTree(child, leafGroups, childGroups, IndexKeys.Join(path, collection, id)))
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
            yield return ToHistoryEntry(version, ChangeType.New, timestamp, name, entity);

        foreach (var (name, entity) in delta.EnumerateChangedEntities().Where(e => InParent(e.Name)))
            yield return ToHistoryEntry(version, ChangeType.Changed, timestamp, name, entity);

        var deletedEntityNames = new HashSet<string>();
        foreach (var (name, entity) in delta.EnumerateDeletedEntities())
        {
            deletedEntityNames.Add(name);
            if (InParent(name))
                yield return ToHistoryEntry(version, timestamp, name, entity);
        }

        foreach (var name in delta.DeletedNames.Where(n => !deletedEntityNames.Contains(n) && InParent(n)))
            yield return new HistoryEntry(version, ChangeType.Deleted, timestamp, 0) { Id = name };
    }

    /// <summary>
    /// The net change of every name in the versions after <paramref name="fromVersion"/> up to and including
    /// <paramref name="toVersion"/>, read from the delta indexes alone, with the tracking id and SLA timestamp of each
    /// name's last change in that range.
    /// </summary>
    /// <remarks>
    /// A name's first change in the range says whether it existed at <paramref name="fromVersion"/> (only a creation
    /// says it did not), its last change whether it exists now. Together: new, changed, deleted, or nothing, as
    /// <see cref="IDatasetReader{T}.GetChangedNames"/> reports. A live poller reads one delta.
    /// </remarks>
    /// <exception cref="ResyncRequiredException">A delta in the range is no longer kept.</exception>
    public static async Task<HistoryChangeSet> ReadHistoryChanges(this IStorageProviderReader storage, ulong fromVersion,
        ulong toVersion, CancellationToken ct = default)
    {
        var firstChanges = new Dictionary<string, ChangeType>();
        var lastChanges = new Dictionary<string, HistoryEntry>();
        var names = new List<string>();
        for (var version = fromVersion + 1; version <= toVersion; version++)
        {
            ct.ThrowIfCancellationRequested();
            DeltaIndex delta;
            try
            {
                delta = DeltaIndex.Parser.ParseFrom((await storage.ReadDeltaIndex(version)).Span);
            }
            catch (FileNotFoundException)
            {
                throw new ResyncRequiredException(fromVersion);
            }

            foreach (var entry in delta.EnumerateHistoryEntries(version, ""))
            {
                if (firstChanges.TryAdd(entry.Id, entry.ChangeType))
                    names.Add(entry.Id);
                lastChanges[entry.Id] = entry;
            }
        }

        var changes = new List<HistoryChange>();
        foreach (var name in names)
        {
            var last = lastChanges[name];
            var change = (Existed: firstChanges[name] != ChangeType.New, Exists: last.ChangeType != ChangeType.Deleted) switch
            {
                (false, true) => ChangeType.New,
                (true, true) => ChangeType.Changed,
                (true, false) => ChangeType.Deleted,
                _ => ChangeType.None
            };
            if (change != ChangeType.None)
                changes.Add(new HistoryChange(name, change, last.TrackingId, last.SlaTimestamp));
        }

        return new HistoryChangeSet(toVersion, changes);
    }

    /// <summary>
    /// The names changed after <paramref name="fromVersion"/> up to <paramref name="index"/>'s latest version, with how
    /// each changed: from the delta indexes (<see cref="ReadHistoryChanges"/>), so the in-memory index needs no history.
    /// </summary>
    /// <remarks>
    /// From version 0 nothing existed before, so the changes are the live entities, all new, and the index answers that
    /// without reading a delta: a full round needs no deltas that cleanup may have removed.
    /// </remarks>
    /// <exception cref="ResyncRequiredException">
    /// <paramref name="fromVersion"/> is not 0 and a delta after it is no longer kept.
    /// </exception>
    public static async Task<ChangeSet> ReadChangedNames(this IStorageProviderReader storage, ImmutableDatasetState index,
        ulong fromVersion, CancellationToken ct = default)
    {
        var toVersion = index.LatestVersion;
        if (fromVersion == 0)
            return new ChangeSet(toVersion, [.. index.EnumerateAll(where: entity => entity.IsLive).Select(entity => (entity.Key, ChangeType.New))]);

        var changes = await storage.ReadHistoryChanges(fromVersion, toVersion, ct);
        return new ChangeSet(toVersion, [.. changes.Changes.Select(change => (change.Id, change.ChangeType))]);
    }

    /// <summary>The entities of a snapshot index with their full names, from the tree and the legacy flat map.</summary>
    public static IEnumerable<(string Name, SnapshotEntity Entity)> EnumerateEntities(this SnapshotIndex index) =>
        EnumerateTree(index.Root,
                node => node.Leaves.Select(leaf => (leaf.Key, (IDictionary<string, SnapshotEntity>)leaf.Value.Entities)),
                node => node.Children.Select(child => (child.Key, (IDictionary<string, SnapshotNode>)child.Value.Nodes)))
            .Concat(index.Entities.Select(e => (e.Key, e.Value)));
}
