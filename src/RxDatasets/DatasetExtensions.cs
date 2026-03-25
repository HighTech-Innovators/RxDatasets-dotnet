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
        var trie = index.Entities;
        var deltaVersion = delta.DeltaVersion > 0 ? delta.DeltaVersion : delta.DeltaVersionLegacy;
        foreach (var newEntity in delta.NewEntities)
        {
            if (!trie.TryGetValue(newEntity.Name, out var existingEntity))
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

            trie = trie.SetItem(newEntity.Name, updatedEntity);
        }

        foreach (var changedEntity in delta.ChangedEntities)
        {
            var existingEntity = trie[changedEntity.Name];

            var updatedEntity = new ImmutableSnapshotEntity(
                existingEntity.LastDeletedVersion,
                existingEntity.LastInsertedVersion,
                deltaVersion,
                changedEntity.Position,
                changedEntity.Length,
                changedEntity.Crc32);

            trie = trie.SetItem(changedEntity.Name, updatedEntity);
        }

        foreach (var deletedName in delta.DeletedNames)
        {
            var existingEntity = trie[deletedName];

            var updatedEntity = new ImmutableSnapshotEntity(
                existingEntity.LastDeletedVersion.Add(deltaVersion),
                existingEntity.LastInsertedVersion,
                existingEntity.LastUpdatedVersion,
                0,
                0,
                existingEntity.Crc32);

            trie = trie.SetItem(deletedName, updatedEntity);
        }

        foreach (var deletedEntity in delta.DeletedEntities)
        {
            var existingEntity = trie[deletedEntity.Name];

            var updatedEntity = new ImmutableSnapshotEntity(
                existingEntity.LastDeletedVersion.Add(deltaVersion),
                existingEntity.LastInsertedVersion,
                existingEntity.LastUpdatedVersion,
                0,
                0,
                existingEntity.Crc32);

            trie = trie.SetItem(deletedEntity.Name, updatedEntity);
        }

        return new ImmutableDatasetState(trie, index.SnapshotVersion, deltaVersion);
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
}
