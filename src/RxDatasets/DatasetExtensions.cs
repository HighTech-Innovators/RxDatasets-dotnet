using System.Collections.Immutable;
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
    var splitCharacter = '/';
    var tempSnapshotEntities = index.Entities;
    var tempSubIndices = index.SubIndices;
    var deltaVersion = delta.DeltaVersion > 0 ? delta.DeltaVersion : delta.DeltaVersionLegacy;

    foreach (var newEntity in delta.NewEntities)
    {
        var immutableEntity = ImmutableDeltaEntity.FromProto(newEntity);
        var pathParts = immutableEntity.Name.Split(splitCharacter, StringSplitOptions.RemoveEmptyEntries);

        if (pathParts.Length == 1)
        {
            // Root level entity
            if (!index.Entities.TryGetValue(immutableEntity.Name, out var existingEntity))
            {
                existingEntity = ImmutableSnapshotEntity.FromProto(new SnapshotEntity());
            }

            var updatedEntity = new ImmutableSnapshotEntity(
                existingEntity.LastDeletedVersion,
                existingEntity.LastInsertedVersion.Add(deltaVersion),
                existingEntity.LastUpdatedVersion,
                immutableEntity.Position,
                immutableEntity.Length,
                immutableEntity.Crc32);

            tempSnapshotEntities = tempSnapshotEntities.SetItem(immutableEntity.Name, updatedEntity);
        }
        else
        {
            // Nested entity in sub-indices
            tempSubIndices = UpdateNestedEntity(tempSubIndices, pathParts, immutableEntity, deltaVersion, ChangeType.New);
        }
    }

    foreach (var changedEntity in delta.ChangedEntities)
    {
        var immutableEntity = ImmutableDeltaEntity.FromProto(changedEntity);
        var pathParts = immutableEntity.Name.Split(splitCharacter, StringSplitOptions.RemoveEmptyEntries);

        if (pathParts.Length == 1)
        {
            var existingEntity = index.Entities[immutableEntity.Name];

            var updatedEntity = new ImmutableSnapshotEntity(
                existingEntity.LastDeletedVersion,
                existingEntity.LastInsertedVersion,
                deltaVersion,
                immutableEntity.Position,
                immutableEntity.Length,
                immutableEntity.Crc32);

            tempSnapshotEntities = tempSnapshotEntities.SetItem(immutableEntity.Name, updatedEntity);
        }
        else
        {
            tempSubIndices = UpdateNestedEntity(tempSubIndices, pathParts, immutableEntity, deltaVersion, ChangeType.Changed);
        }
    }

    foreach (var deletedName in delta.DeletedNames)
    {
        var pathParts = deletedName.Split(splitCharacter, StringSplitOptions.RemoveEmptyEntries);

        if (pathParts.Length == 1)
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
        else
        {
            tempSubIndices = UpdateNestedEntityForDeletion(tempSubIndices, pathParts, deltaVersion);
        }
    }

    foreach (var subIndexPair in index.SubIndices)
    {
        var updatedChildIndex = ApplyDeltaToChildIndex(subIndexPair.Value, delta, deltaVersion);
        tempSubIndices = tempSubIndices.SetItem(subIndexPair.Key, updatedChildIndex);
    }

    return new ImmutableDatasetState(tempSnapshotEntities, tempSubIndices, index.SnapshotVersion, deltaVersion);
}


private static ImmutableDictionary<string, ImmutableSnapshotChildIndex> UpdateNestedEntity(
    ImmutableDictionary<string, ImmutableSnapshotChildIndex> subIndices,
    string[] pathParts,
    ImmutableDeltaEntity entity,
    ulong deltaVersion,
    ChangeType changeType)
{
    var currentSubIndices = subIndices;
    ImmutableSnapshotChildIndex currentChildIndex = null;

    // Navigate/create the path to the nested entity
    for (int i = 0; i < pathParts.Length - 1; i++)
    {
        var key = pathParts[i];

        if (!currentSubIndices.TryGetValue(key, out currentChildIndex))
        {
            currentChildIndex = new ImmutableSnapshotChildIndex(
                ImmutableDictionary<string, ImmutableSnapshotEntity>.Empty,
                ImmutableDictionary<string, ImmutableSnapshotChildIndex>.Empty);
        }

        var nestedChildIndex = currentChildIndex;
        var innerSubIndices = nestedChildIndex.SubIndices;

        // Continue navigating deeper if not at the final level
        if (i < pathParts.Length - 2)
        {
            innerSubIndices = UpdateNestedEntity(innerSubIndices, pathParts.Skip(i + 1).ToArray(), entity, deltaVersion, changeType);
        }
        else
        {
            // We're at the parent of the target entity, add the entity here
            var entityName = pathParts[pathParts.Length - 1];
            var entities = nestedChildIndex.Entities;

            if (!entities.TryGetValue(entityName, out var existingEntity))
            {
                existingEntity = ImmutableSnapshotEntity.FromProto(new SnapshotEntity());
            }

            var updatedEntity = new ImmutableSnapshotEntity(
                existingEntity.LastDeletedVersion,
                changeType == ChangeType.New ? existingEntity.LastInsertedVersion.Add(deltaVersion) : existingEntity.LastInsertedVersion,
                changeType == ChangeType.Changed ? deltaVersion : existingEntity.LastUpdatedVersion,
                entity.Position,
                entity.Length,
                entity.Crc32);

            entities = entities.SetItem(entityName, updatedEntity);
            nestedChildIndex = nestedChildIndex with { Entities = entities };
        }

        currentSubIndices = currentSubIndices.SetItem(key, nestedChildIndex);
    }

    return currentSubIndices;
}

private static ImmutableDictionary<string, ImmutableSnapshotChildIndex> UpdateNestedEntityForDeletion(
    ImmutableDictionary<string, ImmutableSnapshotChildIndex> subIndices,
    string[] pathParts,
    ulong deltaVersion)
{
    var currentSubIndices = subIndices;

    for (int i = 0; i < pathParts.Length - 1; i++)
    {
        var key = pathParts[i];

        if (currentSubIndices.TryGetValue(key, out var currentChildIndex))
        {
            if (i == pathParts.Length - 2)
            {
                var entityName = pathParts[pathParts.Length - 1];
                var entities = currentChildIndex.Entities;

                if (entities.TryGetValue(entityName, out var existingEntity))
                {
                    var updatedEntity = new ImmutableSnapshotEntity(
                        existingEntity.LastDeletedVersion.Add(deltaVersion),
                        existingEntity.LastInsertedVersion,
                        existingEntity.LastUpdatedVersion,
                        0,
                        0,
                        existingEntity.Crc32);

                    entities = entities.SetItem(entityName, updatedEntity);
                    currentChildIndex = currentChildIndex with { Entities = entities };
                }
            }

            currentSubIndices = currentSubIndices.SetItem(key, currentChildIndex);
        }
    }

    return currentSubIndices;
}

    private static ImmutableSnapshotChildIndex ApplyDeltaToChildIndex(ImmutableSnapshotChildIndex childIndex, DeltaIndex delta, ulong deltaVersion)
    {
        var tempEntities = childIndex.Entities;
        var tempSubIndices = childIndex.SubIndices;

        foreach (var newEntity in delta.NewEntities)
        {
            if (childIndex.Entities.TryGetValue(newEntity.Name, out var existingEntity))
            {
                var updatedEntity = new ImmutableSnapshotEntity(
                    existingEntity.LastDeletedVersion,
                    existingEntity.LastInsertedVersion.Add(deltaVersion),
                    existingEntity.LastUpdatedVersion,
                    newEntity.Position,
                    newEntity.Length,
                    newEntity.Crc32);

                tempEntities = tempEntities.SetItem(newEntity.Name, updatedEntity);
            }
        }

        foreach (var changedEntity in delta.ChangedEntities)
        {
            if (childIndex.Entities.TryGetValue(changedEntity.Name, out var existingEntity))
            {
                var updatedEntity = new ImmutableSnapshotEntity(
                    existingEntity.LastDeletedVersion,
                    existingEntity.LastInsertedVersion,
                    deltaVersion,
                    changedEntity.Position,
                    changedEntity.Length,
                    changedEntity.Crc32);

                tempEntities = tempEntities.SetItem(changedEntity.Name, updatedEntity);
            }
        }

        foreach (var deletedName in delta.DeletedNames)
        {
            if (childIndex.Entities.TryGetValue(deletedName, out var existingEntity))
            {
                var updatedEntity = new ImmutableSnapshotEntity(
                    existingEntity.LastDeletedVersion.Add(deltaVersion),
                    existingEntity.LastInsertedVersion,
                    existingEntity.LastUpdatedVersion,
                    0,
                    0,
                    existingEntity.Crc32);

                tempEntities = tempEntities.SetItem(deletedName, updatedEntity);
            }
        }

        // Recursively apply delta to nested sub-indices
        foreach (var nestedSubIndexPair in childIndex.SubIndices)
        {
            var updatedNestedChildIndex = ApplyDeltaToChildIndex(nestedSubIndexPair.Value, delta, deltaVersion);
            tempSubIndices = tempSubIndices.SetItem(nestedSubIndexPair.Key, updatedNestedChildIndex);
        }

        return new ImmutableSnapshotChildIndex(tempEntities, tempSubIndices);
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
