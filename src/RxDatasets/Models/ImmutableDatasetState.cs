using System.Collections.Immutable;

namespace RxDatasets.Models;

public record ImmutableDatasetState()
{
    public readonly ImmutableDictionary<string, ImmutableSnapshotEntity> Entities;
    public readonly ImmutableDictionary<string, ImmutableSnapshotChildIndex> SubIndices;
    public readonly ulong SnapshotVersion, LatestVersion;
    public ImmutableDatasetState(ImmutableDictionary<string, ImmutableSnapshotEntity> entities, ulong snapshotVersion, ulong latestVersion) : this()
    {
        Entities = entities;
        SubIndices = ImmutableDictionary<string, ImmutableSnapshotChildIndex>.Empty;
        SnapshotVersion = snapshotVersion;
        LatestVersion = latestVersion;
    }

    public ImmutableDatasetState(ImmutableDictionary<string, ImmutableSnapshotChildIndex> subIndices, ulong snapshotVersion, ulong latestVersion) : this()
    {
        Entities = ImmutableDictionary<string, ImmutableSnapshotEntity>.Empty;
        SubIndices = subIndices;
        SnapshotVersion = snapshotVersion;
        LatestVersion = latestVersion;
    }

    public static ImmutableDatasetState FromProto(SnapshotIndex snapshotIndex) =>
        snapshotIndex.Entities.Keys.ToList().Any(x => x.Contains('/')) ? CreateNestedDataState(snapshotIndex) : CreateFlatDataState(snapshotIndex);


    private static ImmutableDatasetState CreateFlatDataState(SnapshotIndex snapshotIndex)
    {
        Dictionary<string, ImmutableSnapshotEntity> entities = new();
        snapshotIndex.Entities.ToList().ForEach(x => entities.Add(x.Key, ImmutableSnapshotEntity.FromProto(x.Value)));

        var snapshotIndexSnapshotVersion = snapshotIndex.SnapshotVersion > 0 ? snapshotIndex.SnapshotVersion : snapshotIndex.SnapshotVersionLegacy;
        return new ImmutableDatasetState(entities.ToImmutableDictionary(), snapshotIndexSnapshotVersion, snapshotIndexSnapshotVersion);
    }

   private static ImmutableDatasetState CreateNestedDataState(SnapshotIndex snapshotIndex)
    {
        var rootSubIndices = ImmutableDictionary<string, ImmutableSnapshotChildIndex>.Empty;

        foreach (var entityPair in snapshotIndex.Entities)
        {
            var pathParts = entityPair.Key.Split('/', StringSplitOptions.RemoveEmptyEntries);
            var immutableEntity = ImmutableSnapshotEntity.FromProto(entityPair.Value);

            rootSubIndices = AddNestedEntity(rootSubIndices, pathParts, immutableEntity);
        }

        var snapshotVersion = snapshotIndex.SnapshotVersion > 0 ? snapshotIndex.SnapshotVersion : snapshotIndex.SnapshotVersionLegacy;
        return new ImmutableDatasetState(rootSubIndices, snapshotVersion, snapshotVersion);
    }

    private static ImmutableDictionary<string, ImmutableSnapshotChildIndex> AddNestedEntity(
        ImmutableDictionary<string, ImmutableSnapshotChildIndex> subIndices,
        string[] pathParts,
        ImmutableSnapshotEntity entity)
    {
        if (pathParts.Length == 0)
            return subIndices;

        var currentKey = pathParts[0];
        var currentSubIndices = subIndices;

        if (!currentSubIndices.TryGetValue(currentKey, out var childIndex))
        {
            childIndex = new ImmutableSnapshotChildIndex();
        }

        switch (pathParts.Length)
        {
            case 1:
                // Single part - shouldn't happen for nested, but handle it
                return currentSubIndices.SetItem(currentKey, childIndex);
            case 2:
            {
                // Two parts: first is key, second is entity name
                var entityName = pathParts[1];
                var updatedEntities = childIndex.Entities.SetItem(entityName, entity);
                var updatedChild = new ImmutableSnapshotChildIndex(updatedEntities);
                return currentSubIndices.SetItem(currentKey, new ImmutableSnapshotChildIndex(updatedChild.SubIndices));
            }
        }

        // More than two parts: recurse into sub-indices
        var remainingParts = pathParts.Skip(1).ToArray();
        var updatedSubIndices = AddNestedEntity(childIndex.SubIndices, remainingParts, entity);
        var updatedChildIndex = new ImmutableSnapshotChildIndex(updatedSubIndices);
        return currentSubIndices.SetItem(currentKey, updatedChildIndex);
    }

    public SnapshotIndex ToProto()
    {
        SnapshotIndex snapshotIndex = new SnapshotIndex()
        {
            SnapshotVersionLegacy = 0,
            SnapshotVersion = this.SnapshotVersion
        };

        foreach (var entity in Entities)
        {
            snapshotIndex.Entities.Add(entity.Key, entity.Value.ToProto());
        }

        return snapshotIndex;
    }
}
