using System.Collections.Immutable;
using System.Net.Http.Headers;

namespace RxDatasets.Models;

public record ImmutableDatasetState(ImmutableDictionary<string, ImmutableSnapshotEntity> Entities, ImmutableDictionary<string, ImmutableSnapshotChildIndex> SubIndices,ulong SnapshotVersion, ulong LatestVersion)
{
    public static ImmutableDatasetState FromProto(SnapshotIndex snapshotIndex)
    {
        static ImmutableSnapshotChildIndex MapChildIndex(SnapshotChildIndex childIndex)
        {
            var entities = childIndex.Entities
                .ToDictionary(
                    pair => pair.Key,
                    pair => ImmutableSnapshotEntity.FromProto(pair.Value)
                )
                .ToImmutableDictionary();

            var subIndices = childIndex.SubIndices
                .ToDictionary(
                    pair => pair.Key,
                    pair => MapChildIndex(pair.Value)
                )
                .ToImmutableDictionary();

            return new ImmutableSnapshotChildIndex(entities, subIndices);
        }
        Dictionary<string, ImmutableSnapshotEntity> entities = new();
        snapshotIndex.Entities.ToList().ForEach(entityPair => entities.Add(entityPair.Key, ImmutableSnapshotEntity.FromProto(entityPair.Value)));

        Dictionary<string, ImmutableSnapshotChildIndex> rootSubIndices = new();
        snapshotIndex.SubIndices.ToList().ForEach(subIndexPair => rootSubIndices.Add(subIndexPair.Key, MapChildIndex(subIndexPair.Value)));

        var snapshotIndexSnapshotVersion = snapshotIndex.SnapshotVersion > 0 ? snapshotIndex.SnapshotVersion : snapshotIndex.SnapshotVersionLegacy;
        return new ImmutableDatasetState(entities.ToImmutableDictionary(), rootSubIndices.ToImmutableDictionary(), snapshotIndexSnapshotVersion, snapshotIndexSnapshotVersion);
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
