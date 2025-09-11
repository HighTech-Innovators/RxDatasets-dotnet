using System.Collections.Immutable;

namespace RxDatasets.Models;

public record ImmutableDatasetState(ImmutableDictionary<string, ImmutableSnapshotEntity> Entities, ulong SnapshotVersion, ulong LatestVersion)
{
    public static ImmutableDatasetState FromProto(SnapshotIndex snapshotIndex)
    {
        // Single-pass conversion without materializing intermediate lists.
        var protoEntities = snapshotIndex.Entities;
        var capacity = protoEntities?.Count ?? 0;

        // Use ImmutableDictionary builder to avoid an extra copy when creating the immutable map
        var builder = ImmutableDictionary.CreateBuilder<string, ImmutableSnapshotEntity>();

        if (protoEntities is not null && capacity > 0)
        {
            foreach (var kv in protoEntities)
            {
                builder.Add(kv.Key, ImmutableSnapshotEntity.FromProto(kv.Value));
            }
        }

        var snapshotIndexSnapshotVersion = snapshotIndex.SnapshotVersion > 0 ? snapshotIndex.SnapshotVersion : snapshotIndex.SnapshotVersionLegacy;
        return new ImmutableDatasetState(builder.ToImmutable(), snapshotIndexSnapshotVersion, snapshotIndexSnapshotVersion);
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
