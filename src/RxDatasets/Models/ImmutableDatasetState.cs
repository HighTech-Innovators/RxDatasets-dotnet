using System.Collections.Immutable;

namespace RxDatasets.Models;

public record ImmutableDatasetState(ImmutableDictionary<string, ImmutableSnapshotEntity> Entities, ulong SnapshotVersion, ulong LatestVersion)
{
    public static ImmutableDatasetState FromProto(SnapshotIndex snapshotIndex)
    {
        Dictionary<string, ImmutableSnapshotEntity> entities = new Dictionary<string, ImmutableSnapshotEntity>();
        snapshotIndex.Entities.ToList().ForEach(x => entities.Add(x.Key, ImmutableSnapshotEntity.FromProto(x.Value)));

        return new ImmutableDatasetState(entities.ToImmutableDictionary(), snapshotIndex.SnapshotVersion, snapshotIndex.SnapshotVersion);
    }

    public SnapshotIndex ToProto()
    {
        SnapshotIndex snapshotIndex = new SnapshotIndex()
        {
            SnapshotVersion = this.SnapshotVersion
        };

        foreach(var entity in Entities)
        {
            snapshotIndex.Entities.Add(entity.Key, entity.Value.ToProto());
        }

        return snapshotIndex;
    }
}
