using System.Collections.Immutable;

namespace HTI.RxDatasets.Models;

public record ImmutableDatasetState(ImmutableDictionary<string, ImmutableEntity> Entities, ulong LatestSnapshotVersion, ulong LatestVersion)
{
    public static ImmutableDatasetState FromProto(SnapshotIndex snapshotIndex)
    {
        Dictionary<string, ImmutableEntity> entities = new Dictionary<string, ImmutableEntity>();
        snapshotIndex.Entities.ToList().ForEach(x => entities.Add(x.Key, ImmutableEntity.FromProto(x.Value)));

        return new ImmutableDatasetState(entities.ToImmutableDictionary(), snapshotIndex.SnapshotVersion, snapshotIndex.SnapshotVersion);
    }

    public SnapshotIndex ToProto()
    {
        SnapshotIndex snapshotIndex = new SnapshotIndex()
        {
            SnapshotVersion = LatestSnapshotVersion
        };

        foreach(var entity in Entities)
        {
            snapshotIndex.Entities.Add(entity.Key, entity.Value.ToProto());
        }

        return snapshotIndex;
    }
}
