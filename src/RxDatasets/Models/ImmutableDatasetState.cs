namespace RxDatasets.Models;

public record ImmutableDatasetState(EntityTrie Entities, ulong SnapshotVersion, ulong LatestVersion)
{
    public static ImmutableDatasetState FromProto(SnapshotIndex snapshotIndex)
    {
        var builder = new EntityTrie.Builder();
        foreach (var kvp in snapshotIndex.Entities)
        {
            builder.Add(kvp.Key, ImmutableSnapshotEntity.FromProto(kvp.Value));
        }

        var snapshotIndexSnapshotVersion = snapshotIndex.SnapshotVersion > 0 ? snapshotIndex.SnapshotVersion : snapshotIndex.SnapshotVersionLegacy;
        return new ImmutableDatasetState(builder.Build(), snapshotIndexSnapshotVersion, snapshotIndexSnapshotVersion);
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
