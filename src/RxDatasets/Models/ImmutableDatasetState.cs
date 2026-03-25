namespace RxDatasets.Models;

public record ImmutableDatasetState(EntityTrie Entities, ulong SnapshotVersion, ulong LatestVersion)
{
    public static ImmutableDatasetState FromProto(SnapshotIndex snapshotIndex)
    {
        var builder = new EntityTrie.Builder();

        if (snapshotIndex.Nested != null
            && (snapshotIndex.Nested.Leaves.Count > 0 || snapshotIndex.Nested.Children.Count > 0))
        {
            // New nested trie format
            NestedIndexHelper.ReadNestedSnapshot(snapshotIndex.Nested, builder);
        }
        else
        {
            // Legacy flat format
            foreach (var kvp in snapshotIndex.Entities)
            {
                builder.Add(kvp.Key, ImmutableSnapshotEntity.FromProto(kvp.Value));
            }
        }

        var snapshotIndexSnapshotVersion = snapshotIndex.SnapshotVersion > 0 ? snapshotIndex.SnapshotVersion : snapshotIndex.SnapshotVersionLegacy;
        return new ImmutableDatasetState(builder.Build(), snapshotIndexSnapshotVersion, snapshotIndexSnapshotVersion);
    }

    public SnapshotIndex ToProto()
    {
        var snapshotIndex = new SnapshotIndex()
        {
            SnapshotVersionLegacy = 0,
            SnapshotVersion = this.SnapshotVersion,
            Nested = NestedIndexHelper.ToNestedSnapshot(Entities)
        };

        // Flat entities field left empty — new readers use nested

        return snapshotIndex;
    }
}
