namespace RxDatasets.Models;

public record ImmutableDatasetState(EntityTrie Entities, ulong SnapshotVersion, ulong LatestVersion)
{
    public static ImmutableDatasetState FromProto(SnapshotIndex snapshotIndex, NameCompressor? compressor = null)
    {
        var builder = new EntityTrie.Builder(compressor);

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
                if (!IndexEntity.IsDeletedInProto(kvp.Value))
                    builder.Add(kvp.Key, IndexEntity.FromProto(kvp.Value));
            }
        }

        var snapshotIndexSnapshotVersion = snapshotIndex.SnapshotVersion > 0 ? snapshotIndex.SnapshotVersion : snapshotIndex.SnapshotVersionLegacy;
        return new ImmutableDatasetState(builder.Build(), snapshotIndexSnapshotVersion, snapshotIndexSnapshotVersion);
    }
}
