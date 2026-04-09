namespace RxDatasets.Models;

public record ImmutableDatasetState(EntityTrie Entities, ulong SnapshotVersion, ulong LatestVersion)
{
    public static ImmutableDatasetState FromProto(SnapshotIndex snapshotIndex, NameCompressor? compressor = null)
    {
        // Auto-detect compressor from on-disk id_pattern if not explicitly provided.
        // This handles reading compressed protos even when no pattern was configured in code.
        if (compressor == null && snapshotIndex.IdPattern.Length > 0)
            compressor = NameCompressor.FromPattern(snapshotIndex.IdPattern);

        EntityTrie trie;

        if (snapshotIndex.Nested != null
            && (snapshotIndex.Nested.Leaves.Count > 0 || snapshotIndex.Nested.Children.Count > 0))
        {
            // Check if proto compression state matches the runtime compressor —
            // when they match, we can map the proto tree directly to the trie tree
            // without reconstructing or re-splitting any entity name strings.
            var protoPattern = snapshotIndex.IdPattern;
            var runtimePattern = compressor?.Pattern ?? "";

            if (protoPattern == runtimePattern)
            {
                // Direct tree-to-tree: proto SnapshotBucket → frozen TrieNode
                trie = EntityTrie.FromSnapshotBucket(snapshotIndex.Nested, compressor);
            }
            else
            {
                // Transition case (e.g. pattern changed, or old uncompressed snapshot
                // being read with a compressor active): must reconstruct names and re-compress
                var builder = new EntityTrie.Builder(compressor);
                NestedIndexHelper.ReadNestedSnapshot(snapshotIndex.Nested, builder);
                trie = builder.Build();
            }
        }
        else
        {
            // Legacy flat format
            var builder = new EntityTrie.Builder(compressor);
            foreach (var kvp in snapshotIndex.Entities)
            {
                if (!IndexEntity.IsDeletedInProto(kvp.Value))
                    builder.Add(kvp.Key, IndexEntity.FromProto(kvp.Value));
            }
            trie = builder.Build();
        }

        var snapshotIndexSnapshotVersion = snapshotIndex.SnapshotVersion > 0 ? snapshotIndex.SnapshotVersion : snapshotIndex.SnapshotVersionLegacy;
        return new ImmutableDatasetState(trie, snapshotIndexSnapshotVersion, snapshotIndexSnapshotVersion);
    }
}
