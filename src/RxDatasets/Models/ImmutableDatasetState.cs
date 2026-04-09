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
                // being read with a compressor active): must reconstruct names and re-compress.
                // If the proto was itself compressed, expand those names first.
                var protoCompressor = protoPattern.Length > 0
                    ? NameCompressor.FromPattern(protoPattern)
                    : null;
                var builder = new EntityTrie.Builder(compressor);
                NestedIndexHelper.ReadNestedSnapshot(snapshotIndex.Nested, builder, protoCompressor);
                trie = builder.Build();
            }
        }
        else
        {
            // Legacy flat format (or rare edge case: flat keys with compression).
            // If the proto was compressed with a different pattern than runtime,
            // expand flat keys before adding to the builder.
            var flatExpander =
                snapshotIndex.IdPattern.Length > 0
                && snapshotIndex.IdPattern != (compressor?.Pattern ?? "")
                    ? NameCompressor.FromPattern(snapshotIndex.IdPattern)
                    : null;
            var builder = new EntityTrie.Builder(compressor);
            foreach (var kvp in snapshotIndex.Entities)
            {
                if (!IndexEntity.IsDeletedInProto(kvp.Value))
                {
                    var name = flatExpander != null ? flatExpander.Expand(kvp.Key) : kvp.Key;
                    builder.Add(name, IndexEntity.FromProto(kvp.Value));
                }
            }
            trie = builder.Build();
        }

        var snapshotIndexSnapshotVersion = snapshotIndex.SnapshotVersion > 0 ? snapshotIndex.SnapshotVersion : snapshotIndex.SnapshotVersionLegacy;
        return new ImmutableDatasetState(trie, snapshotIndexSnapshotVersion, snapshotIndexSnapshotVersion);
    }
}
