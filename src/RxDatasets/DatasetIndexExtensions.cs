using RxDatasets.Models;

namespace RxDatasets;

/// <summary>
/// Extension methods for applying delta indices to <see cref="ImmutableDatasetState"/>
/// using the slim <see cref="IndexEntity"/> (no version-history arrays).
/// Deleted entities are removed from the trie instead of being kept with a Deleted marker.
///
/// When a delta has a <see cref="DeltaIndex.Nested"/> tree and the <c>id_pattern</c> matches
/// the trie's compressor, mutations are applied via structural tree merge — walking the
/// DeltaBucket and trie trees in parallel with zero name string construction or splitting.
/// </summary>
public static class DatasetIndexExtensions
{
    /// <summary>
    /// Percentage of the trie entity count above which we switch from immutable path-copy
    /// to a full builder thaw/freeze. Path-copy is O(changes × depth × branchFactor) per
    /// entity; builder is O(totalNodes) fixed cost for thaw + freeze. When the delta touches
    /// a significant fraction of the trie, the builder amortises better.
    /// </summary>
    private const double BuilderThresholdFraction = 0.05;

    /// <summary>
    /// Applies a single delta index. Adaptively chooses between immutable path-copy
    /// (efficient for small deltas) and builder thaw/freeze (efficient when the delta
    /// touches a significant fraction of the trie).
    /// </summary>
    public static ImmutableDatasetState ApplyDelta(this ImmutableDatasetState index, DeltaIndex delta)
    {
        int changeCount = delta.NewEntities.Count + delta.ChangedEntities.Count
                        + delta.DeletedNames.Count + delta.DeletedEntities.Count;
        int threshold = Math.Max(1, (int)(index.Entities.Count * BuilderThresholdFraction));

        if (changeCount >= threshold)
        {
            return ApplyDeltaViaBuilder(index, delta);
        }

        return ApplyDeltaImmutable(index, delta);
    }

    private static ImmutableDatasetState ApplyDeltaImmutable(ImmutableDatasetState index, DeltaIndex delta)
    {
        var deltaVersion = delta.DeltaVersion > 0 ? delta.DeltaVersion : delta.DeltaVersionLegacy;

        // Structural tree merge: walk DeltaBucket + TrieNode trees in parallel
        if (CanUseNestedMerge(delta, index.Entities))
        {
            var merged = index.Entities.MergeNestedDelta(delta.Nested!, deltaVersion);
            return new ImmutableDatasetState(merged, index.SnapshotVersion, deltaVersion);
        }

        // Fallback: per-entity immutable path-copy
        var trie = index.Entities;

        foreach (var newEntity in delta.NewEntities)
        {
            trie = trie.SetItem(newEntity.Name,
                new IndexEntity(deltaVersion, newEntity.Position, newEntity.Length, newEntity.Crc32));
        }

        foreach (var changedEntity in delta.ChangedEntities)
        {
            trie = trie.SetItem(changedEntity.Name,
                new IndexEntity(deltaVersion, changedEntity.Position, changedEntity.Length, changedEntity.Crc32));
        }

        foreach (var deletedName in delta.DeletedNames)
        {
            trie = trie.RemoveItem(deletedName);
        }

        foreach (var deletedEntity in delta.DeletedEntities)
        {
            trie = trie.RemoveItem(deletedEntity.Name);
        }

        return new ImmutableDatasetState(trie, index.SnapshotVersion, deltaVersion);
    }

    private static ImmutableDatasetState ApplyDeltaViaBuilder(ImmutableDatasetState index, DeltaIndex delta)
    {
        var builder = index.Entities.ToBuilder();
        var deltaVersion = ApplyDeltaToBuilder(builder, delta, index.Entities.Compressor?.Pattern ?? "");
        return new ImmutableDatasetState(builder.Build(), index.SnapshotVersion, deltaVersion);
    }

    /// <summary>
    /// Applies multiple deltas efficiently by thawing the trie once, applying all deltas
    /// mutably, then freezing once. Eliminates O(N × deltas) intermediate immutable allocations.
    /// </summary>
    public static ImmutableDatasetState ApplyDeltas(this ImmutableDatasetState index, IEnumerable<DeltaIndex> deltas)
    {
        var builder = index.Entities.ToBuilder();
        var compressorPattern = index.Entities.Compressor?.Pattern ?? "";
        ulong latestVersion = index.LatestVersion;

        foreach (var delta in deltas)
        {
            latestVersion = ApplyDeltaToBuilder(builder, delta, compressorPattern);
        }

        return new ImmutableDatasetState(builder.Build(), index.SnapshotVersion, latestVersion);
    }

    private static ulong ApplyDeltaToBuilder(EntityTrie.Builder builder, DeltaIndex delta, string compressorPattern)
    {
        var deltaVersion = delta.DeltaVersion > 0 ? delta.DeltaVersion : delta.DeltaVersionLegacy;

        // Structural tree merge: walk DeltaBucket + MutableTrieNode trees in parallel
        if (delta.Nested != null && delta.IdPattern == compressorPattern)
        {
            builder.MergeNestedDelta(delta.Nested, deltaVersion);
            return deltaVersion;
        }

        // Fallback: per-entity flat path (handles pattern transitions, legacy format)
        foreach (var newEntity in delta.NewEntities)
        {
            builder.SetItem(newEntity.Name,
                new IndexEntity(deltaVersion, newEntity.Position, newEntity.Length, newEntity.Crc32));
        }

        foreach (var changedEntity in delta.ChangedEntities)
        {
            builder.SetItem(changedEntity.Name,
                new IndexEntity(deltaVersion, changedEntity.Position, changedEntity.Length, changedEntity.Crc32));
        }

        foreach (var deletedName in delta.DeletedNames)
        {
            builder.Remove(deletedName);
        }

        foreach (var deletedEntity in delta.DeletedEntities)
        {
            builder.Remove(deletedEntity.Name);
        }

        return deltaVersion;
    }

    /// <summary>
    /// Returns true when the delta's nested tree structure can be merged directly
    /// into the trie — requires the nested tree to be present and the compression
    /// patterns to match so segment names align.
    /// </summary>
    private static bool CanUseNestedMerge(DeltaIndex delta, EntityTrie trie)
    {
        return delta.Nested != null
            && delta.IdPattern == (trie.Compressor?.Pattern ?? "");
    }
}
