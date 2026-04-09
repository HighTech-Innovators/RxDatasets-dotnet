using RxDatasets.Models;

namespace RxDatasets;

/// <summary>
/// Extension methods for applying delta indices to <see cref="ImmutableDatasetState"/>
/// using the slim <see cref="IndexEntity"/> (no version-history arrays).
/// Deleted entities are removed from the trie instead of being kept with a Deleted marker.
/// </summary>
public static class DatasetIndexExtensions
{
    public static ImmutableDatasetState ApplyDelta(this ImmutableDatasetState index, DeltaIndex delta)
    {
        var builder = index.Entities.ToBuilder();
        var deltaVersion = ApplyDeltaToBuilder(builder, delta);
        return new ImmutableDatasetState(builder.Build(), index.SnapshotVersion, deltaVersion);
    }

    /// <summary>
    /// Applies multiple deltas efficiently by thawing the trie once, applying all deltas
    /// mutably, then freezing once. Eliminates O(N × deltas) intermediate immutable allocations.
    /// </summary>
    public static ImmutableDatasetState ApplyDeltas(this ImmutableDatasetState index, IEnumerable<DeltaIndex> deltas)
    {
        var builder = index.Entities.ToBuilder();
        ulong latestVersion = index.LatestVersion;

        foreach (var delta in deltas)
        {
            latestVersion = ApplyDeltaToBuilder(builder, delta);
        }

        return new ImmutableDatasetState(builder.Build(), index.SnapshotVersion, latestVersion);
    }

    private static ulong ApplyDeltaToBuilder(EntityTrie.Builder builder, DeltaIndex delta)
    {
        var deltaVersion = delta.DeltaVersion > 0 ? delta.DeltaVersion : delta.DeltaVersionLegacy;

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
}
