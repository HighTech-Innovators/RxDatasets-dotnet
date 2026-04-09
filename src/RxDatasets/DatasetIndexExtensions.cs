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
        var trie = index.Entities;
        var deltaVersion = delta.DeltaVersion > 0 ? delta.DeltaVersion : delta.DeltaVersionLegacy;

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

    public static ImmutableDatasetState ApplyDeltas(this ImmutableDatasetState index, IEnumerable<DeltaIndex> deltas)
    {
        var tempIndex = index;

        foreach (var delta in deltas)
        {
            tempIndex = tempIndex.ApplyDelta(delta);
        }

        return tempIndex;
    }
}
