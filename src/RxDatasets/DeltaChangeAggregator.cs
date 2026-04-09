using RxDatasets.Abstractions;

namespace RxDatasets;

/// <summary>
/// Aggregates per-entity change types across multiple delta indices.
/// Used by <c>GetChangedNames</c> to derive changes from delta indices
/// instead of scanning the full in-memory index.
/// </summary>
public static class DeltaChangeAggregator
{
    /// <summary>
    /// Reads multiple delta indices and produces a single aggregated list of changes per entity.
    /// Deltas must be ordered by version ascending.
    /// </summary>
    public static List<(string key, ChangeType change)> AggregateChanges(IEnumerable<DeltaIndex> deltas)
    {
        var accumulated = new Dictionary<string, ChangeType>();

        foreach (var delta in deltas)
        {
            foreach (var entity in delta.NewEntities)
            {
                accumulated[entity.Name] = Combine(
                    accumulated.GetValueOrDefault(entity.Name, ChangeType.None),
                    ChangeType.New);
            }

            foreach (var entity in delta.ChangedEntities)
            {
                accumulated[entity.Name] = Combine(
                    accumulated.GetValueOrDefault(entity.Name, ChangeType.None),
                    ChangeType.Changed);
            }

            foreach (var entity in delta.DeletedEntities)
            {
                accumulated[entity.Name] = Combine(
                    accumulated.GetValueOrDefault(entity.Name, ChangeType.None),
                    ChangeType.Deleted);
            }

            foreach (var name in delta.DeletedNames)
            {
                accumulated[name] = Combine(
                    accumulated.GetValueOrDefault(name, ChangeType.None),
                    ChangeType.Deleted);
            }
        }

        return accumulated
            .Where(kvp => kvp.Value != ChangeType.None)
            .Select(kvp => (kvp.Key, kvp.Value))
            .ToList();
    }

    /// <summary>
    /// Combines two sequential change types into a single net effect.
    /// </summary>
    private static ChangeType Combine(ChangeType current, ChangeType next)
    {
        if (current == ChangeType.None)
            return next;

        return next switch
        {
            ChangeType.New => current == ChangeType.Deleted ? ChangeType.Changed : ChangeType.New,
            ChangeType.Changed => current == ChangeType.New ? ChangeType.New : ChangeType.Changed,
            ChangeType.Deleted => current == ChangeType.New ? ChangeType.None : ChangeType.Deleted,
            _ => next,
        };
    }
}
