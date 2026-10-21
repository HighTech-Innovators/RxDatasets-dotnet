using System.Collections.Immutable;
using System.Diagnostics.Metrics;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.Models;

namespace RxDatasets;

/// <summary>
/// Checks the aliases of one write on top of the state it starts from, operation by operation in the order they
/// arrive, and gives each entity's net alias change for the delta. A claim of an alias that another live entity
/// owns at that point throws <see cref="AliasConflictException"/>: freeing an alias earlier in the same write makes a
/// later claim valid, freeing it later does not.
/// </summary>
public sealed class AliasStaging(
    ImmutableDatasetState state,
    IReadOnlyList<AliasDefinition> definitions,
    string definitionsHash)
{
    private readonly Dictionary<string, ImmutableArray<(string Name, string Value)>> _owned = new(StringComparer.Ordinal);
    private readonly Dictionary<(string Name, string Value), string?> _owners = new();
    private List<Action>? _undo; // only kept once Mark is called

    /// <summary>
    /// Stages a put: claims the aliases the definitions give for <paramref name="bytes"/>, and frees the ones the
    /// entity no longer has. Bytes the index already holds keep their owned set; they aren't evaluated again.
    /// </summary>
    /// <param name="crc">The CRC32 of <paramref name="bytes"/>, which the caller computes anyway.</param>
    /// <exception cref="AliasConflictException">An alias is owned by another entity.</exception>
    /// <exception cref="ArgumentException">A definition can't evaluate the bytes.</exception>
    public void Put(string name, byte[] bytes, uint crc)
    {
        if (definitions.Count == 0)
            return;

        Claim(name, state.TryGetEntity(name, out var entity) && entity.IsLive && entity.Crc32 != 0 && entity.Crc32 == crc
            ? entity.Aliases
            : Evaluate(definitions, name, bytes));
    }

    /// <summary>Stages a delete: frees all aliases of the entity.</summary>
    public void Release(string name)
    {
        if (OwnedBy(name).Length > 0)
            Claim(name, []);
    }

    /// <summary>Frees the aliases of every entity, for a write that replaces the whole dataset.</summary>
    public void ReleaseAll()
    {
        if (definitions.Count == 0)
            return;

        foreach (var (name, _) in state.EnumerateAll(where: HasAliases))
            Release(name);
    }

    /// <summary>Frees the aliases of the direct children of <paramref name="parent"/>, for a write that replaces them.</summary>
    public void ReleaseChildren(string parent)
    {
        if (definitions.Count == 0)
            return;

        foreach (var (name, _) in state.EnumerateAll(parent, HasAliases))
        {
            if (DatasetHelper.FindParentOfEntity(name) == parent)
                Release(name);
        }
    }

    /// <summary>A point to <see cref="Rollback"/> to, so one failed part of a write can be undone on its own.</summary>
    public int Mark() => (_undo ??= []).Count;

    public void Rollback(int mark)
    {
        for (int i = _undo!.Count - 1; i >= mark; i--)
            _undo[i]();
        _undo.RemoveRange(mark, _undo.Count - mark);
    }

    /// <summary>
    /// Puts each entity's net alias change on its entry in the flat lists of <paramref name="delta"/>, the rest in
    /// <see cref="DeltaIndex.AliasChanges"/>, and the definitions hash on the delta.
    /// </summary>
    public void AttachTo(DeltaIndex delta)
    {
        delta.AliasDefinitionsHash = definitionsHash;

        var changes = new Dictionary<string, (List<AliasRef> Added, List<AliasRef> Removed)>();
        foreach (var (name, owned) in _owned)
        {
            var before = OwnedInState(name);
            var added = owned.Except(before).ToProto().ToList();
            var removed = before.Except(owned).ToProto().ToList();
            if (added.Count > 0 || removed.Count > 0)
                changes[name] = (added, removed);
        }

        if (changes.Count == 0)
            return;

        foreach (var entity in delta.NewEntities.Concat(delta.ChangedEntities))
        {
            if (!changes.Remove(entity.Name, out var change))
                continue;
            entity.AddedAliases.Add(change.Added);
            entity.RemovedAliases.Add(change.Removed);
        }

        foreach (var entity in delta.DeletedEntities)
        {
            if (changes.Remove(entity.Name, out var change))
                entity.RemovedAliases.Add(change.Removed);
        }

        foreach (var (name, change) in changes)
        {
            delta.AliasChanges.Add(new AliasOnlyChange
            {
                Name = name, AddedAliases = { change.Added }, RemovedAliases = { change.Removed }
            });
        }
    }

    private static bool HasAliases(ImmutableSnapshotEntity entity) => entity.IsLive && !entity.Aliases.IsEmpty;

    /// <summary>The aliases the definitions give for an entity.</summary>
    /// <exception cref="ArgumentException">A definition can't evaluate the bytes.</exception>
    public static ImmutableArray<(string Name, string Value)> Evaluate(IReadOnlyList<AliasDefinition> definitions,
        string name, ReadOnlyMemory<byte> bytes)
    {
        var aliases = ImmutableArray.CreateBuilder<(string Name, string Value)>();
        foreach (var definition in definitions)
        {
            try
            {
                aliases.AddRange(definition.Factory(name, bytes).Distinct().Select(value => (definition.Name, value)));
            }
            catch (Exception e)
            {
                throw new ArgumentException($"Can't evaluate the '{definition.Name}' aliases of '{name}': {e.Message}", e);
            }
        }

        return aliases.DrainToImmutable();
    }

    private void Claim(string name, ImmutableArray<(string Name, string Value)> aliases)
    {
        foreach (var alias in aliases)
        {
            if (OwnerOf(alias) is { } owner && owner != name)
                throw new AliasConflictException(alias.Name, alias.Value, name, owner);
        }

        foreach (var alias in OwnedBy(name))
        {
            if (!aliases.Contains(alias) && OwnerOf(alias) == name)
                Set(_owners, alias, null);
        }

        foreach (var alias in aliases)
            Set(_owners, alias, name);
        Set(_owned, name, aliases);
    }

    private string? OwnerOf((string Name, string Value) alias) =>
        _owners.TryGetValue(alias, out var owner) ? owner : state.ResolveAlias(alias.Name, alias.Value);

    private ImmutableArray<(string Name, string Value)> OwnedBy(string name) =>
        _owned.TryGetValue(name, out var owned) ? owned : OwnedInState(name);

    private ImmutableArray<(string Name, string Value)> OwnedInState(string name) =>
        state.TryGetEntity(name, out var entity) && entity.IsLive ? entity.Aliases : [];

    private void Set<TKey, TValue>(Dictionary<TKey, TValue> map, TKey key, TValue value) where TKey : notnull
    {
        _undo?.Add(map.TryGetValue(key, out var previous) ? () => map[key] = previous : () => map.Remove(key));
        map[key] = value;
    }
}

public static class AliasMetrics
{
    public const string MeterName = "RxDatasets.Aliases";
    private static readonly Meter Meter = new(MeterName);

    /// <summary>
    /// Aliases that a rebuild after changed alias definitions found on more than one entity, and took from all but one.
    /// </summary>
    public static readonly Counter<long> RebuildConflicts = Meter.CreateCounter<long>("rxdatasets.aliases.rebuild_conflicts",
        description: "Aliases claimed by more than one entity when the aliases were recomputed for changed definitions");

    /// <summary>Entities that a rebuild after changed alias definitions couldn't evaluate, and left without aliases.</summary>
    public static readonly Counter<long> RebuildUnparseable = Meter.CreateCounter<long>(
        "rxdatasets.aliases.rebuild_unparseable",
        description: "Entities the alias definitions couldn't evaluate when the aliases were recomputed, left without aliases");
}
