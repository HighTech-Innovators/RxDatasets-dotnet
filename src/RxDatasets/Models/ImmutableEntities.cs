using System.Collections.Immutable;
using RxDatasets.Abstractions;

namespace RxDatasets.Models;

/// <summary>
/// A live entity in the index: the current state only. <see cref="Version"/> is the version of its latest change. How
/// it got there (earlier versions, deletes) is in the deltas, and a deleted entity is not in the index.
/// </summary>
public record ImmutableSnapshotEntity(ulong Version, long Position, int Length, uint Crc32 = 0)
{
    /// <summary>The aliases the entity owns: (alias name, alias value).</summary>
    public ImmutableArray<(string Name, string Value)> Aliases { get; init; } = [];

    /// <summary>
    /// The version goes in <c>last_inserted_version</c>, the only list entry: a reader of the earlier format then
    /// reads a live entity created at that version, and a snapshot replay reports it as created.
    /// </summary>
    public SnapshotEntity ToProto()
    {
        return new SnapshotEntity
        {
            LastInsertedVersion = { Version },
            Position = Position,
            Length = Length,
            Crc32 = Crc32,
            Aliases = { Aliases.ToProto() }
        };
    }

    /// <summary>
    /// The entity as a snapshot of either format records it; <c>null</c> for a tombstone, which an earlier writer kept
    /// for a deleted entity.
    /// </summary>
    public static ImmutableSnapshotEntity? FromProto(SnapshotEntity snapshotEntity)
    {
        if (snapshotEntity.GetLatestChangeType() == ChangeType.Deleted)
            return null;

        return new ImmutableSnapshotEntity(snapshotEntity.GetLatestEntityVersion(),
            snapshotEntity.Position,
            snapshotEntity.Length,
            snapshotEntity.Crc32)
        {
            Aliases = snapshotEntity.Aliases.FromProto()
        };
    }
}

internal static class AliasRefExtensions
{
    public static IEnumerable<AliasRef> ToProto(this IEnumerable<(string Name, string Value)> aliases) =>
        aliases.Select(alias => new AliasRef { Name = alias.Name, Value = alias.Value });

    public static ImmutableArray<(string Name, string Value)> FromProto(this IReadOnlyCollection<AliasRef> aliases) =>
        aliases.Count == 0 ? [] : [.. aliases.Select(alias => (alias.Name, alias.Value))];
}

public record ImmutableDeltaEntity(string Name, long Position, int Length, uint Crc32 = 0)
{
    public DeltaEntity ToProto()
    {
        return new DeltaEntity
        {
            Name = Name,
            Position = Position,
            Length = Length,
            Crc32 = Crc32
        };
    }

    public static ImmutableDeltaEntity FromProto(DeltaEntity deltaEntity)
    {
        return new ImmutableDeltaEntity(
            deltaEntity.Name,
            deltaEntity.Position,
            deltaEntity.Length,
            deltaEntity.Crc32);
    }
}
