using System.Collections.Immutable;
using RxDatasets.Abstractions;

namespace RxDatasets.Models;

public record ImmutableSnapshotEntity(ImmutableArray<ulong> LastDeletedVersion, ImmutableArray<ulong> LastInsertedVersion, ulong LastUpdatedVersion, long Position, int Length, uint Crc32 = 0)
    : IIndexValue
{
    public bool IsLive => this.GetLatestChangeType() != ChangeType.Deleted;

    /// <summary>The aliases the entity owns: (alias name, alias value).</summary>
    public ImmutableArray<(string Name, string Value)> Aliases { get; init; } = [];

    public SnapshotEntity ToProto()
    {
        return new SnapshotEntity
        {
            LastDeletedVersion = { LastDeletedVersion },
            LastInsertedVersion = { LastInsertedVersion },
            LastUpdatedVersion = LastUpdatedVersion,
            Position = Position,
            Length = Length,
            Crc32 = Crc32,
            Aliases = { Aliases.ToProto() }
        };
    }

    public static ImmutableSnapshotEntity FromProto(SnapshotEntity snapshotEntity)
    {
        return new ImmutableSnapshotEntity([.. snapshotEntity.LastDeletedVersion],
            [.. snapshotEntity.LastInsertedVersion],
            snapshotEntity.LastUpdatedVersion,
            snapshotEntity.Position,
            snapshotEntity.Length,
            snapshotEntity.Crc32)
        {
            Aliases = snapshotEntity.Aliases.FromProto()
        };
    }
}

public static class AliasRefExtensions
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
