using System.Collections.Immutable;
using Google.Protobuf.WellKnownTypes;
using RxDatasets.Abstractions;

namespace RxDatasets.Models;

public record ImmutableSnapshotEntity(ImmutableArray<ulong> LastDeletedVersion, ImmutableArray<ulong> LastInsertedVersion, ulong LastUpdatedVersion, long Position, int Length, uint Crc32 = 0)
    : IIndexValue
{
    public bool IsLive => this.GetLatestChangeType() != ChangeType.Deleted;

    /// <summary>The aliases the entity owns: (alias name, alias value).</summary>
    public ImmutableArray<(string Name, string Value)> Aliases { get; init; } = [];

    /// <summary>
    /// The tracking id and SLA timestamp of the write that made the latest change; <c>null</c> when it passed
    /// neither, so an entity costs no more than a reference when nobody tracks.
    /// </summary>
    public ChangeOrigin? Origin { get; init; }

    public SnapshotEntity ToProto()
    {
        var proto = new SnapshotEntity
        {
            LastDeletedVersion = { LastDeletedVersion },
            LastInsertedVersion = { LastInsertedVersion },
            LastUpdatedVersion = LastUpdatedVersion,
            Position = Position,
            Length = Length,
            Crc32 = Crc32,
            Aliases = { Aliases.ToProto() },
            TrackingId = Origin?.TrackingId ?? ""
        };
        if (Origin?.SlaTimestamp is { } sla)
            proto.SlaTimestamp = Timestamp.FromDateTimeOffset(sla);
        return proto;
    }

    /// <summary>The origin of a change as a delta or snapshot records it; <c>null</c> when there is none.</summary>
    public static ChangeOrigin? OriginOf(string trackingId, Timestamp? slaTimestamp) =>
        trackingId.Length == 0 && slaTimestamp == null ? null : new ChangeOrigin(trackingId, slaTimestamp?.ToDateTimeOffset());

    public static ImmutableSnapshotEntity FromProto(SnapshotEntity snapshotEntity)
    {
        return new ImmutableSnapshotEntity([.. snapshotEntity.LastDeletedVersion],
            [.. snapshotEntity.LastInsertedVersion],
            snapshotEntity.LastUpdatedVersion,
            snapshotEntity.Position,
            snapshotEntity.Length,
            snapshotEntity.Crc32)
        {
            Aliases = snapshotEntity.Aliases.FromProto(),
            Origin = OriginOf(snapshotEntity.TrackingId, snapshotEntity.SlaTimestamp)
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
