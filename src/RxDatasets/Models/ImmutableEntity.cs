using System.Collections.Immutable;

namespace RxDatasets.Models;

public record ImmutableEntity(ImmutableArray<ulong> LastDeletedVersion, ImmutableArray<ulong> LastInsertedVersion, ulong LastUpdatedVersion, long Position, int Length, uint Crc32 = 0)
{
    public SnapshotEntity ToProto()
    {
        return new SnapshotEntity
        {
            LastDeletedVersion = { LastDeletedVersion },
            LastInsertedVersion = { LastInsertedVersion },
            LastUpdatedVersion = LastUpdatedVersion,
            Position = Position,
            Length = Length,
            Crc32 = Crc32
        };
    }

    public static ImmutableEntity FromProto(SnapshotEntity snapshotEntity)
    {
        return new ImmutableEntity(snapshotEntity.LastDeletedVersion.ToImmutableArray(),
            snapshotEntity.LastInsertedVersion.ToImmutableArray(),
            snapshotEntity.LastUpdatedVersion,
            snapshotEntity.Position,
            snapshotEntity.Length,
            snapshotEntity.Crc32);
    }
}
