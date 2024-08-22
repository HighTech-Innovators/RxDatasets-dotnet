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
        return new ImmutableEntity([..snapshotEntity.LastDeletedVersion],
            [..snapshotEntity.LastInsertedVersion],
            snapshotEntity.LastUpdatedVersion,
            snapshotEntity.Position,
            snapshotEntity.Length,
            snapshotEntity.Crc32);
    }

    //todo: add delta entity creation to here
}
