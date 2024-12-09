using System.Collections.Immutable;

namespace RxDatasets.Models;

public record ImmutableSnapshotEntity(ImmutableArray<ulong> LastDeletedVersion, ImmutableArray<ulong> LastInsertedVersion, ulong LastUpdatedVersion, long Position, int Length, uint Crc32 = 0)
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

    public static ImmutableSnapshotEntity FromProto(SnapshotEntity snapshotEntity)
    {
        return new ImmutableSnapshotEntity([..snapshotEntity.LastDeletedVersion],
            [..snapshotEntity.LastInsertedVersion],
            snapshotEntity.LastUpdatedVersion,
            snapshotEntity.Position,
            snapshotEntity.Length,
            snapshotEntity.Crc32);
    }
}

public record ImmutableDeltaEntity(string Name, long Position, int Length, uint Crc32 = 0)
{
    public DeltaEntity ToProto()
    {
        return new DeltaEntity
        {
            Name =  Name ,
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