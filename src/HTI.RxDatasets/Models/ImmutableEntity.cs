using System.Collections.Immutable;

namespace HTI.RxDatasets.Models;

public record ImmutableEntity(ImmutableArray<ulong> LastDeletedVersion, ImmutableArray<ulong> LastInsertedVersion, ulong LastUpdatedVersion, long Position, int Length, int HashCode = 0)
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
            Hashcode = HashCode
        };
    }

    public static ImmutableEntity FromProto(SnapshotEntity snapshotEntity)
    {
        return new ImmutableEntity(snapshotEntity.LastDeletedVersion.ToImmutableArray(),
            snapshotEntity.LastInsertedVersion.ToImmutableArray(),
            snapshotEntity.LastUpdatedVersion,
            snapshotEntity.Position,
            snapshotEntity.Length,
            snapshotEntity.Hashcode);
    }
}
