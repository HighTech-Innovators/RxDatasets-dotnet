using Google.Protobuf.Collections;

namespace RxDatasets.Models;

public record MutableDatasetState(SnapshotIndex InnerIndex, ulong LatestVersion)
{
    public MapField<string, SnapshotEntity> Entities => InnerIndex.Entities;
    public ulong SnapshotVersion
    {
        get
        {
            return InnerIndex.SnapshotVersion > 0 ? InnerIndex.SnapshotVersion : InnerIndex.SnapshotVersionLegacy;
        }
        set
        {
            InnerIndex.SnapshotVersion = value;
            InnerIndex.SnapshotVersionLegacy = 0;
        }
    }

    public static MutableDatasetState FromProto(SnapshotIndex snapshotIndex)
    {
        var snapshotIndexSnapshotVersion = snapshotIndex.SnapshotVersion > 0 ? snapshotIndex.SnapshotVersion : snapshotIndex.SnapshotVersionLegacy;
        return new MutableDatasetState(snapshotIndex, snapshotIndexSnapshotVersion);
    }

    public SnapshotIndex ToProto() => InnerIndex;
}
