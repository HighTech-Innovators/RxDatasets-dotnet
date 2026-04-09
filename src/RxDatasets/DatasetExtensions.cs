using RxDatasets.Abstractions;
using RxDatasets.Models;

namespace RxDatasets;

public static class DatasetExtensions
{
    public static ulong GetLatestEntityVersion(this SnapshotEntity entity)
    {
        return Math.Max(entity.LastUpdatedVersion, Math.Max(entity.LastDeletedVersion.LastOrDefault(), entity.LastInsertedVersion.LastOrDefault()));
    }

    public static ChangeType GetLatestChangeType(this SnapshotEntity entity)
    {
        ulong latestVersion = entity.GetLatestEntityVersion();

        if (latestVersion == entity.LastInsertedVersion.LastOrDefault())
            return ChangeType.New;

        if (latestVersion == entity.LastUpdatedVersion)
            return ChangeType.Changed;

        return ChangeType.Deleted;
    }
}
