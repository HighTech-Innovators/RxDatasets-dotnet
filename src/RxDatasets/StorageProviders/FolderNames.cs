namespace RxDatasets.StorageProviders;

public static class FolderNames
{
    public const string SnapshotsFolderName = "snapshots";
    public const string DeltasFolderName = "deltas";

    public static string SnapshotsFolderNameWithSlash => SnapshotsFolderName + "/";
    public static string DeltasFolderNameWithSlash => DeltasFolderName + "/";
}
