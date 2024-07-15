namespace RxDatasets.DatasetUpdateNotifications;

public struct DatasetUpdateNotification
{
    public ulong Version { get; set; }
    public ulong SnapshotVersion { get; set; }
    public string DtoName { get; set; }
}
