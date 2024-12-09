namespace RxDatasets.Abstractions;

public interface IStorageProvider
{
    public Task StoreSnapshot(ulong snapshotVersion, byte[] index, Stream data);
    public Task StoreSnapshot(ulong snapshotVersion, byte[] index, byte[] data);
    public Task StoreDelta(ulong deltaVersion, byte[] index, byte[] data);
    public Task StoreDelta(ulong deltaVersion, byte[] index, Stream data);
    public Task<ReadOnlyMemory<byte>> ReadDeltaIndex(ulong version);
    public Task<ReadOnlyMemory<byte>> ReadSnapshotIndex(ulong version);
    public Task ReadSnapshotData(ulong snapshotVersion, Action<Stream> callback);
    public Task<ReadOnlyMemory<byte>> ReadSnapshotData(ulong snapshotVersion);
    public Task<long> GetSnapshotDataContentLength(ulong snapshotVersion);
    public Task DeleteOlderThan(ulong snapshotVersion, bool keepAllDeltas);
    public Task<(ulong oldestDelta, ulong newestDelta, List<SnapshotType> snapshots)> ListVersions();
    public Task StoreProcessedVersion(string who, ulong version);

    public Task<ReadOnlyMemory<byte>> ReadDataFromSources(
        List<(ObjectNameTypeEnum type, ulong version, long position)> sources, int length);
    public Task<ulong> GetProcessedVersion(string who);
    public Task<ulong> GetLowestProcessedVersion();
}
public interface IStorageProvider<T> :  IStorageProvider {}
