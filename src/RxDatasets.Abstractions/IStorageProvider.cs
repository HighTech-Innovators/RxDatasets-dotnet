namespace RxDatasets.Abstractions;

public interface IStorageProvider
{
    public Task StoreSnapshot(ulong snapshotVersion, byte[] index, FileStream data);
    public Task StoreDelta(ulong deltaVersion, byte[] index, byte[] data);
    public Task StoreDelta(ulong deltaVersion, byte[] index, FileStream data);
    public Task<ReadOnlyMemory<byte>> ReadDeltaIndex(ulong version);
    public Task<ReadOnlyMemory<byte>> ReadDeltaData(ulong deltaVersion, long position, int length);
    public Task<ReadOnlyMemory<byte>> ReadSnapshotIndex(ulong version);
    public Task<ReadOnlyMemory<byte>> ReadSnapshotData(ulong snapshotVersion, long position, int length);
    public Task ReadSnapshotData(ulong snapshotVersion, Action<Stream> callback);
    public Task DeleteOlderThan(ulong snapshotVersion, bool keepAllDeltas);
    public Task<(ulong oldestDelta, ulong newestDelta, List<(ulong version, DateTimeOffset DateTime)> snapshots)> ListVersions();
    public Task StoreProcessedVersion(string who, ulong version);
    public Task<ulong> GetProcessedVersion(string who);
    public Task<ulong> GetLowestProcessedVersion();
    public Task<ulong> GetLastPresentVersion();
}
public interface IStorageProvider<T> : IStorageProvider {}
