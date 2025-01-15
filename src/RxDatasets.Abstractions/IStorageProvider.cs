namespace RxDatasets.Abstractions;

public interface IStorageProviderReader
{
    public Task<ReadOnlyMemory<byte>> ReadDeltaIndex(ulong version);
    public Task<ReadOnlyMemory<byte>> ReadSnapshotIndex(ulong version);
    public Task ReadSnapshotData(ulong snapshotVersion, Action<Stream> callback);
    public Task ReadSnapshotDataAsync(ulong snapshotVersion, Func<Stream,Task> asyncCallback);
    public Task<ReadOnlyMemory<byte>> ReadSnapshotData(ulong snapshotVersion);
    
    public Task<(ulong OldestDelta, ulong NewestDelta, List<SnapshotType> Snapshots)> ListVersions();
    public Task<ReadOnlyMemory<byte>> ReadDataFromSources(
        List<(ObjectNameTypeEnum type, ulong version, long position)> sources, int length);
    public Task<long> GetSnapshotDataContentLength(ulong snapshotVersion);

}

public interface IStorageProviderWriter
{
    public Task StoreSnapshot(ulong snapshotVersion, byte[] index, Stream data);
    public Task StoreSnapshot(ulong snapshotVersion, byte[] index, byte[] data);
    public Task StoreDelta(ulong deltaVersion, byte[] index, byte[] data);
    public Task StoreDelta(ulong deltaVersion, byte[] index, Stream data);
    public Task DeleteOlderThan(ulong snapshotVersion, bool keepAllDeltas);
    public Task StoreProcessedVersion(string who, ulong version);
}


public interface IStorageProvider : IStorageProviderReader, IStorageProviderWriter
{
    public Task<ulong> GetProcessedVersion(string who);
    public Task<ulong> GetLowestProcessedVersion();
}

public interface IStorageProvider<T> :  IReadOnlyStorageProvider<T>, IStorageProvider {}
public interface IReadOnlyStorageProvider<T> :  IStorageProviderReader {}
public interface IStorageReader<T> :  IStorageProviderReader {}
