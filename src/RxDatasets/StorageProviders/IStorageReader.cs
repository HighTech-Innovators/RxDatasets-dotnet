using RxDatasets.Abstractions;

namespace RxDatasets.StorageProviders;

public interface IStorageReader<T>
{
    public Task<ReadOnlyMemory<byte>> ReadDeltaIndex(ulong version);
    public Task<ReadOnlyMemory<byte>> ReadSnapshotIndex(ulong version);
    public Task ReadSnapshotData(ulong snapshotVersion, Action<Stream> callback);
    
    public Task ReadSnapshotDataAsync(ulong snapshotVersion, Func<Stream,Task> asyncCallback);
    public Task<ReadOnlyMemory<byte>> ReadSnapshotData(ulong snapshotVersion);
    public Task<ReadOnlyMemory<byte>> ReadDataFromSources(
        List<(ObjectNameTypeEnum type, ulong version, long position)> sources, int length);
    public Task<(ulong OldestDelta, ulong NewestDelta, List<SnapshotType> Snapshots)> ListVersions();

}