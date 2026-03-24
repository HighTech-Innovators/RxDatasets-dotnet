// ReSharper disable UnusedTypeParameter
namespace RxDatasets.Abstractions;

public interface IStorageProviderReader
{
    Task<ReadOnlyMemory<byte>> ReadDeltaIndex(ulong version);
    Task<ReadOnlyMemory<byte>> ReadSnapshotIndex(ulong version);
    Task ReadSnapshotData(ulong snapshotVersion, Action<Stream> callback);
    Task ReadSnapshotDataAsync(ulong snapshotVersion, Func<Stream, Task> asyncCallback);
    Task<ReadOnlyMemory<byte>> ReadSnapshotData(ulong snapshotVersion);

    Task<(ulong OldestDelta, ulong NewestDelta, List<SnapshotType> Snapshots)> ListVersions();
    Task<ReadOnlyMemory<byte>> ReadDataFromSources(
        List<(ObjectNameTypeEnum type, ulong version, long position)> sources, int length);
    Task<long> GetSnapshotDataContentLength(ulong snapshotVersion);

    /// <summary>
    /// Reads delta indices for a range of versions in a single operation.
    /// Backends that support range scans (e.g. Bigtable) can override this for efficiency.
    /// </summary>
    async IAsyncEnumerable<(ulong Version, ReadOnlyMemory<byte> IndexBytes)> ReadDeltaIndexRange(
        ulong fromVersionExclusive,
        ulong toVersionInclusive,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        for (ulong v = fromVersionExclusive + 1; v <= toVersionInclusive; v++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var bytes = await ReadDeltaIndex(v);
            yield return (v, bytes);
        }
    }
}

public interface IStorageProviderWriter
{
    Task StoreSnapshot(ulong snapshotVersion, byte[] index, Stream data);
    Task StoreSnapshot(ulong snapshotVersion, byte[] index, byte[] data);
    Task StoreDelta(ulong deltaVersion, byte[] index, byte[] data);
    Task StoreDelta(ulong deltaVersion, byte[] index, Stream data);
    Task DeleteOlderThan(ulong snapshotVersion, bool keepAllDeltas);
    Task StoreProcessedVersion(string who, ulong version);
}


public interface IStorageProvider : IStorageProviderReader, IStorageProviderWriter
{
    Task<ulong> GetProcessedVersion(string who);
    Task<ulong> GetLowestProcessedVersion();
}

public interface IStorageProvider<T> : IReadOnlyStorageProvider<T>, IStorageProvider { }
public interface IReadOnlyStorageProvider<T> : IStorageProviderReader { }
public interface IStorageReader<T> : IStorageProviderReader { }
