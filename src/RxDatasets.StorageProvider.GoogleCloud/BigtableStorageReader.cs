using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;

namespace RxDatasets.StorageProvider.GoogleCloud;

/// <summary>
/// Reads the deltas and snapshots of a dataset from <see cref="BigtableRowStore"/>, and the payloads too large for a
/// row from the bucket. Which versions exist is read from the row keys.
/// </summary>
public class BigtableStorageReader<T>(
    IOptions<StorageConfig> storageConfig,
    IOptions<GoogleCloudConfig> googleCloudConfig,
    BigtableRowStore rowStore,
    IDiskStorage<T> bucket)
    : IStorageReader<T>
{
    private readonly string _prefix = DatasetHelper.GetObjectNamePrefix<T>(storageConfig.Value);

    // A row is read whole, so reading one entity reads all data of its version. Keeping that data serves the other
    // entities of the version, like those of one BatchGet, without a read each. Deltas and snapshots never change.
    // ponytail: no read coalescing; concurrent first reads of one version each read the row
    private readonly MemoryCache _inlineData = new(new MemoryCacheOptions
    {
        SizeLimit = googleCloudConfig.Value.InlineDataCacheMB * 1024L * 1024
    });

    // The newest delta seen: the scan for newer ones starts at its row, which cleaning never deletes
    private ulong _newestDelta;

    public async Task<(ulong OldestDelta, ulong NewestDelta, List<SnapshotType> Snapshots)> ListVersions()
    {
        Task<List<ulong>> oldest = rowStore.Versions(_prefix, ObjectNameTypeEnum.Delta, 0, rowsLimit: 1);
        List<SnapshotType> snapshots = (await rowStore.List(_prefix, ObjectNameTypeEnum.Snapshot))
            .Select(s => new SnapshotType(s.Version, s.CreatedAt))
            .ToList();
        ulong latestSnapshot = snapshots.Count > 0 ? snapshots.Max(s => s.Version) : 0;

        // The newest delta is at or above both the newest seen and the latest snapshot (cleaning keeps it), so the scan
        // starts there and reads only the keys written since, also on a cold start
        ulong start = Math.Max(_newestDelta, latestSnapshot);
        List<ulong> newer = await rowStore.Versions(_prefix, ObjectNameTypeEnum.Delta, start);
        if (newer.Count == 0 && start > latestSnapshot) // the newest seen is gone: the dataset was cleared
            newer = await rowStore.Versions(_prefix, ObjectNameTypeEnum.Delta, latestSnapshot);
        _newestDelta = newer.Count > 0 ? newer[^1] : 0;

        List<ulong> first = await oldest;
        return (first.Count > 0 ? first[0] : 0, _newestDelta, snapshots);
    }

    public Task<ReadOnlyMemory<byte>> ReadDeltaIndex(ulong version) => ReadIndex(ObjectNameTypeEnum.Delta, version);

    public Task<ReadOnlyMemory<byte>> ReadSnapshotIndex(ulong version) => ReadIndex(ObjectNameTypeEnum.Snapshot, version);

    public async Task<ReadOnlyMemory<byte>> ReadSnapshotData(ulong snapshotVersion)
    {
        BigtableArtifact artifact = await Read(ObjectNameTypeEnum.Snapshot, snapshotVersion, BigtablePayload.Data);
        if (artifact.DataObject != null)
            return await bucket.DownloadBytes(artifact.DataObject);
        return InlineData(ObjectNameTypeEnum.Snapshot, artifact);
    }

    public async Task ReadSnapshotData(ulong snapshotVersion, Action<Stream> callback)
    {
        BigtableArtifact artifact = await Read(ObjectNameTypeEnum.Snapshot, snapshotVersion, BigtablePayload.Data);
        if (artifact.DataObject != null)
        {
            await bucket.DownloadWithCallBack(artifact.DataObject, callback); // streams; it can exceed 2 GB
            return;
        }

        using var stream = new MemoryStream(InlineData(ObjectNameTypeEnum.Snapshot, artifact), writable: false);
        callback(stream);
    }

    public Task ReadSnapshotDataAsync(ulong snapshotVersion, Func<Stream, Task> asyncCallback)
    {
        throw new NotImplementedException();
    }

    public async Task<ReadOnlyMemory<byte>> ReadDataFromSources(
        List<(ObjectNameTypeEnum type, ulong version, long position)> sources, int length)
    {
        if (sources.Count == 0)
            throw new ArgumentException("at least one source must be filled");
        if (length == 0)
            return ReadOnlyMemory<byte>.Empty;

        foreach (var (type, version, position) in sources)
        {
            if (_inlineData.TryGetValue((type, version), out byte[]? cached))
                return cached.AsMemory((int)position, length);

            BigtableArtifact? artifact = await rowStore.Read(_prefix, type, version, BigtablePayload.Data);
            if (artifact == null)
                continue;
            if (artifact.DataObject != null)
                return await bucket.DownloadBytes(artifact.DataObject, position, length);
            return InlineData(type, artifact).AsMemory((int)position, length);
        }

        throw new FileNotFoundException(
            $"None of the sources {string.Join(", ", sources.Select(s => $"{s.type} {s.version}"))} exists");
    }

    public async Task<long> GetSnapshotDataContentLength(ulong snapshotVersion) =>
        (await Read(ObjectNameTypeEnum.Snapshot, snapshotVersion, BigtablePayload.None)).DataLength;

    private async Task<ReadOnlyMemory<byte>> ReadIndex(ObjectNameTypeEnum type, ulong version)
    {
        BigtableArtifact artifact = await Read(type, version, BigtablePayload.Index);
        byte[] index = artifact.Index
            ?? (await bucket.DownloadBytes(artifact.IndexObject
                ?? throw new FileNotFoundException($"{type} {version} has no index"))).ToArray();
        return BigtableRowStore.Decompress(index, artifact.IndexEncoding);
    }

    private byte[] InlineData(ObjectNameTypeEnum type, BigtableArtifact artifact)
    {
        byte[] data = BigtableRowStore.Decompress(artifact.Data ?? [], artifact.DataEncoding);
        _inlineData.Set((type, artifact.Version), data, new MemoryCacheEntryOptions { Size = data.Length });
        return data;
    }

    private async Task<BigtableArtifact> Read(ObjectNameTypeEnum type, ulong version, BigtablePayload payload) =>
        await rowStore.Read(_prefix, type, version, payload)
        ?? throw new FileNotFoundException($"{type} {version} of {_prefix} does not exist");
}
