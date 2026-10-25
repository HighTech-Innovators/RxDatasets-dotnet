using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Notifications;
using RxDatasets.StorageProviders;

namespace RxDatasets.StorageProvider.GoogleCloud;

/// <summary>
/// Stores deltas and snapshots as rows of <see cref="BigtableRowStore"/>, payloads too large for a row in the bucket.
/// <para>
/// There is no DeltaInfo or SnapshotInfo: the rows are the record of which versions exist, and the write of a delta's
/// row is its commit. A writer whose index is behind (an earlier lock holder that did not notice it lost the lock)
/// cannot store a delta: its version either has a row already, or is at or below a snapshot (its row may have been
/// cleaned up). It fails, and the dataset is reloaded. Everything else (lock, processed versions, lifecycle) is
/// <see cref="ObjectStorageProvider{T}"/>'s, with the bucket as its <see cref="IDiskStorage{T}"/>.
/// </para>
/// </summary>
public class BigtableStorageProvider<T>(
    ILogger<BigtableStorageProvider<T>> logger,
    IOptions<StorageConfig> storageConfig,
    IOptions<GoogleCloudConfig> googleCloudConfig,
    IDiskStorage<T> bucket,
    BigtableRowStore rowStore,
    INotificationsHub<T> notificationsHub,
    IStorageReader<T> storageReader,
    bool startIfInitFails = true,
    DistributedLockService<T>? lockService = null)
    : ObjectStorageProvider<T>(logger, storageConfig, bucket, notificationsHub, storageReader, startIfInitFails, lockService)
{
    private const int DeleteChunk = 10_000;
    private readonly string _prefix = DatasetHelper.GetObjectNamePrefix<T>(storageConfig.Value);
    private readonly IDiskStorage<T> _bucket = bucket;
    private readonly INotificationsHub<T> _notificationsHub = notificationsHub;
    private readonly DistributedLockService<T>? _lockService = lockService;

    public override Task StoreDelta(ulong deltaVersion, byte[] index, byte[] data) =>
        StoreDelta(deltaVersion, index, new MemoryStream(data, writable: false));

    public override async Task StoreDelta(ulong deltaVersion, byte[] index, Stream data)
    {
        ThrowIfNoLockIsAcquired();
        using var slot = await DeltaLockAsync();

        BigtableArtifact artifact = await ToArtifact(ObjectNameTypeEnum.Delta, deltaVersion, index, data);
        // A snapshot at or above the version means it was written already, even when cleaning removed its row since.
        // ponytail: a check, then the insert; closing the gap would need the snapshot and the cleaning in between
        bool stale = (await rowStore.Versions(_prefix, ObjectNameTypeEnum.Snapshot, deltaVersion, rowsLimit: 1)).Count > 0;
        if (stale || !await rowStore.TryInsert(_prefix, ObjectNameTypeEnum.Delta, artifact))
        {
            logger.LogError("Delta {Version} of {Dataset} was stored by another writer; reloading the dataset",
                deltaVersion, _prefix);
            await DeleteFromBucket(Objects(artifact));
            // Reads the versions again and notifies Ready, on which the writer and reader reload their index. Not without
            // the lock: the writer would report ready while it cannot write.
            if (_lockService?.HasLock != false)
                await Initialize(CancellationToken.None);
            throw new InvalidOperationException(
                $"Delta {deltaVersion} of {_prefix} was stored by another writer; the dataset is being reloaded");
        }

        DeltaInfo.LatestDeltaVersion = deltaVersion;
        if (DeltaInfo.EarliestDeltaVersion == 0)
            DeltaInfo.EarliestDeltaVersion = deltaVersion;
        await _notificationsHub.Notify(new DatasetUpdatedNotification(deltaVersion));
    }

    public override Task StoreSnapshot(ulong snapshotVersion, byte[] index, byte[] data) =>
        StoreSnapshot(snapshotVersion, index, new MemoryStream(data, writable: false));

    public override async Task StoreSnapshot(ulong snapshotVersion, byte[] index, Stream data)
    {
        ThrowIfNoLockIsAcquired();
        using var slot = await SnapshotLockAsync();

        BigtableArtifact artifact = await ToArtifact(ObjectNameTypeEnum.Snapshot, snapshotVersion, index, data);
        await rowStore.Upsert(_prefix, ObjectNameTypeEnum.Snapshot, artifact);

        if (Snapshots.All(s => s.Version != snapshotVersion))
            Snapshots.Add(new SnapshotType(snapshotVersion, artifact.CreatedAt));
        await _notificationsHub.Notify(new SnapshotCreatedNotification(Snapshots.Max(s => s.Version)));
    }

    public override async Task DeleteOlderThan(ulong version, bool keepAllDeltas)
    {
        ThrowIfNoLockIsAcquired();
        using var slotSnapshot = await SnapshotLockAsync();

        if (Snapshots.All(x => x.Version != version))
            throw new InvalidOperationException($"Snapshot version {version} does not exist");

        logger.LogInformation("Storage cleaning of {Dataset}: deleting anything older than version {Version}",
            _prefix, version);
        await DeleteRows(ObjectNameTypeEnum.Snapshot, version == 0 ? 0 : version - 1);
        Snapshots.RemoveAll(s => s.Version < version);

        if (keepAllDeltas || DeltaInfo.LatestDeltaVersion == 0)
            return;

        using var slotDelta = await DeltaLockAsync();
        // The newest delta row stays: the scan for the latest version starts at it
        ulong lastToDelete = Math.Min(version, DeltaInfo.LatestDeltaVersion - 1);
        await DeleteRows(ObjectNameTypeEnum.Delta, lastToDelete);
        DeltaInfo.EarliestDeltaVersion = Math.Max(DeltaInfo.EarliestDeltaVersion, lastToDelete + 1);
    }

    /// <summary>
    /// Deletes the rows up to <paramref name="toVersion"/> in chunks (one MutateRows takes at most 100k), then every bucket
    /// object of those versions: rows first, so a row that is still there always has its payloads. Deleting by version
    /// folder, not by what the rows point at, also removes the objects of writes that never got a row (a crash between
    /// upload and row, a lost race), and those of an earlier cleaning that stopped halfway.
    /// </summary>
    private async Task DeleteRows(ObjectNameTypeEnum type, ulong toVersion)
    {
        List<ulong> versions = await rowStore.Versions(_prefix, type, 0);
        foreach (ulong[] chunk in versions.TakeWhile(v => v <= toVersion).Chunk(DeleteChunk))
            await rowStore.Delete(_prefix, type, chunk);

        string folder = BigtableRowStore.RowKeyPrefix(_prefix, type);
        IEnumerable<string> objects = (await _bucket.ListObjects(folder))
            .Select(o => o.ObjectKey)
            .Where(key => ulong.TryParse(key[folder.Length..].Split('/')[0],
                out ulong version) && version <= toVersion);
        await DeleteFromBucket(objects);
    }

    private static IEnumerable<string> Objects(BigtableArtifact artifact) =>
        new[] { artifact.IndexObject, artifact.DataObject }.OfType<string>();

    private async Task DeleteFromBucket(IEnumerable<string> objectKeys)
    {
        foreach (string objectKey in objectKeys)
        {
            try
            {
                await _bucket.DeleteFile(objectKey);
            }
            catch (FileNotFoundException)
            {
            }
        }
    }

    /// <summary>
    /// Builds the row of a delta or snapshot, and first uploads the payloads too large for it to the bucket, so they
    /// exist before the row does.
    /// </summary>
    private async Task<BigtableArtifact> ToArtifact(ObjectNameTypeEnum type, ulong version, byte[] index, Stream data)
    {
        GoogleCloudConfig config = googleCloudConfig.Value;
        long dataLength = data.Length;
        if (data.CanSeek)
            data.Position = 0;

        (byte[]? indexBytes, string? indexEncoding) = BigtableRowStore.Compress(index, config.CompressPayloads);
        string? indexObject = null;
        if (indexBytes.Length > config.MaxInlinePayloadBytes)
        {
            indexObject = await Upload(type, version, "index.pb", new MemoryStream(indexBytes, writable: false));
            indexBytes = null;
        }

        byte[]? dataBytes = null;
        string? dataEncoding = null;
        string? dataObject = null;
        if (dataLength > config.MaxInlinePayloadBytes)
        {
            dataObject = await Upload(type, version, "data.pbs", data); // uncompressed, so entities can be read by range
        }
        else if (dataLength > 0)
        {
            using var buffer = new MemoryStream((int)dataLength);
            await data.CopyToAsync(buffer);
            (dataBytes, dataEncoding) = BigtableRowStore.Compress(buffer.ToArray(), config.CompressPayloads);
        }

        return new BigtableArtifact(version, DateTimeOffset.UtcNow, dataLength, indexBytes, dataBytes, indexObject,
            dataObject, indexEncoding, dataEncoding);
    }

    private async Task<string> Upload(ObjectNameTypeEnum type, ulong version, string name, Stream payload)
    {
        // A unique name per attempt: a writer that loses the race for a version must not overwrite the payload the
        // row of the winner points at. UploadFileStream is never queued, so the object exists when this returns.
        string objectKey = $"{BigtableRowStore.RowKeyPrefix(_prefix, type)}{version}/{Guid.NewGuid():N}-{name}";
        await _bucket.UploadFileStream(objectKey, payload);
        return objectKey;
    }
}
