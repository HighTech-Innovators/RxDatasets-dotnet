using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.StorageProviders;

namespace RxDatasets.StorageProvider.GoogleCloud;

public class HybridBigtableStorageReader<T>(
    ILogger<HybridBigtableStorageReader<T>> logger,
    IOptions<StorageConfig> storageConfig,
    IVersionedArtifactRowStore rowStore,
    IDiskStorage<T> blobStorage,
    ObjectStorageReader<T> legacyReader)
    : IStorageReader<T>
{
    private readonly string _datasetPrefix = DatasetHelper.GetObjectNamePrefix<T>(storageConfig.Value);

    public async Task ReadSnapshotDataAsync(ulong snapshotVersion, Func<Stream, Task> asyncCallback)
    {
        var data = await ReadSnapshotData(snapshotVersion);
        await using var stream = new MemoryStream(data.ToArray(), writable: false);
        await asyncCallback(stream);
    }

    public async Task<ReadOnlyMemory<byte>> ReadSnapshotData(ulong snapshotVersion)
    {
        var artifact = await rowStore.GetArtifact(_datasetPrefix, ObjectNameTypeEnum.Snapshot, snapshotVersion);
        if (artifact == null)
        {
            if (storageConfig.Value.LogStorageAccess)
            {
                logger.LogInformation("Falling back to Legacy GCS reader for Snapshot {Version}", snapshotVersion);
            }
            return await legacyReader.ReadSnapshotData(snapshotVersion);
        }

        return await ReadPayload(artifact, ObjectNameIndexOrDataEnum.Data);
    }

    public async Task ReadSnapshotData(ulong snapshotVersion, Action<Stream> callback)
    {
        var artifact = await rowStore.GetArtifact(_datasetPrefix, ObjectNameTypeEnum.Snapshot, snapshotVersion);
        if (artifact == null)
        {
            if (storageConfig.Value.LogStorageAccess)
            {
                logger.LogInformation("Falling back to Legacy GCS reader for Snapshot {Version}", snapshotVersion);
            }
            await legacyReader.ReadSnapshotData(snapshotVersion, callback);
            return;
        }

        if (artifact.DataLength == 0)
        {
            using var emptyStream = new MemoryStream([]);
            callback(emptyStream);
            return;
        }

        if (!string.IsNullOrEmpty(artifact.DataFallbackObjectKey))
        {
            if (storageConfig.Value.LogStorageAccess)
            {
                logger.LogInformation("Streaming Snapshot Data from GCS (Fallback Key: {Key})", artifact.DataFallbackObjectKey);
            }
            await blobStorage.DownloadWithCallBack(artifact.DataFallbackObjectKey, callback);
            return;
        }

        using var stream = new MemoryStream(artifact.InlineData ?? []);
        if (storageConfig.Value.LogStorageAccess)
        {
            logger.LogInformation("Streaming Snapshot Data from Bigtable (Inline, {Length} bytes)", stream.Length);
        }
        callback(stream);
    }

    public async Task<ReadOnlyMemory<byte>> ReadDeltaIndex(ulong version)
    {
        var artifact = await rowStore.GetArtifact(_datasetPrefix, ObjectNameTypeEnum.Delta, version);
        if (artifact == null)
        {
            if (storageConfig.Value.LogStorageAccess)
            {
                logger.LogInformation("Falling back to Legacy GCS reader for Delta {Version}", version);
            }
            return await legacyReader.ReadDeltaIndex(version);
        }

        return await ReadPayload(artifact, ObjectNameIndexOrDataEnum.Index);
    }

    public async IAsyncEnumerable<(ulong Version, ReadOnlyMemory<byte> IndexBytes)> ReadDeltaIndexRange(
        ulong fromVersionExclusive,
        ulong toVersionInclusive,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (storageConfig.Value.LogStorageAccess)
        {
            logger.LogInformation(
                "Reading delta index range v{From}..v{To} ({Count} versions) from Bigtable",
                fromVersionExclusive + 1, toVersionInclusive, toVersionInclusive - fromVersionExclusive);
        }

        var yielded = new HashSet<ulong>();
        long totalBytes = 0;

        await foreach (var artifact in rowStore.GetArtifactRange(
            _datasetPrefix, ObjectNameTypeEnum.Delta, fromVersionExclusive, toVersionInclusive, cancellationToken))
        {
            var payload = await ReadPayload(artifact, ObjectNameIndexOrDataEnum.Index);
            yielded.Add(artifact.Version);
            totalBytes += payload.Length;
            yield return (artifact.Version, payload);
        }

        if (storageConfig.Value.LogStorageAccess)
        {
            logger.LogInformation(
                "Read {Count} delta indices ({Bytes} bytes total) from Bigtable range scan",
                yielded.Count, totalBytes);
        }

        // Fall back to legacy reader for any versions missing from Bigtable
        var legacyCount = (toVersionInclusive - fromVersionExclusive) - (ulong)yielded.Count;
        if (legacyCount > 0 && storageConfig.Value.LogStorageAccess)
        {
            logger.LogInformation(
                "Falling back to Legacy GCS reader for {Count} deltas missing from Bigtable",
                legacyCount);
        }

        for (ulong v = fromVersionExclusive + 1; v <= toVersionInclusive; v++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (yielded.Contains(v))
                continue;

            var bytes = await legacyReader.ReadDeltaIndex(v);
            yield return (v, bytes);
        }
    }

    public async Task<ReadOnlyMemory<byte>> ReadSnapshotIndex(ulong version)
    {
        var artifact = await rowStore.GetArtifact(_datasetPrefix, ObjectNameTypeEnum.Snapshot, version);
        if (artifact == null)
        {
            if (storageConfig.Value.LogStorageAccess)
            {
                logger.LogInformation("Falling back to Legacy GCS reader for Snapshot Index {Version}", version);
            }
            return await legacyReader.ReadSnapshotIndex(version);
        }

        return await ReadPayload(artifact, ObjectNameIndexOrDataEnum.Index);
    }

    public async Task<(ulong OldestDelta, ulong NewestDelta, List<SnapshotType> Snapshots)> ListVersions()
    {
        var deltaRows = await rowStore.ListArtifacts(_datasetPrefix, ObjectNameTypeEnum.Delta);
        var snapshotRows = await rowStore.ListArtifacts(_datasetPrefix, ObjectNameTypeEnum.Snapshot);

        var legacyVersions = await TryListLegacyVersions();

        var deltaVersions = deltaRows.Select(r => r.Version)
            .Concat(Enumerable.Range(0, legacyVersions.NewestDelta >= legacyVersions.OldestDelta && legacyVersions.NewestDelta > 0
                    ? checked((int)(legacyVersions.NewestDelta - legacyVersions.OldestDelta + 1))
                    : 0)
                .Select(i => legacyVersions.OldestDelta + (ulong)i))
            .Distinct()
            .Order()
            .ToList();

        var snapshots = snapshotRows
            .Select(r => new SnapshotType(r.Version, r.CreatedAt))
            .Concat(legacyVersions.Snapshots)
            .GroupBy(s => s.Version)
            .Select(g => g.OrderByDescending(s => s.DateTime).First())
            .OrderBy(s => s.Version)
            .ToList();

        return (
            deltaVersions.Count == 0 ? 0ul : deltaVersions.First(),
            deltaVersions.Count == 0 ? 0ul : deltaVersions.Last(),
            snapshots);
    }

    public async Task<ReadOnlyMemory<byte>> ReadDataFromSources(
        List<(ObjectNameTypeEnum type, ulong version, long position)> sources, int length)
    {
        if (sources.Count == 0)
        {
            throw new ArgumentException("at least one source must be filled");
        }

        if (length == 0)
        {
            return ReadOnlyMemory<byte>.Empty;
        }

        foreach (var source in sources)
        {
            var artifact = await rowStore.GetArtifact(_datasetPrefix, source.type, source.version);
            if (artifact != null)
            {
                return await ReadRange(artifact, source.position, length);
            }

            try
            {
                return await legacyReader.ReadDataFromSources([source], length);
            }
            catch (FileNotFoundException)
            {
                logger.LogDebug("Legacy source {Type} v{Version} was not found, trying the next source", source.type, source.version);
            }
        }

        throw new FileNotFoundException("None of the requested sources could be resolved");
    }

    public async Task<long> GetSnapshotDataContentLength(ulong snapshotVersion)
    {
        var artifact = await rowStore.GetArtifact(_datasetPrefix, ObjectNameTypeEnum.Snapshot, snapshotVersion);
        if (artifact == null)
        {
            return await legacyReader.GetSnapshotDataContentLength(snapshotVersion);
        }

        return artifact.DataLength;
    }

    private async Task<(ulong OldestDelta, ulong NewestDelta, List<SnapshotType> Snapshots)> TryListLegacyVersions()
    {
        try
        {
            return await legacyReader.ListVersions();
        }
        catch (FileNotFoundException)
        {
            return (0ul, 0ul, []);
        }
    }

    private async Task<ReadOnlyMemory<byte>> ReadPayload(HybridArtifactRecord artifact, ObjectNameIndexOrDataEnum payloadType)
    {
        byte[]? inlinePayload;
        string? fallbackObjectKey;

        if (payloadType == ObjectNameIndexOrDataEnum.Index)
        {
            inlinePayload = artifact.InlineIndex;
            fallbackObjectKey = artifact.IndexFallbackObjectKey;
        }
        else
        {
            if (artifact.DataLength == 0)
            {
                return ReadOnlyMemory<byte>.Empty;
            }

            inlinePayload = artifact.InlineData;
            fallbackObjectKey = artifact.DataFallbackObjectKey;
        }

        if (inlinePayload != null)
        {
            if (storageConfig.Value.LogStorageAccess)
            {
                logger.LogInformation("Read {Length} bytes for {PayloadType} from Bigtable (Inline)", inlinePayload.Length, payloadType);
            }
            return inlinePayload;
        }

        if (!string.IsNullOrEmpty(fallbackObjectKey))
        {
            if (storageConfig.Value.LogStorageAccess)
            {
                logger.LogInformation("Reading {PayloadType} from GCS (Fallback Key: {Key})", payloadType, fallbackObjectKey);
            }
            return await blobStorage.DownloadBytes(fallbackObjectKey);
        }

        throw new FileNotFoundException($"The requested {payloadType} payload is missing from the hybrid artifact row.");
    }

    private async Task<ReadOnlyMemory<byte>> ReadRange(HybridArtifactRecord artifact, long position, int length)
    {
        if (artifact.DataLength == 0)
        {
            return ReadOnlyMemory<byte>.Empty;
        }

        if (position < 0 || position + length > artifact.DataLength)
        {
            throw new ArgumentOutOfRangeException(nameof(position));
        }

        if (artifact.InlineData != null)
        {
            if (storageConfig.Value.LogStorageAccess)
            {
                logger.LogInformation("Read {Length} bytes for Data (Range) from Bigtable (Inline)", length);
            }
            return new ReadOnlyMemory<byte>(artifact.InlineData, (int)position, length);
        }

        if (!string.IsNullOrEmpty(artifact.DataFallbackObjectKey))
        {
            if (storageConfig.Value.LogStorageAccess)
            {
                logger.LogInformation("Reading Data (Range) from GCS (Fallback Key: {Key})", artifact.DataFallbackObjectKey);
            }
            return await blobStorage.DownloadBytes(artifact.DataFallbackObjectKey, position, length);
        }

        throw new FileNotFoundException("The requested hybrid artifact data payload is missing.");
    }
}