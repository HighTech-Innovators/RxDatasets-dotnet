using System.Diagnostics;
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
    private bool Log => storageConfig.Value.LogStorageAccess;

    public async Task ReadSnapshotDataAsync(ulong snapshotVersion, Func<Stream, Task> asyncCallback)
    {
        var data = await ReadSnapshotData(snapshotVersion);
        await using var stream = new MemoryStream(data.ToArray(), writable: false);
        await asyncCallback(stream);
    }

    public async Task<ReadOnlyMemory<byte>> ReadSnapshotData(ulong snapshotVersion)
    {
        var sw = Log ? Stopwatch.StartNew() : null;
        var artifact = await rowStore.GetArtifact(_datasetPrefix, ObjectNameTypeEnum.Snapshot, snapshotVersion);
        if (artifact == null)
        {
            if (Log)
            {
                logger.LogInformation("Falling back to Legacy GCS reader for Snapshot {Version} ({ElapsedMs}ms)", snapshotVersion, sw!.ElapsedMilliseconds);
            }
            return await legacyReader.ReadSnapshotData(snapshotVersion);
        }

        if (Log)
        {
            logger.LogInformation("Read Snapshot v{Version} Data ({Length} bytes) from Bigtable in {ElapsedMs}ms", snapshotVersion, artifact.DataLength, sw!.ElapsedMilliseconds);
        }
        return await ReadPayload(artifact, ObjectNameIndexOrDataEnum.Data);
    }

    public async Task ReadSnapshotData(ulong snapshotVersion, Action<Stream> callback)
    {
        var sw = Log ? Stopwatch.StartNew() : null;
        var artifact = await rowStore.GetArtifact(_datasetPrefix, ObjectNameTypeEnum.Snapshot, snapshotVersion);
        if (artifact == null)
        {
            if (Log)
            {
                logger.LogInformation("Falling back to Legacy GCS reader for Snapshot {Version} ({ElapsedMs}ms)", snapshotVersion, sw!.ElapsedMilliseconds);
            }
            await legacyReader.ReadSnapshotData(snapshotVersion, callback);
            return;
        }

        if (Log)
        {
            logger.LogInformation("Read Snapshot v{Version} Data ({Length} bytes) from Bigtable in {ElapsedMs}ms", snapshotVersion, artifact.DataLength, sw!.ElapsedMilliseconds);
        }

        if (artifact.DataLength == 0)
        {
            using var emptyStream = new MemoryStream([]);
            callback(emptyStream);
            return;
        }

        if (!string.IsNullOrEmpty(artifact.DataFallbackObjectKey))
        {
            var swGcs = Log ? Stopwatch.StartNew() : null;
            await blobStorage.DownloadWithCallBack(artifact.DataFallbackObjectKey, callback);
            if (Log)
            {
                logger.LogInformation("Streamed Snapshot Data from GCS (Fallback Key: {Key}) in {ElapsedMs}ms", artifact.DataFallbackObjectKey, swGcs!.ElapsedMilliseconds);
            }
            return;
        }

        using var stream = new MemoryStream(artifact.InlineData ?? []);
        callback(stream);
    }

    public async Task<ReadOnlyMemory<byte>> ReadDeltaIndex(ulong version)
    {
        var sw = Log ? Stopwatch.StartNew() : null;
        var artifact = await rowStore.GetArtifact(_datasetPrefix, ObjectNameTypeEnum.Delta, version);
        if (artifact == null)
        {
            if (Log)
            {
                logger.LogInformation("Falling back to Legacy GCS reader for Delta {Version} ({ElapsedMs}ms)", version, sw!.ElapsedMilliseconds);
            }
            return await legacyReader.ReadDeltaIndex(version);
        }

        if (Log)
        {
            logger.LogInformation("Read Delta v{Version} Index ({Length} bytes) from Bigtable in {ElapsedMs}ms", version, artifact.IndexLength, sw!.ElapsedMilliseconds);
        }
        return await ReadPayload(artifact, ObjectNameIndexOrDataEnum.Index);
    }

    public async IAsyncEnumerable<(ulong Version, ReadOnlyMemory<byte> IndexBytes)> ReadDeltaIndexRange(
        ulong fromVersionExclusive,
        ulong toVersionInclusive,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var sw = Log ? Stopwatch.StartNew() : null;

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

        if (Log)
        {
            logger.LogInformation(
                "Scanned delta index range v{From}..v{To}: {Count} indices ({Bytes} bytes) from Bigtable in {ElapsedMs}ms",
                fromVersionExclusive + 1, toVersionInclusive, yielded.Count, totalBytes, sw!.ElapsedMilliseconds);
        }

        // Fall back to legacy reader for any versions missing from Bigtable
        var legacyCount = (toVersionInclusive - fromVersionExclusive) - (ulong)yielded.Count;
        if (legacyCount > 0 && Log)
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
        var sw = Log ? Stopwatch.StartNew() : null;
        var artifact = await rowStore.GetArtifact(_datasetPrefix, ObjectNameTypeEnum.Snapshot, version);
        if (artifact == null)
        {
            if (Log)
            {
                logger.LogInformation("Falling back to Legacy GCS reader for Snapshot Index {Version} ({ElapsedMs}ms)", version, sw!.ElapsedMilliseconds);
            }
            return await legacyReader.ReadSnapshotIndex(version);
        }

        if (Log)
        {
            logger.LogInformation("Read Snapshot v{Version} Index ({Length} bytes) from Bigtable in {ElapsedMs}ms", version, artifact.IndexLength, sw!.ElapsedMilliseconds);
        }
        return await ReadPayload(artifact, ObjectNameIndexOrDataEnum.Index);
    }

    public async Task<(ulong OldestDelta, ulong NewestDelta, List<SnapshotType> Snapshots)> ListVersions()
    {
        var sw = Log ? Stopwatch.StartNew() : null;
        var deltaRows = await rowStore.ListArtifacts(_datasetPrefix, ObjectNameTypeEnum.Delta);
        var snapshotRows = await rowStore.ListArtifacts(_datasetPrefix, ObjectNameTypeEnum.Snapshot);

        if (Log)
        {
            logger.LogInformation("Listed versions ({Deltas} deltas, {Snapshots} snapshots) from Bigtable in {ElapsedMs}ms",
                deltaRows.Count, snapshotRows.Count, sw!.ElapsedMilliseconds);
        }

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
            var sw = Log ? Stopwatch.StartNew() : null;
            var artifact = await rowStore.GetArtifact(_datasetPrefix, source.type, source.version);
            if (artifact != null)
            {
                if (Log)
                {
                    logger.LogInformation("Read {Type} v{Version} Data range (offset {Position}, {Length} bytes) from Bigtable in {ElapsedMs}ms",
                        source.type, source.version, source.position, length, sw!.ElapsedMilliseconds);
                }
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
        var sw = Log ? Stopwatch.StartNew() : null;
        var artifact = await rowStore.GetArtifact(_datasetPrefix, ObjectNameTypeEnum.Snapshot, snapshotVersion);
        if (artifact == null)
        {
            return await legacyReader.GetSnapshotDataContentLength(snapshotVersion);
        }

        if (Log)
        {
            logger.LogInformation("Read Snapshot v{Version} content length ({Length} bytes) from Bigtable in {ElapsedMs}ms", snapshotVersion, artifact.DataLength, sw!.ElapsedMilliseconds);
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
            return inlinePayload;
        }

        if (!string.IsNullOrEmpty(fallbackObjectKey))
        {
            var sw = Log ? Stopwatch.StartNew() : null;
            var data = await blobStorage.DownloadBytes(fallbackObjectKey);
            if (Log)
            {
                logger.LogInformation("Read {PayloadType} ({Length} bytes) from GCS (Fallback Key: {Key}) in {ElapsedMs}ms", payloadType, data.Length, fallbackObjectKey, sw!.ElapsedMilliseconds);
            }
            return data;
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
            return new ReadOnlyMemory<byte>(artifact.InlineData, (int)position, length);
        }

        if (!string.IsNullOrEmpty(artifact.DataFallbackObjectKey))
        {
            var sw = Log ? Stopwatch.StartNew() : null;
            var data = await blobStorage.DownloadBytes(artifact.DataFallbackObjectKey, position, length);
            if (Log)
            {
                logger.LogInformation("Read Data range ({Length} bytes) from GCS (Fallback Key: {Key}) in {ElapsedMs}ms", length, artifact.DataFallbackObjectKey, sw!.ElapsedMilliseconds);
            }
            return data;
        }

        throw new FileNotFoundException("The requested hybrid artifact data payload is missing.");
    }
}
