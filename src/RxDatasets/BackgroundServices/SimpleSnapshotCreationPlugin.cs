using System.Reactive.Concurrency;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.Models;

namespace RxDatasets.BackgroundServices;

/// <summary>
/// Simplified snapshot creator that borrows the DatasetWriter's in-memory indices
/// and reads entity data via the multi-source read API (random-access, cache-friendly).
/// Does NOT store version history arrays — suitable for deployments that don't need
/// <c>ListChanges</c> for ranges beyond what is still available in delta files.
/// </summary>
public class SimpleSnapshotCreationPlugin<T> : IDatasetUpdateConsumer<T>
{
    private readonly ILogger<SimpleSnapshotCreationPlugin<T>> _logger;
    private readonly IOptions<SnapshotCreationConfig> _config;
    private readonly IStorageProvider<T> _storageProvider;
    private readonly IScheduler _scheduler;
    private readonly TimeSpan _periodInterval;
    private readonly SemaphoreSlim _semaphore = new(1);
    private readonly NameCompressor? _nameCompressor;
    private readonly ICurrentIndexProvider<T> _indexProvider;

    public SimpleSnapshotCreationPlugin(
        ILogger<SimpleSnapshotCreationPlugin<T>> logger,
        IStorageProvider<T> storageProvider,
        IOptions<SnapshotCreationConfig> config,
        IScheduler scheduler,
        ICurrentIndexProvider<T> indexProvider,
        NameCompressor? nameCompressor = null)
    {
        _logger = logger;
        _storageProvider = storageProvider;
        _config = config;
        _scheduler = scheduler;
        _indexProvider = indexProvider;
        _nameCompressor = nameCompressor;
        _periodInterval = _config.Value.PeriodIntervalAsTimespan;
    }

    public string ConsumerIdentifier { get; } = $"SimpleSnapshotCreationPlugin-of-{typeof(T)}";
    public TimeSpan StartupDelay => _config.Value.StartupDelay;

    public async Task HandleNewDeltas(CancellationToken stoppingToken)
    {
        await CreateSnapshotIfNecessary(stoppingToken);
    }

    private async Task CreateSnapshotIfNecessary(CancellationToken stoppingToken)
    {
        using var slot = await Lock(stoppingToken);
        var (_, newestDelta, snapshots) = await _storageProvider.ListVersions();

        var snapshot = snapshots.Count > 0
            ? snapshots.MaxBy(s => s.Version)
            : new SnapshotType(0, null);

        ulong lastSnapshotVersion = snapshot!.Version;
        if (newestDelta <= lastSnapshotVersion)
            return;

        if (SnapshotCreationPlugin<T>.IsDeltaGapConditionMet(
                newestDelta, lastSnapshotVersion, _config.Value.SnapshotInterval))
        {
            await CreateASnapshot(newestDelta, stoppingToken);
            return;
        }

        var timeUntilNext = SnapshotCreationPlugin<T>.CalculateTimeUntilNextSnapshot(
            _scheduler, snapshot.DateTime, _periodInterval);

        if (timeUntilNext.Ticks <= 0)
        {
            await CreateASnapshot(newestDelta, stoppingToken);
            return;
        }

        _scheduler.Schedule(timeUntilNext, _ =>
            Task.Run(async () => await HandleNewDeltas(CancellationToken.None)).GetAwaiter().GetResult());
    }

    private async Task CreateASnapshot(ulong version, CancellationToken stoppingToken)
    {
        _logger.LogInformation("SimpleSnapshot: creating snapshot for delta {Version}", version);

        var currentIndex = _indexProvider.CurrentDatasetState;
        var snapshotIndex = _indexProvider.MostRecentSnapshotState;

        if (currentIndex == null)
        {
            _logger.LogWarning("SimpleSnapshot: DatasetWriter has no index yet, skipping");
            return;
        }

        if (currentIndex.LatestVersion < version)
        {
            _logger.LogWarning(
                "SimpleSnapshot: DatasetWriter index v{WriterVersion} < requested v{Version}, skipping",
                currentIndex.LatestVersion, version);
            return;
        }

        var newSnapshotVersion = currentIndex.LatestVersion;
        _logger.LogInformation("SimpleSnapshot: creating v{Version} from {Count} entities",
            newSnapshotVersion, currentIndex.Entities.Count);

        var tempPath = DatasetHelper.GetTempFilePath();
        await using var targetStream = File.OpenWrite(tempPath);
        try
        {
            // DFS the trie, for each entity: read data via multi-source, write to output,
            // build SnapshotBucket tree mirroring the trie structure.
            var rootBucket = await BuildSnapshotFromTrie(
                currentIndex, snapshotIndex, targetStream, stoppingToken);

            if (currentIndex.Entities.Count == 0)
                DatasetHelper.WriteProtobufLengthPrefix(targetStream, 0);

            await targetStream.FlushAsync(stoppingToken);
            targetStream.Close();

            await using var dataReader = File.OpenRead(tempPath);
            stoppingToken.ThrowIfCancellationRequested();

            await _storageProvider.StoreSnapshot(
                newSnapshotVersion,
                StreamingIndexCodec.WriteSnapshot(
                    newSnapshotVersion, _nameCompressor?.Pattern ?? "", rootBucket),
                dataReader);

            dataReader.Close();
            _logger.LogInformation("SimpleSnapshot: created snapshot v{Version}", newSnapshotVersion);
        }
        finally
        {
            targetStream.Close();
            File.Delete(tempPath);
        }
    }

    /// <summary>
    /// DFS-walks the EntityTrie's internal TrieNode tree. For each leaf entity,
    /// reads data via multi-source API, writes to output stream, and builds a
    /// SnapshotBucket tree with new positions. One leaf bucket at a time is "hot".
    /// </summary>
    private async Task<SnapshotBucket> BuildSnapshotFromTrie(
        ImmutableDatasetState currentIndex,
        ImmutableDatasetState? snapshotIndex,
        Stream targetStream,
        CancellationToken ct)
    {
        var ctx = new SnapshotBuildContext(currentIndex, snapshotIndex, _storageProvider, targetStream);
        var rootBucket = new SnapshotBucket();
        await BuildBucketRecursive(ctx, currentIndex.Entities.Root, rootBucket, "", ct);
        return rootBucket;
    }

    private async Task BuildBucketRecursive(
        SnapshotBuildContext ctx, TrieNode node, SnapshotBucket bucket, string compressedPrefix,
        CancellationToken ct)
    {
        // Process leaf entity at this node (if any — rare, only for root-level entities)
        if (node.Value is { } entity)
        {
            var fullName = ctx.CurrentIndex.Entities.Compressor?.Expand(compressedPrefix) ?? compressedPrefix;
            var snapshotEntity = await ReadAndWriteEntity(ctx, fullName, entity, ct);
            bucket.Leaves[""] = snapshotEntity;
        }

        foreach (var (segment, childNode) in node.Children)
        {
            ct.ThrowIfCancellationRequested();

            var childPrefix = compressedPrefix.Length == 0 ? segment : compressedPrefix + "/" + segment;

            if (childNode.Value is { } leafEntity && childNode.Children.Count == 0)
            {
                // Pure leaf: entity with no further children.
                var fullName = ctx.CurrentIndex.Entities.Compressor?.Expand(childPrefix) ?? childPrefix;
                var snapshotEntity = await ReadAndWriteEntity(ctx, fullName, leafEntity, ct);
                bucket.Leaves[segment] = snapshotEntity;
            }
            else
            {
                // Subtree: recurse.
                var childBucket = new SnapshotBucket();
                await BuildBucketRecursive(ctx, childNode, childBucket, childPrefix, ct);
                if (childBucket.Leaves.Count > 0 || childBucket.Children.Count > 0)
                    bucket.Children[segment] = childBucket;
            }
        }
    }

    private async Task<SnapshotEntity> ReadAndWriteEntity(
        SnapshotBuildContext ctx, string fullName, IndexEntity entity, CancellationToken ct)
    {
        // Build source list (same logic as DatasetWriter.ReadEntityBytes)
        var sources = new List<(ObjectNameTypeEnum type, ulong version, long position)>(2);

        // Prefer the most-recent snapshot index — it may be newer than what
        // currentIndex was built on, so entity positions there reference a more
        // recent (and more likely cached) data file.
        var snapshotIdx = ctx.SnapshotIndex;
        if (snapshotIdx != null && entity.LatestVersion <= snapshotIdx.SnapshotVersion)
        {
            if (snapshotIdx.Entities.TryGetValue(fullName, out var snapshotEntity))
            {
                sources.Add((ObjectNameTypeEnum.Snapshot, snapshotIdx.SnapshotVersion,
                    snapshotEntity.Position));
            }
        }

        if (entity.LatestVersion <= ctx.CurrentIndex.SnapshotVersion)
        {
            if (sources.Count == 0 || sources[0].version != ctx.CurrentIndex.SnapshotVersion)
                sources.Add((ObjectNameTypeEnum.Snapshot, ctx.CurrentIndex.SnapshotVersion, entity.Position));
        }
        else
        {
            sources.Add((ObjectNameTypeEnum.Delta, entity.LatestVersion, entity.Position));
        }

        var data = await ctx.StorageProvider.ReadDataFromSources(sources, entity.Length);

        // Write to output: length-prefix + data
        var prefixLen = DatasetHelper.WriteProtobufLengthPrefix(ctx.TargetStream, entity.Length);
        var newPosition = ctx.DataPosition + prefixLen;
        ctx.TargetStream.Write(data.Span);
        ctx.DataPosition = newPosition + entity.Length;

        // Build slim SnapshotEntity (no version history — just current version)
        var snapshotProto = new SnapshotEntity
        {
            Position = newPosition,
            Length = entity.Length,
            Crc32 = entity.Crc32,
            LastUpdatedVersion = entity.LatestVersion,
        };
        snapshotProto.LastInsertedVersion.Add(entity.LatestVersion);

        return snapshotProto;
    }

    private sealed class SnapshotBuildContext(
        ImmutableDatasetState currentIndex,
        ImmutableDatasetState? snapshotIndex,
        IReadOnlyStorageProvider<T> storageProvider,
        Stream targetStream)
    {
        public ImmutableDatasetState CurrentIndex { get; } = currentIndex;
        public ImmutableDatasetState? SnapshotIndex { get; } = snapshotIndex;
        public IReadOnlyStorageProvider<T> StorageProvider { get; } = storageProvider;
        public Stream TargetStream { get; } = targetStream;
        public long DataPosition { get; set; }
    }

    private async Task<IDisposable> Lock(CancellationToken cancellationToken)
    {
        await _semaphore.WaitAsync(cancellationToken)
            .ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        return new ActionDisposable(() => _semaphore.Release());
    }
}
