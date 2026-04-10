using System.Buffers;
using System.Reactive.Concurrency;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.Models;

namespace RxDatasets.BackgroundServices;

public class SnapshotCreationPlugin<T> : IDatasetUpdateConsumer<T>
{
    private const int MaxStackAllocSize = 8 * 1024;
    private readonly ILogger<SnapshotCreationPlugin<T>> _logger;
    private readonly IOptions<SnapshotCreationConfig> _config;

    private readonly IStorageProvider<T> _storageProvider;
    private readonly DatasetHelper _datasetHelper;
    private readonly IScheduler _scheduler;
    private readonly TimeSpan _currentValuePeriodIntervalAsTimespan;
    private readonly SemaphoreSlim _semaphore = new(1);
    private readonly NameCompressor? _nameCompressor;

    public SnapshotCreationPlugin(
        ILogger<SnapshotCreationPlugin<T>> logger,
        IStorageProvider<T> storageProvider,
        IOptions<SnapshotCreationConfig> config, IScheduler scheduler,
        NameCompressor? nameCompressor = null)
    {
        _logger = logger;
        _storageProvider = storageProvider;
        _datasetHelper = new DatasetHelper(logger, _storageProvider, nameCompressor);
        _config = config;
        _scheduler = scheduler;
        _nameCompressor = nameCompressor;
        _currentValuePeriodIntervalAsTimespan = _config.Value.PeriodIntervalAsTimespan;
    }
    public string ConsumerIdentifier { get; } = $"SnapshotCreationPlugin-of-{typeof(T)}";

    public TimeSpan StartupDelay => _config.Value.StartupDelay;

    private async Task CreateSnapshotIfNecessary(CancellationToken stoppingToken)
    {
        using var slot = await Lock(stoppingToken);
        var (_, newestDelta, snapshots) = await _storageProvider.ListVersions();
        var conditionsAreMet = AreSnapshotCreationConditionsMet(newestDelta, snapshots);
        if (!conditionsAreMet)
            return;

        await CreateASnapshot(newestDelta, stoppingToken);
    }


    private bool AreSnapshotCreationConditionsMet(ulong newestDelta,
        List<SnapshotType> snapshots)
    {
        var snapshot =
             snapshots.Count > 0
                 ? snapshots.MaxBy(s => s.Version)
                 : new SnapshotType(0, null);

        ulong lastSnapshotVersion = snapshot!.Version;
        DateTimeOffset? lastSnapshotTime = snapshot.DateTime;
        // Don't do or schedule anything if there is no delta since the last snapshot (or no data at all)
        if (newestDelta <= lastSnapshotVersion)
            return false;

        // If the delta gap is large enough, make a snapshot, no need to schedule anything
        if (IsDeltaGapConditionMet(newestDelta, lastSnapshotVersion, _config.Value.SnapshotInterval))
            return true;

        // Calculate how much time we have until the next snapshot should be scheduled - can be 0 or negative.
        var timeUntilNext = CalculateTimeUntilNextSnapshot(_scheduler, lastSnapshotTime, _currentValuePeriodIntervalAsTimespan);

        // If overdue, let's go
        if (timeUntilNext.Ticks <= 0)
            return true;

        // If not, schedule for later
        _scheduler.Schedule(timeUntilNext, _ => Task.Run(async () => await HandleNewDeltas(CancellationToken.None)).GetAwaiter().GetResult());

        return false;
    }

    public static bool IsDeltaGapConditionMet(ulong lastDelta, ulong lastSnapshotVersion, ulong interval)
    {
        return lastDelta - lastSnapshotVersion >= interval;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <returns>True if enough time has passed to create a snapshot</returns>
    public static TimeSpan CalculateTimeUntilNextSnapshot(IScheduler scheduler, DateTimeOffset? snapshotTime,
        TimeSpan maxInterval)
    {
        if (!snapshotTime.HasValue)
            return default; // NOW!

        var timeSinceLastSnapshot = scheduler.Now - snapshotTime.Value;
        return maxInterval - timeSinceLastSnapshot;
    }

    private async Task CreateASnapshot(ulong version, CancellationToken stoppingToken)
    {
        _logger.LogInformation($"Snapshot Hosted {typeof(T).Name} Service is working. Creating snapshot for delta {version}.");
        var tempState = await _datasetHelper.GetIndexOfVersion(version);
        _logger.LogInformation($"The previous snapshot is {tempState.SnapshotVersion}.");

        await CreateNewSnapshot(tempState, stoppingToken);
    }

    private async Task CreateNewSnapshot(ImmutableDatasetState currentIndex, CancellationToken stoppingToken)
    {
        var newSnapshotVersion = currentIndex.LatestVersion;

        _logger.LogInformation($"Creating snapshot version {newSnapshotVersion}");

        var tempPath = DatasetHelper.GetTempFilePath();
        await using var targetSnapshotStream = File.OpenWrite(tempPath);
        try
        {
            long targetSnapshotStreamPosition = 0;

            // Phase 1a: Copy alive entities from old snapshot data → output.
            // Records oldPosition → newPosition for position remapping (no proto objects allocated).
            var positionMap = new Dictionary<long, long>();
            if (currentIndex.SnapshotVersion > 0)
            {
                targetSnapshotStreamPosition = await CopyOldSnapshotData(
                    currentIndex, targetSnapshotStream, targetSnapshotStreamPosition,
                    positionMap, stoppingToken);
            }

            // Phase 1b: Copy alive entities from deltas → output.
            // Builds proto entities for delta-originated items (compressed names).
            var deltaProtoEntities = new Dictionary<string, SnapshotEntity>();
            targetSnapshotStreamPosition = await CopyDeltaData(
                currentIndex, deltaProtoEntities,
                targetSnapshotStream, targetSnapshotStreamPosition, stoppingToken);

            if (targetSnapshotStreamPosition == 0)
                DatasetHelper.WriteProtobufLengthPrefix(targetSnapshotStream, 0);

            await targetSnapshotStream.FlushAsync(stoppingToken);
            targetSnapshotStream.Close();

            // Phase 2: Build SnapshotIndex bucket-by-bucket.
            // Only one leaf bucket's proto entities are in memory at a time (~100K entries).
            var snapshotBucket = await BuildIndexBucketByBucket(
                currentIndex, positionMap, deltaProtoEntities, stoppingToken);

            await using var localSnapshotReader = File.OpenRead(tempPath);
            stoppingToken.ThrowIfCancellationRequested();

            await _storageProvider.StoreSnapshot(newSnapshotVersion,
                StreamingIndexCodec.WriteSnapshot(
                    newSnapshotVersion, _nameCompressor?.Pattern ?? "", snapshotBucket),
                localSnapshotReader);

            localSnapshotReader.Close();
            _logger.LogInformation($"Created snapshot version {newSnapshotVersion}");
        }
        finally
        {
            targetSnapshotStream.Close();
            File.Delete(tempPath);
        }
    }

    /// <summary>
    /// Phase 1a: Streams old snapshot data, copies alive entities to the output stream,
    /// and records old→new position mappings. Does NOT parse proto entities — saving ~2-3 GB.
    /// </summary>
    private async Task<long> CopyOldSnapshotData(
        ImmutableDatasetState currentIndex, Stream targetStream, long targetStreamPosition,
        Dictionary<long, long> positionMap, CancellationToken stoppingToken)
    {
        await _storageProvider.ReadSnapshotData(currentIndex.SnapshotVersion, sourceStream =>
        {
            long sourceStreamPosition = 0;

            foreach (var entity in currentIndex.Entities
                         .Where(x => x.Value.LatestVersion <= currentIndex.SnapshotVersion)
                         .OrderBy(x => x.Value.Position))
            {
                if (stoppingToken.IsCancellationRequested) return;

                var entityLength = entity.Value.Length;
                targetStreamPosition += DatasetHelper.WriteProtobufLengthPrefix(targetStream, entityLength);
                var newEntityPosition = targetStreamPosition;

                var gap = entity.Value.Position - sourceStreamPosition;
                sourceStreamPosition += SeekForwardBy(sourceStream, gap);
                EfficientCopy(sourceStream, targetStream, entityLength);
                sourceStreamPosition += entityLength;
                targetStreamPosition += entityLength;

                positionMap[entity.Value.Position] = newEntityPosition;
            }
        });

        stoppingToken.ThrowIfCancellationRequested();
        return targetStreamPosition;
    }

    /// <summary>
    /// Phase 1b: Copies delta entities to output, builds proto entities keyed by compressed name.
    /// Also records deletion proto entries for entities removed by deltas.
    /// </summary>
    private async Task<long> CopyDeltaData(
        ImmutableDatasetState currentIndex,
        Dictionary<string, SnapshotEntity> deltaProtoEntities,
        Stream targetSnapshotStream, long targetSnapshotStreamPosition, CancellationToken stoppingToken)
    {
        var liveEntities = currentIndex.Entities
            .Where(x => x.Value.LatestVersion > currentIndex.SnapshotVersion)
            .ToList();

        // Record deleted entities from delta indices
        var deltas = await _datasetHelper.GetChangedDeltas(currentIndex.SnapshotVersion, currentIndex.LatestVersion);
        foreach (var delta in deltas)
        {
            var deltaVersion = delta.DeltaVersion > 0 ? delta.DeltaVersion : delta.DeltaVersionLegacy;
            foreach (var deletedEntity in delta.DeletedEntities)
            {
                if (!currentIndex.Entities.ContainsKey(deletedEntity.Name))
                {
                    var protoEntity = new SnapshotEntity
                    {
                        LastDeletedVersion = { deltaVersion },
                        Position = 0,
                        Length = 0
                    };
                    if (deletedEntity.PreviousVersion > 0)
                        protoEntity.LastInsertedVersion.Add(deletedEntity.PreviousVersion);

                    var compressedName = _nameCompressor?.Compress(deletedEntity.Name) ?? deletedEntity.Name;
                    deltaProtoEntities[compressedName] = protoEntity;
                }
            }

            foreach (var deletedName in delta.DeletedNames)
            {
                if (!currentIndex.Entities.ContainsKey(deletedName))
                {
                    var compressedName = _nameCompressor?.Compress(deletedName) ?? deletedName;
                    deltaProtoEntities[compressedName] = new SnapshotEntity
                    {
                        LastDeletedVersion = { deltaVersion },
                        Position = 0,
                        Length = 0
                    };
                }
            }
        }

        if (liveEntities.Count == 0)
            return targetSnapshotStreamPosition;

        // Group entities by their source delta version
        var entitiesByDelta = liveEntities
            .GroupBy(e => e.Value.LatestVersion)
            .ToDictionary(g => g.Key, g => g.ToList());

        // Pre-fetch all unique delta data blobs in parallel
        var deltaDataBlobs = new Dictionary<ulong, ReadOnlyMemory<byte>>();
        using var throttle = new SemaphoreSlim(10);
        var fetchTasks = entitiesByDelta.Keys.Select(async deltaVersion =>
        {
            await throttle.WaitAsync(stoppingToken);
            try
            {
                var maxExtent = entitiesByDelta[deltaVersion]
                    .Max(e => e.Value.Position + e.Value.Length);
                var blob = await _storageProvider.ReadDataFromSources(
                    [(ObjectNameTypeEnum.Delta, deltaVersion, 0)], (int)maxExtent);
                lock (deltaDataBlobs)
                {
                    deltaDataBlobs[deltaVersion] = blob;
                }
            }
            finally
            {
                throttle.Release();
            }
        }).ToList();

        await Task.WhenAll(fetchTasks);

        // Build a lookup of delta info per entity for proto construction
        var deltaEntityInfo = new Dictionary<string, (ulong deltaVersion, ChangeType changeType, ulong previousVersion)>();
        foreach (var delta in deltas)
        {
            var deltaVersion = delta.DeltaVersion > 0 ? delta.DeltaVersion : delta.DeltaVersionLegacy;
            foreach (var e in delta.NewEntities)
                deltaEntityInfo[e.Name] = (deltaVersion, ChangeType.New, e.PreviousVersion);
            foreach (var e in delta.ChangedEntities)
                deltaEntityInfo[e.Name] = (deltaVersion, ChangeType.Changed, e.PreviousVersion);
        }

        // Write entities to the snapshot stream from pre-fetched blobs
        foreach (var entity in liveEntities)
        {
            stoppingToken.ThrowIfCancellationRequested();

            var entityLength = entity.Value.Length;
            targetSnapshotStreamPosition += DatasetHelper.WriteProtobufLengthPrefix(targetSnapshotStream, entityLength);
            var targetEntityPosition = targetSnapshotStreamPosition;

            var deltaVersion = entity.Value.LatestVersion;
            var deltaBlob = deltaDataBlobs[deltaVersion];
            var entityBytes = deltaBlob.Slice((int)entity.Value.Position, entityLength);

            await targetSnapshotStream.WriteAsync(entityBytes, stoppingToken);
            targetSnapshotStreamPosition += entityLength;

            var protoEntity = new SnapshotEntity
            {
                Position = targetEntityPosition,
                Length = entityLength,
                Crc32 = entity.Value.Crc32
            };

            if (deltaEntityInfo.TryGetValue(entity.Key, out var info))
            {
                if (info.changeType == ChangeType.New)
                    protoEntity.LastInsertedVersion.Add(info.deltaVersion);
                else
                    protoEntity.LastUpdatedVersion = info.deltaVersion;
            }
            else
            {
                protoEntity.LastInsertedVersion.Add(deltaVersion);
            }

            var compressedName = _nameCompressor?.Compress(entity.Key) ?? entity.Key;
            deltaProtoEntities[compressedName] = protoEntity;
        }

        return targetSnapshotStreamPosition;
    }

    /// <summary>
    /// Phase 2: Builds the SnapshotIndex bucket-by-bucket from old snapshot index bytes.
    /// For each leaf bucket (~100K entities): parse old proto entities, update positions,
    /// merge delta entities, then discard. Peak memory: ~20 MB per bucket instead of ~3 GB total.
    /// </summary>
    private async Task<SnapshotBucket> BuildIndexBucketByBucket(
        ImmutableDatasetState currentIndex,
        Dictionary<long, long> positionMap,
        Dictionary<string, SnapshotEntity> deltaProtoEntities,
        CancellationToken stoppingToken)
    {
        if (currentIndex.SnapshotVersion == 0)
        {
            // No old snapshot — build entirely from delta proto entities
            return BuildNestedSnapshotBucket(deltaProtoEntities);
        }

        var oldIndexBytes = await _storageProvider.ReadSnapshotIndex(currentIndex.SnapshotVersion);
        var (_, _, leafBuckets) = StreamingIndexCodec.ScanLeafBuckets(oldIndexBytes.Span);

        if (leafBuckets.Count == 0)
        {
            // Legacy flat format — fall back to monolithic approach
            return BuildIndexFromFlatSnapshot(oldIndexBytes, positionMap, deltaProtoEntities);
        }

        // Group delta proto entities by bucket prefix for efficient per-bucket merging
        var deltasByBucket = new Dictionary<string, Dictionary<string, SnapshotEntity>>();
        var unclaimedDeltas = new Dictionary<string, SnapshotEntity>();

        // Known bucket prefixes from the scan
        var bucketPrefixes = new HashSet<string>(leafBuckets.Select(b => b.Prefix));

        foreach (var (compressedName, proto) in deltaProtoEntities)
        {
            var lastSlash = compressedName.LastIndexOf('/');
            var prefix = lastSlash >= 0 ? compressedName[..lastSlash] : "";
            var leafKey = lastSlash >= 0 ? compressedName[(lastSlash + 1)..] : compressedName;

            if (bucketPrefixes.Contains(prefix))
            {
                if (!deltasByBucket.TryGetValue(prefix, out var dict))
                {
                    dict = new Dictionary<string, SnapshotEntity>();
                    deltasByBucket[prefix] = dict;
                }
                dict[leafKey] = proto;
            }
            else
            {
                unclaimedDeltas[compressedName] = proto;
            }
        }

        // Build the result tree bucket-by-bucket
        var rootBucket = new SnapshotBucket();

        foreach (var leafBucket in leafBuckets)
        {
            stoppingToken.ThrowIfCancellationRequested();

            // Parse only this bucket's proto entities from the old index bytes
            var oldProtoEntities = StreamingIndexCodec.ReadBucketProtoEntities(oldIndexBytes.Span, leafBucket);
            var bucketDeltas = deltasByBucket.GetValueOrDefault(leafBucket.Prefix);

            // Merge into a new dictionary: update positions, override with delta entities
            var mergedEntities = new Dictionary<string, SnapshotEntity>(oldProtoEntities.Count);
            foreach (var (leafKey, oldProto) in oldProtoEntities)
            {
                if (bucketDeltas != null && bucketDeltas.Remove(leafKey, out var deltaProto))
                {
                    // Delta overrides old proto (new/changed/deleted)
                    mergedEntities[leafKey] = deltaProto;
                }
                else if (positionMap.TryGetValue(oldProto.Position, out var newPosition))
                {
                    // Entity alive from old snapshot — remap position
                    var updated = oldProto.Clone();
                    updated.Position = newPosition;
                    mergedEntities[leafKey] = updated;
                }
                else
                {
                    // Entity was already dead in old snapshot — keep as-is
                    mergedEntities[leafKey] = oldProto;
                }
            }

            // Add delta entities NEW to this bucket (not in old snapshot)
            if (bucketDeltas != null)
            {
                foreach (var (leafKey, deltaProto) in bucketDeltas)
                    mergedEntities[leafKey] = deltaProto;
            }

            // Place this bucket's leaves into the result tree
            InsertLeafBucket(rootBucket, leafBucket.Prefix, mergedEntities);
            // oldProtoEntities is now eligible for GC
        }

        // Handle delta entities for buckets not in the old snapshot (new stores)
        foreach (var (compressedName, proto) in unclaimedDeltas)
        {
            InsertEntity(rootBucket, compressedName, proto);
        }

        return rootBucket;
    }

    /// <summary>
    /// Fallback for legacy flat-format snapshots: parse the full index and build from it.
    /// </summary>
    private SnapshotBucket BuildIndexFromFlatSnapshot(
        ReadOnlyMemory<byte> oldIndexBytes,
        Dictionary<long, long> positionMap,
        Dictionary<string, SnapshotEntity> deltaProtoEntities)
    {
        var snapshotIndex = SnapshotIndex.Parser.ParseFrom(oldIndexBytes.Span);
        NestedIndexHelper.EnsureFlat(snapshotIndex);
        if (snapshotIndex.IdPattern.Length > 0)
            NestedIndexHelper.ExpandSnapshotNames(snapshotIndex);

        // Update positions from old snapshot entities, merge deltas
        var protoEntities = new Dictionary<string, SnapshotEntity>();
        foreach (var (name, oldProto) in snapshotIndex.Entities)
        {
            var compressedName = _nameCompressor?.Compress(name) ?? name;
            if (deltaProtoEntities.Remove(compressedName, out var deltaProto))
            {
                protoEntities[compressedName] = deltaProto;
            }
            else if (positionMap.TryGetValue(oldProto.Position, out var newPosition))
            {
                var updated = oldProto.Clone();
                updated.Position = newPosition;
                protoEntities[compressedName] = updated;
            }
            else
            {
                protoEntities[compressedName] = oldProto;
            }
        }

        foreach (var (compressedName, proto) in deltaProtoEntities)
            protoEntities[compressedName] = proto;

        return BuildNestedSnapshotBucket(protoEntities);
    }

    /// <summary>
    /// Navigates/creates the tree path for a leaf bucket prefix and sets its leaves.
    /// </summary>
    private static void InsertLeafBucket(
        SnapshotBucket root, string prefix, Dictionary<string, SnapshotEntity> leaves)
    {
        var current = root;
        if (prefix.Length > 0)
        {
            foreach (var segment in prefix.Split('/'))
            {
                if (!current.Children.TryGetValue(segment, out var child))
                {
                    child = new SnapshotBucket();
                    current.Children[segment] = child;
                }
                current = child;
            }
        }

        foreach (var (key, entity) in leaves)
            current.Leaves[key] = entity;
    }

    /// <summary>
    /// Inserts a single entity into the tree at the correct position.
    /// </summary>
    private static void InsertEntity(SnapshotBucket root, string compressedName, SnapshotEntity entity)
    {
        var segments = compressedName.Split('/');
        var current = root;
        for (int i = 0; i < segments.Length - 1; i++)
        {
            if (!current.Children.TryGetValue(segments[i], out var child))
            {
                child = new SnapshotBucket();
                current.Children[segments[i]] = child;
            }
            current = child;
        }
        current.Leaves[segments[^1]] = entity;
    }

    /// <summary>
    /// Builds a nested SnapshotBucket from a flat dictionary of (compressed) name → entity.
    /// </summary>
    private static SnapshotBucket BuildNestedSnapshotBucket(Dictionary<string, SnapshotEntity> entities)
    {
        var root = new SnapshotBucket();
        foreach (var (name, entity) in entities)
            InsertEntity(root, name, entity);
        return root;
    }

    public static long EfficientCopy(Stream sourceStream, Stream targetStream, int numberOfBytesToCopy)
    {
        if (numberOfBytesToCopy <= MaxStackAllocSize)
        {
            Span<byte> buffer = stackalloc byte[numberOfBytesToCopy];
            sourceStream.ReadExactly(buffer);
            targetStream.Write(buffer);
        }
        else
        {
            var buffer = ArrayPool<byte>.Shared.Rent(numberOfBytesToCopy);
            try
            {
                var span = buffer[..numberOfBytesToCopy];
                sourceStream.ReadExactly(span);
                targetStream.Write(span);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        return numberOfBytesToCopy;
    }

    /// <summary>
    /// Seeks forward
    /// </summary>
    /// <param name="sourceStream"></param>
    /// <param name="relative"></param>
    /// <returns>The 'relative' value</returns>
    private static long SeekForwardBy(Stream sourceStream, long relative)
    {
        switch (relative)
        {
            case < 0:
                throw new ArgumentOutOfRangeException(nameof(relative));
            case 0:
                return 0;
        }

        // Check if the stream is seekable
        if (sourceStream.CanSeek)
        {
            sourceStream.Seek(relative, SeekOrigin.Current);
            return relative;
        }

        var gapLeft = (int)relative;

        // Define a reasonable stack allocation size
        Span<byte> buffer = stackalloc byte[MaxStackAllocSize];

        while (gapLeft > 0)
        {
            var currentReadSize = Math.Min(gapLeft, MaxStackAllocSize);
            var span = buffer[..currentReadSize];
            sourceStream.ReadExactly(span);
            gapLeft -= currentReadSize;
        }

        return relative;
    }

    public async Task HandleNewDeltas(CancellationToken stoppingToken)
    {
        await CreateSnapshotIfNecessary(stoppingToken);
    }

    private async Task<IDisposable> Lock(CancellationToken cancellationToken)
    {
        await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        return new ActionDisposable(() => _semaphore.Release());
    }
}
