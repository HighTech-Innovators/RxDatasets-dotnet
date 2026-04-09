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

        // Build the mutable proto index for on-disk serialization (full version arrays).
        // Uses a Dictionary<string, SnapshotEntity> built from old snapshot proto + delta protos.
        var protoEntities = new Dictionary<string, SnapshotEntity>();

        var tempPath = DatasetHelper.GetTempFilePath();

        await using var targetSnapshotStream = File.OpenWrite(tempPath);
        try
        {
            long targetSnapshotStreamPosition = 0;

            // Load old snapshot proto for version-array preservation
            Dictionary<string, SnapshotEntity>? oldSnapshotProtoEntities = null;
            if (currentIndex.SnapshotVersion > 0)
            {
                oldSnapshotProtoEntities = await LoadOldSnapshotProtoEntities(currentIndex.SnapshotVersion);

                targetSnapshotStreamPosition =
                    await CopyRelevantItemsFromOldSnapshot(currentIndex, protoEntities, oldSnapshotProtoEntities,
                        targetSnapshotStream, targetSnapshotStreamPosition, stoppingToken);
            }

            targetSnapshotStreamPosition = await CopyRelevantItemsFromDeltas(currentIndex, protoEntities,
                targetSnapshotStream, targetSnapshotStreamPosition, stoppingToken);

            if (targetSnapshotStreamPosition == 0)
                DatasetHelper.WriteProtobufLengthPrefix(targetSnapshotStream, 0);

            await targetSnapshotStream.FlushAsync(stoppingToken);
            targetSnapshotStream.Close();

            await using var localSnapshotReader = File.OpenRead(tempPath);

            // Build SnapshotIndex proto directly from protoEntities
            var snapshotIndexProto = new SnapshotIndex
            {
                SnapshotVersionLegacy = 0,
                SnapshotVersion = newSnapshotVersion,
                Nested = BuildNestedSnapshotBucket(protoEntities)
            };

            localSnapshotReader.Position = 0;
            stoppingToken.ThrowIfCancellationRequested();

            await _storageProvider.StoreSnapshot(newSnapshotVersion, snapshotIndexProto.ToByteArray(),
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
    /// Loads the old snapshot index proto from disk and flattens it into a name→SnapshotEntity dictionary.
    /// </summary>
    private async Task<Dictionary<string, SnapshotEntity>> LoadOldSnapshotProtoEntities(ulong snapshotVersion)
    {
        var indexBytes = await _storageProvider.ReadSnapshotIndex(snapshotVersion);
        var snapshotIndex = SnapshotIndex.Parser.ParseFrom(indexBytes.Span);
        NestedIndexHelper.EnsureFlat(snapshotIndex);
        return new Dictionary<string, SnapshotEntity>(snapshotIndex.Entities);
    }

    /// <summary>
    /// Builds a nested SnapshotBucket from a flat dictionary of proto entities.
    /// </summary>
    private static SnapshotBucket BuildNestedSnapshotBucket(Dictionary<string, SnapshotEntity> entities)
    {
        var root = new SnapshotBucket();
        foreach (var (name, entity) in entities)
        {
            var segments = name.Split('/');
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
        return root;
    }

    private async Task<long> CopyRelevantItemsFromDeltas(ImmutableDatasetState currentIndex,
        Dictionary<string, SnapshotEntity> protoEntities,
        Stream targetSnapshotStream, long targetSnapshotStreamPosition, CancellationToken stoppingToken)
    {
        // All entities in the slim index with LatestVersion > SnapshotVersion came from deltas.
        // They are all alive (no deleted entities in the trie).
        var liveEntities = currentIndex.Entities
            .Where(x => x.Value.LatestVersion > currentIndex.SnapshotVersion)
            .ToList();

        // Also gather deleted entities from delta indices (not in trie but need proto entries)
        var deltas = await _datasetHelper.GetChangedDeltas(currentIndex.SnapshotVersion, currentIndex.LatestVersion);
        foreach (var delta in deltas)
        {
            var deltaVersion = delta.DeltaVersion > 0 ? delta.DeltaVersion : delta.DeltaVersionLegacy;
            foreach (var deletedEntity in delta.DeletedEntities)
            {
                // Only record if not already alive in the trie (meaning it wasn't re-created later)
                if (!currentIndex.Entities.ContainsKey(deletedEntity.Name))
                {
                    var protoEntity = new SnapshotEntity
                    {
                        LastDeletedVersion = { deltaVersion },
                        Position = 0,
                        Length = 0
                    };
                    if (deletedEntity.PreviousVersion > 0)
                    {
                        protoEntity.LastInsertedVersion.Add(deletedEntity.PreviousVersion);
                    }
                    protoEntities[deletedEntity.Name] = protoEntity;
                }
            }

            foreach (var deletedName in delta.DeletedNames)
            {
                if (!currentIndex.Entities.ContainsKey(deletedName))
                {
                    protoEntities[deletedName] = new SnapshotEntity
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

        // Group entities by their source delta version to avoid fetching the same delta multiple times
        var entitiesByDelta = liveEntities
            .GroupBy(e => e.Value.LatestVersion)
            .ToDictionary(g => g.Key, g => g.ToList());

        // Pre-fetch all unique delta data blobs in parallel (limited concurrency)
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

            // Build the proto entity with version arrays
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
                // Fallback: we know it's alive with this version
                protoEntity.LastInsertedVersion.Add(deltaVersion);
            }

            protoEntities[entity.Key] = protoEntity;
        }

        return targetSnapshotStreamPosition;
    }

    private async Task<long> CopyRelevantItemsFromOldSnapshot(ImmutableDatasetState currentIndex,
        Dictionary<string, SnapshotEntity> protoEntities,
        Dictionary<string, SnapshotEntity> oldSnapshotProtoEntities,
        Stream targetSnapshotStream, long targetSnapshotStreamPosition, CancellationToken stoppingToken)
    {
        await _storageProvider.ReadSnapshotData(currentIndex.SnapshotVersion, sourceStream =>
        {
            long sourceStreamPosition = 0;

            foreach (var entity in currentIndex.Entities
                         .Where(x => x.Value.LatestVersion <= currentIndex.SnapshotVersion)
                         .OrderBy(x => x.Value.Position))
            {
                if (stoppingToken.IsCancellationRequested)
                    return;

                // Entity is in the trie → alive. Copy its data.
                var entityLength = entity.Value.Length;

                targetSnapshotStreamPosition += DatasetHelper.WriteProtobufLengthPrefix(targetSnapshotStream, entityLength);
                var targetEntityPosition = targetSnapshotStreamPosition;

                var gap = entity.Value.Position - sourceStreamPosition;
                sourceStreamPosition += SeekForwardBy(sourceStream, gap);

                EfficientCopy(sourceStream, targetSnapshotStream, entityLength);

                sourceStreamPosition += entityLength;
                targetSnapshotStreamPosition += entityLength;

                // Preserve the old snapshot proto entity (with full version arrays), update Position
                if (oldSnapshotProtoEntities.TryGetValue(entity.Key, out var oldProtoEntity))
                {
                    var updatedProtoEntity = oldProtoEntity.Clone();
                    updatedProtoEntity.Position = targetEntityPosition;
                    updatedProtoEntity.Length = entityLength;
                    protoEntities[entity.Key] = updatedProtoEntity;
                }
                else
                {
                    // Fallback: create a minimal proto entity
                    protoEntities[entity.Key] = new SnapshotEntity
                    {
                        LastInsertedVersion = { entity.Value.LatestVersion },
                        Position = targetEntityPosition,
                        Length = entityLength,
                        Crc32 = entity.Value.Crc32
                    };
                }
            }
        });

        stoppingToken.ThrowIfCancellationRequested();
        return targetSnapshotStreamPosition;
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
