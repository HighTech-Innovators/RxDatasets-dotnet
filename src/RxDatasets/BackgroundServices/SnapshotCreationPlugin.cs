using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.Models;
using System.Buffers;
using System.Collections.Immutable;
using System.Reactive.Concurrency;

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
        IOptions<SnapshotCreationConfig> config, IScheduler scheduler)
    {
        _logger = logger;
        _storageProvider = storageProvider;
        _datasetHelper = new DatasetHelper(logger, _storageProvider);
        _config = config;
        _scheduler = scheduler;
        _currentValuePeriodIntervalAsTimespan = _config.Value.PeriodIntervalAsTimespan;
    }
    public string ConsumerIdentifier { get; } = $"SnapshotCreationPlugin-of-{typeof(T)}";

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
       var snapshot  =
            snapshots.Count > 0 
                ? snapshots.MaxBy(s => s.Version)
                : new SnapshotType(0, null);
       
       ulong lastSnapshotVersion = snapshot.Version;
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
        _scheduler.Schedule(timeUntilNext, _ => HandleNewDeltas(CancellationToken.None).Wait());

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

        Dictionary<string, ImmutableSnapshotEntity> newSnapshotEntities = [];

        var tempPath = DatasetHelper.GetTempFilePath();

        await using var targetSnapshotStream = File.OpenWrite(tempPath);
        try
        {
            long targetSnapshotStreamPosition = 0;

            if (currentIndex.SnapshotVersion > 0)
                targetSnapshotStreamPosition =
                    await CopyRelevantItemsFromOldSnapshot(currentIndex, newSnapshotEntities, targetSnapshotStream,
                        targetSnapshotStreamPosition, stoppingToken);

            targetSnapshotStreamPosition = await CopyRelevantItemsFromDeltas(currentIndex, newSnapshotEntities,
                targetSnapshotStream, targetSnapshotStreamPosition, stoppingToken);

            if (targetSnapshotStreamPosition == 0)
                DatasetHelper.WriteProtobufLengthPrefix(targetSnapshotStream, 0);

            await targetSnapshotStream.FlushAsync(stoppingToken);
            targetSnapshotStream.Close();

            await using var localSnapshotReader = File.OpenRead(tempPath);
            var newSnapshotIndex =
                new ImmutableDatasetState(newSnapshotEntities.ToImmutableDictionary(), newSnapshotVersion,
                    currentIndex.LatestVersion);
            localSnapshotReader.Position = 0;
            stoppingToken.ThrowIfCancellationRequested();

            await _storageProvider.StoreSnapshot(newSnapshotVersion, newSnapshotIndex.ToProto().ToByteArray(),
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

    private async Task<long> CopyRelevantItemsFromDeltas(ImmutableDatasetState currentIndex,
        Dictionary<string, ImmutableSnapshotEntity> newSnapshotEntities,
        Stream targetSnapshotStream, long targetSnapshotStreamPosition, CancellationToken stoppingToken)
    {
        foreach (var entity in currentIndex.Entities.Where(x =>
                     x.Value.GetLatestEntityVersion() > currentIndex.SnapshotVersion))
        {
            stoppingToken.ThrowIfCancellationRequested();

            if (entity.Value.GetLatestChangeType() == ChangeType.Deleted)
            {
                var newSnapshotEntity = entity.Value with { Position = 0, Length = 0 };
                newSnapshotEntities.Add(entity.Key, newSnapshotEntity);
            }
            else
            {
                var entityLength = entity.Value.Length;

                targetSnapshotStreamPosition += DatasetHelper.WriteProtobufLengthPrefix(targetSnapshotStream, entityLength);
                var targetEntityPosition = targetSnapshotStreamPosition;

                var latestEntityVersion = entity.Value.GetLatestEntityVersion();
                var entityBytes = await _storageProvider.ReadDataFromSources([(ObjectNameTypeEnum.DELTA, latestEntityVersion, entity.Value.Position)], entity.Value.Length);

                await targetSnapshotStream.WriteAsync(entityBytes, stoppingToken);
                targetSnapshotStreamPosition += entityLength;

                var newSnapshotEntity = entity.Value with { Position = targetEntityPosition, Length = entityLength };
                newSnapshotEntities.Add(entity.Key, newSnapshotEntity);
            }
        }

        return targetSnapshotStreamPosition;
    }

    private async Task<long> CopyRelevantItemsFromOldSnapshot(ImmutableDatasetState currentIndex,
        Dictionary<string, ImmutableSnapshotEntity> newSnapshotEntities,
        Stream targetSnapshotStream, long targetSnapshotStreamPosition, CancellationToken stoppingToken)
    {
        await _storageProvider.ReadSnapshotData(currentIndex.SnapshotVersion, sourceStream =>
        {
            long sourceStreamPosition = 0;

            foreach (var entity in currentIndex.Entities
                         .Where(x => x.Value.GetLatestEntityVersion() <= currentIndex.SnapshotVersion)
                         .OrderBy(x => x.Value.Position))
            {
                if (stoppingToken.IsCancellationRequested)
                    return;

                if (entity.Value.GetLatestChangeType() == ChangeType.Deleted)
                {
                    ImmutableSnapshotEntity newSnapshotSnapshotEntity = entity.Value with { Position = 0, Length = 0 };
                    newSnapshotEntities.Add(entity.Key, newSnapshotSnapshotEntity);
                }
                else
                {
                    var entityLength = entity.Value.Length;

                    targetSnapshotStreamPosition += DatasetHelper.WriteProtobufLengthPrefix(targetSnapshotStream, entityLength);
                    var targetEntityPosition = targetSnapshotStreamPosition;

                    var gap = entity.Value.Position - sourceStreamPosition;
                    sourceStreamPosition += SeekForwardBy(sourceStream, gap);

                    EfficientCopy(sourceStream, targetSnapshotStream, entityLength);

                    sourceStreamPosition += entityLength;
                    targetSnapshotStreamPosition += entityLength;

                    ImmutableSnapshotEntity newSnapshotSnapshotEntity = entity.Value with
                    {
                        Position = targetEntityPosition, Length = entityLength
                    };
                    newSnapshotEntities.Add(entity.Key, newSnapshotSnapshotEntity);
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