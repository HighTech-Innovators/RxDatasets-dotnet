using Google.Protobuf;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.DatasetUpdateNotifications;
using RxDatasets.Models;
using System.Buffers;
using System.Collections.Immutable;
using System.Reactive.Concurrency;

namespace RxDatasets.BackgroundServices;

public class SnapshotCreationService<T> : BackgroundService
{
    private const int MaxStackAllocSize = 8 * 1024;
    private readonly ILogger<SnapshotCreationService<T>> _logger;
    private readonly IOptions<SnapshotCreationConfig> _config;

    private readonly IStorageProvider<T> _storageProvider;
    private readonly DatasetHelper _datasetHelper;
    private readonly IDatasetUpdateNotifier<T> _datasetUpdateNotifier;
    private readonly IScheduler _scheduler;
    private readonly TimeSpan _currentValuePeriodIntervalAsTimespan;
    private volatile TaskCompletionSource _newSnapshotMayBeNeeded = new();
    private volatile int iteration = 0;

    public SnapshotCreationService(
        ILogger<SnapshotCreationService<T>> logger,
        IStorageProvider<T> storageProvider,
        IDatasetUpdateNotifier<T> datasetUpdateNotifier,
        IOptions<SnapshotCreationConfig> config, IScheduler scheduler)
    {
        _logger = logger;
        _storageProvider = storageProvider;
        _datasetHelper = new DatasetHelper(logger, _storageProvider);
        _datasetUpdateNotifier = datasetUpdateNotifier;
        _config = config;
        _scheduler = scheduler;
        _currentValuePeriodIntervalAsTimespan = _config.Value.PeriodIntervalAsTimespan;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation($"SnapshotCreationService<{typeof(T).Name}> Started");
        var subscription = _datasetUpdateNotifier.Subscribe(OnDatasetUpdated);
        try
        {
            await WorkAndWaitLoop(stoppingToken);
        }
        finally
        {
            _logger.LogInformation($"SnapshotCreationService<{typeof(T).Name}> Stopped");
            subscription?.Dispose();
        }
    }

    private async Task CreateSnapshotIfNecessary(CancellationToken stoppingToken)
    {
        var (_, newestDelta, snapshots) = await _storageProvider.ListVersions();
        var conditionsAreMet = AreSnapshotCreationConditionsMet(newestDelta, snapshots);
        if (!conditionsAreMet)
            return;

        var latestSnapshot = snapshots.Count > 0 ? snapshots.Max(s => s.version) : 0;
        await CreateASnapshot(newestDelta, latestSnapshot, stoppingToken);
    }

    private async Task WorkAndWaitLoop(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
              
                await CreateSnapshotIfNecessary(stoppingToken);
                Interlocked.Increment(ref iteration);
                await _newSnapshotMayBeNeeded.Task.WaitAsync(stoppingToken);
                _newSnapshotMayBeNeeded = new TaskCompletionSource();
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                _logger.LogWarning(e, "Exception during snapshot creation or check");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }

    private bool AreSnapshotCreationConditionsMet(ulong newestDelta,
        List<(ulong version, DateTimeOffset Datetime)> snapshots)
    {
        var (lastSnapshotVersion, lastSnapshotTime)  =
            snapshots.Count > 0 
                ? snapshots.MaxBy(s => s.version)
                : (0UL,(DateTimeOffset?)null);

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
        _scheduler.Schedule(timeUntilNext, _ => QueueSnapshotCheck().Wait());

        return false;
    }

    private async Task OnDatasetUpdated(DatasetUpdateNotification notification)
    {
        await QueueSnapshotCheck();
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

    private async Task CreateASnapshot(ulong version, ulong latestSnapshot, CancellationToken stoppingToken)
    {
        _logger.LogInformation($"Snapshot Hosted {typeof(T).Name} Service is working. Creating snapshot for delta {version}. The previous snapshot is {latestSnapshot}");
        var tempState = await _datasetHelper.GetIndexOfVersion(version, latestSnapshot);
        await CreateNewSnapshot(tempState, stoppingToken);
    }

    private volatile int _handlingNotifications;
    private async Task QueueSnapshotCheck()
    {
        try
        {
            var stackDepth = Interlocked.Increment(ref _handlingNotifications);
            var mustWait = stackDepth == 1;

            var previousIteration = iteration;
            _ = _newSnapshotMayBeNeeded.TrySetResult();

            while (mustWait && iteration == previousIteration)
                await Task.Delay(1);
        }
        finally
        {
            Interlocked.Decrement(ref _handlingNotifications);
        }
    }
    private async Task CreateNewSnapshot(ImmutableDatasetState currentIndex, CancellationToken stoppingToken)
    {
        var newSnapshotVersion = currentIndex.LatestVersion;

        _logger.LogInformation($"Creating snapshot version {newSnapshotVersion}");

        Dictionary<string, ImmutableEntity> newSnapshotEntities = [];

        var tempPath = Path.GetTempFileName();

        await using var targetSnapshotStream = File.OpenWrite(tempPath);
        try
        {
            long targetSnapshotStreamPosition = 0;

            if (currentIndex.LatestSnapshotVersion > 0)
                targetSnapshotStreamPosition =
                    await CopyRelevantItemsFromOldSnapshot(currentIndex, newSnapshotEntities, targetSnapshotStream,
                        targetSnapshotStreamPosition, stoppingToken);

            targetSnapshotStreamPosition = await CopyRelevantItemsFromDeltas(currentIndex, newSnapshotEntities,
                targetSnapshotStream, targetSnapshotStreamPosition, stoppingToken);

            if (targetSnapshotStreamPosition == 0)
                WriteLengthPrefix(targetSnapshotStream, 0);

            await targetSnapshotStream.FlushAsync(stoppingToken);
            targetSnapshotStream.Close();

            await using var localSnapshotReader = File.OpenRead(tempPath);
            var newSnapshotIndex =
                new ImmutableDatasetState(newSnapshotEntities.ToImmutableDictionary(), newSnapshotVersion,
                    currentIndex.LatestVersion);

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
        Dictionary<string, ImmutableEntity> newSnapshotEntities,
        Stream targetSnapshotStream, long targetSnapshotStreamPosition, CancellationToken stoppingToken)
    {
        foreach (var entity in currentIndex.Entities.Where(x =>
                     x.Value.GetLatestEntityVersion() > currentIndex.LatestSnapshotVersion))
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

                targetSnapshotStreamPosition += WriteLengthPrefix(targetSnapshotStream, entityLength);
                var targetEntityPosition = targetSnapshotStreamPosition;

                var latestEntityVersion = entity.Value.GetLatestEntityVersion();
                var entityBytes = await _storageProvider.ReadDeltaData(latestEntityVersion, entity.Value.Position,
                    entity.Value.Length);

                await targetSnapshotStream.WriteAsync(entityBytes, stoppingToken);
                targetSnapshotStreamPosition += entityLength;

                var newSnapshotEntity = entity.Value with { Position = targetEntityPosition, Length = entityLength };
                newSnapshotEntities.Add(entity.Key, newSnapshotEntity);
            }
        }

        return targetSnapshotStreamPosition;
    }

    private async Task<long> CopyRelevantItemsFromOldSnapshot(ImmutableDatasetState currentIndex,
        Dictionary<string, ImmutableEntity> newSnapshotEntities,
        Stream targetSnapshotStream, long targetSnapshotStreamPosition, CancellationToken stoppingToken)
    {
        await _storageProvider.ReadSnapshotData(currentIndex.LatestSnapshotVersion, sourceStream =>
        {
            long sourceStreamPosition = 0;

            foreach (var entity in currentIndex.Entities
                         .Where(x => x.Value.GetLatestEntityVersion() <= currentIndex.LatestSnapshotVersion)
                         .OrderBy(x => x.Value.Position))
            {
                if (stoppingToken.IsCancellationRequested)
                    return;

                if (entity.Value.GetLatestChangeType() == ChangeType.Deleted)
                {
                    ImmutableEntity newSnapshotEntity = entity.Value with { Position = 0, Length = 0 };
                    newSnapshotEntities.Add(entity.Key, newSnapshotEntity);
                }
                else
                {
                    var entityLength = entity.Value.Length;

                    targetSnapshotStreamPosition += WriteLengthPrefix(targetSnapshotStream, entityLength);
                    var targetEntityPosition = targetSnapshotStreamPosition;

                    var gap = entity.Value.Position - sourceStreamPosition;
                    sourceStreamPosition += SeekForwardBy(sourceStream, gap);

                    EfficientCopy(sourceStream, targetSnapshotStream, entityLength);
                    sourceStreamPosition += entityLength;
                    targetSnapshotStreamPosition += entityLength;

                    ImmutableEntity newSnapshotEntity = entity.Value with
                    {
                        Position = targetEntityPosition, Length = entityLength
                    };
                    newSnapshotEntities.Add(entity.Key, newSnapshotEntity);
                }
            }
        });

        stoppingToken.ThrowIfCancellationRequested();
        return targetSnapshotStreamPosition;
    }

    public static long EfficientCopy(Stream sourceStream, Stream snapshotFileStream, int numberOfBytesToCopy)
    {
        if (numberOfBytesToCopy <= MaxStackAllocSize)
        {
            Span<byte> buffer = stackalloc byte[numberOfBytesToCopy];
            sourceStream.ReadExactly(buffer);
            snapshotFileStream.Write(buffer);
        }
        else
        {
            var buffer = ArrayPool<byte>.Shared.Rent(numberOfBytesToCopy);
            try
            {
                var span = buffer[..numberOfBytesToCopy];
                sourceStream.ReadExactly(span);
                snapshotFileStream.Write(span);
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

    private static int WriteLengthPrefix(Stream stream, int length)
    {
        // Google.Protobuf.CodedOutputStream has a method for writing a length prefix as a varint
        using var cos = new CodedOutputStream(stream, true);
        cos.WriteLength(length);
        var lengthOfTheVarPrefix = CodedOutputStream.ComputeLengthSize(length);
        return lengthOfTheVarPrefix;
    }
}