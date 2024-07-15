using System.Buffers;
using System.Collections.Immutable;
using Google.Protobuf;
using HTI.RxDatasets.Abstractions;
using HTI.RxDatasets.DatasetUpdateNotifications;
using HTI.RxDatasets.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace HTI.RxDatasets.BackgroundServices;

public class SnapshotCreationService<T> : IHostedService, IDisposable
{
    private const int MaxStackAllocSize = 8 * 1024;
    private readonly ILogger<SnapshotCreationService<T>> _logger;
    private readonly SemaphoreSlim _semaphore = new(1);
    private readonly IOptionsMonitor<SnapshotCreationConfig> _configMonitor;

    private readonly IStorageProvider<T> _storageProvider;
    private readonly DatasetHelper _datasetHelper;
    private readonly IDatasetUpdateNotifier<T> _datasetUpdateNotifier;
    private Timer? _timer;

    public SnapshotCreationService(
        ILogger<SnapshotCreationService<T>> logger,
        IStorageProvider<T> storageProvider,
        IDatasetUpdateNotifier<T> datasetUpdateNotifier,
        IOptionsMonitor<SnapshotCreationConfig> configMonitor,

        IOptions<DtoConfig> dtoConfig)
    {
        _logger = logger;
        _storageProvider = storageProvider;
        _datasetHelper = new DatasetHelper(logger, _storageProvider);
        _datasetUpdateNotifier = datasetUpdateNotifier;
        _configMonitor = configMonitor;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Snapshot Hosted Service is starting.");
        _datasetUpdateNotifier.Subscribe(OnDatasetUpdated);
        var creationPeriod = _configMonitor.CurrentValue.PeriodIntervalAsTimespan;
        _timer = new Timer(async (s) => {
            var latestSnapshotVersion = 0ul;

            var (oldestDelta, newestDelta, snapshots) = await _storageProvider.ListVersions();
            if (snapshots.Count > 0)
            {
                var latestSnapshot = snapshots.MaxBy(snapshot => snapshot.version);
                var utcNow = DateTime.UtcNow;
                if (utcNow - latestSnapshot.DateTime.UtcDateTime < creationPeriod) return;
                latestSnapshotVersion = latestSnapshot.version;
            }

            await CreateASnapshot(newestDelta, latestSnapshotVersion); 
        }, null, TimeSpan.Zero, creationPeriod);

        return Task.CompletedTask;
    }

    private async Task OnDatasetUpdated(DatasetUpdateNotification notification)
    {
        if (notification.Version % _configMonitor.CurrentValue.SnapshotInterval == 0) await CreateASnapshot(notification.Version, notification.SnapshotVersion);
    }

    private async Task CreateASnapshot(ulong version = 0ul, ulong latestSnapshot = 0ul)
    {
        _logger.LogInformation($"Snapshot Hosted {typeof(T).Name} Service is working.");
        var tempState = await _datasetHelper.GetIndexOfVersion(version, latestSnapshot);
        if (tempState.LatestVersion == 0 || (version == latestSnapshot)) return;
        await CreateNewSnapshot(tempState);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Snapshot Hosted Service is stopping.");

        _timer?.Change(Timeout.Infinite, 0);

        return Task.CompletedTask;
    }

    public void Dispose()
    {
        _timer?.Dispose();
    }

    private async Task<ImmutableDatasetState> CreateNewSnapshot(ImmutableDatasetState currentIndex)
    {
        var newSnapshotVersion = currentIndex.LatestVersion;

        _logger.LogInformation($"Creating snapshot version {newSnapshotVersion}");

        ImmutableDictionary<string, ImmutableEntity> newSnapshotEntities = new Dictionary<string, ImmutableEntity>().ToImmutableDictionary();

        var tempPath = Path.GetTempFileName();

        await using var targetSnapshotStream = File.OpenWrite(tempPath);
        long targetSnapshotStreamPosition = 0;

        //read previous snapshot if exists and insert it at the beginning of the new snapshot
        if (currentIndex.LatestSnapshotVersion > 0)
        {
            await _storageProvider.ReadSnapshotData(currentIndex.LatestSnapshotVersion, sourceStream =>
            {
                long sourceStreamPosition = 0;

                foreach (var entity in currentIndex.Entities
                             .Where(x => x.Value.GetLatestEntityVersion() <= currentIndex.LatestSnapshotVersion)
                             .OrderBy(x => x.Value.Position))
                {
                    if (entity.Value.GetLatestChangeType() == ChangeType.Deleted)
                    {
                        ImmutableEntity newSnapshotEntity = entity.Value with { Position = 0, Length = 0 };
                        newSnapshotEntities = newSnapshotEntities.Add(entity.Key, newSnapshotEntity);
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

                        ImmutableEntity newSnapshotEntity = entity.Value with { Position = targetEntityPosition, Length = entityLength };
                        newSnapshotEntities = newSnapshotEntities.Add(entity.Key, newSnapshotEntity);
                    }
                }
            });
        }

        foreach (var entity in currentIndex.Entities.Where(x => x.Value.GetLatestEntityVersion() > currentIndex.LatestSnapshotVersion))
        {

            if (entity.Value.GetLatestChangeType() == ChangeType.Deleted)
            {
                var newSnapshotEntity = entity.Value with { Position = 0, Length = 0 };
                newSnapshotEntities = newSnapshotEntities.Add(entity.Key, newSnapshotEntity);
            }
            else
            {
                var entityLength = entity.Value.Length;

                targetSnapshotStreamPosition += WriteLengthPrefix(targetSnapshotStream, entityLength);
                var targetEntityPosition = targetSnapshotStreamPosition;

                var latestEntityVersion = entity.Value.GetLatestEntityVersion();
                var entityBytes = await _storageProvider.ReadDeltaData(latestEntityVersion, entity.Value.Position, entity.Value.Length);

                await targetSnapshotStream.WriteAsync(entityBytes);
                targetSnapshotStreamPosition += entityLength;

                var newSnapshotEntity = entity.Value with { Position = targetEntityPosition, Length = entityLength };
                newSnapshotEntities = newSnapshotEntities.Add(entity.Key, newSnapshotEntity);
            }
        }

        if (targetSnapshotStreamPosition == 0)
        {
            WriteLengthPrefix(targetSnapshotStream, 0);
        }

        await targetSnapshotStream.FlushAsync();
        targetSnapshotStream.Close();

        await using var localSnapshotReader = File.OpenRead(tempPath);
        var newSnapshotIndex = new ImmutableDatasetState(newSnapshotEntities, newSnapshotVersion, currentIndex.LatestVersion);
        await _storageProvider.StoreSnapshot(newSnapshotVersion, newSnapshotIndex.ToProto().ToByteArray(), localSnapshotReader);

        localSnapshotReader.Close();

        File.Delete(tempPath);
        _logger.LogInformation($"Created snapshot version {newSnapshotVersion}");

        return newSnapshotIndex;
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

    private async Task<IDisposable> Lock()
    {
        await _semaphore.WaitAsync().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        return new ActionDisposable(() => _semaphore.Release());
    }

    private async Task<IDisposable> Lock(CancellationToken cancellationToken)
    {
        await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        return new ActionDisposable(() => _semaphore.Release());
    }
}