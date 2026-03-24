using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RxDatasets.Abstractions;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Notifications;

namespace RxDatasets.StorageProvider.GoogleCloud;

public class ReadOnlyHybridBigtableStorageProvider<T> : IReadOnlyStorageProvider<T>, IHostedService
{
    private readonly ILogger<ReadOnlyHybridBigtableStorageProvider<T>> _logger;
    private readonly IStorageReader<T> _storageReader;
    private readonly INotificationsHub<T>? _notificationsHub;
    private readonly bool _startIfInitFails;
    private readonly CancellationTokenSource _cts = new();

    protected readonly DeltaInfo DeltaInfo = new();
    protected List<SnapshotType> Snapshots = [];
    protected Task? InitTask;

    public ReadOnlyHybridBigtableStorageProvider(
        ILogger<ReadOnlyHybridBigtableStorageProvider<T>> logger,
        IStorageReader<T> storageReader,
        INotificationsHub<T>? notificationsHub,
        bool startIfInitFails = true)
    {
        _logger = logger;
        _storageReader = storageReader;
        _notificationsHub = notificationsHub;
        _startIfInitFails = startIfInitFails;
    }

    public virtual async Task StartAsync(CancellationToken cancellationToken)
    {
        cancellationToken.Register(_cts.Cancel);

        if (_startIfInitFails)
        {
            _ = InitLoop(_cts.Token);
        }
        else
        {
            await Initialize(_cts.Token);
            await NotifyReadiness(ServiceStatus.Ready);
        }

        _ = InitAndUpdateLoop(_cts.Token);
    }

    public virtual async Task StopAsync(CancellationToken cancellationToken)
    {
        await _cts.CancelAsync();
        await NotifyReadiness(ServiceStatus.NotReady);
    }

    public Task ReadSnapshotDataAsync(ulong snapshotVersion, Func<Stream, Task> asyncCallback)
    {
        throw new NotImplementedException();
    }

    public async Task<ReadOnlyMemory<byte>> ReadSnapshotData(ulong snapshotVersion)
    {
        await EnsureProviderIsInitialized();
        return await _storageReader.ReadSnapshotData(snapshotVersion);
    }

    public async Task ReadSnapshotData(ulong version, Action<Stream> callback)
    {
        await EnsureProviderIsInitialized();
        await _storageReader.ReadSnapshotData(version, callback);
    }

    public async Task<ReadOnlyMemory<byte>> ReadDeltaIndex(ulong version)
    {
        await EnsureProviderIsInitialized();
        return await _storageReader.ReadDeltaIndex(version);
    }

    public async Task<ReadOnlyMemory<byte>> ReadSnapshotIndex(ulong version)
    {
        await EnsureProviderIsInitialized();
        return await _storageReader.ReadSnapshotIndex(version);
    }

    public async Task<(ulong OldestDelta, ulong NewestDelta, List<SnapshotType> Snapshots)> ListVersions()
    {
        await EnsureProviderIsInitialized();
        return (DeltaInfo.EarliestDeltaVersion, DeltaInfo.LatestDeltaVersion, Snapshots);
    }

    public async Task<ReadOnlyMemory<byte>> ReadDataFromSources(
        List<(ObjectNameTypeEnum type, ulong version, long position)> sources, int length)
    {
        await EnsureProviderIsInitialized();
        return await _storageReader.ReadDataFromSources(
            sources.Where(source =>
            {
                return source.type switch
                {
                    ObjectNameTypeEnum.Snapshot => Snapshots.Any(s => s.Version == source.version),
                    ObjectNameTypeEnum.Delta => DeltaInfo.EarliestDeltaVersion == 0 ||
                                                (source.version >= DeltaInfo.EarliestDeltaVersion &&
                                                 source.version <= DeltaInfo.LatestDeltaVersion),
                    _ => false
                };
            }).ToList(), length);
    }

    public async Task<long> GetSnapshotDataContentLength(ulong snapshotVersion)
    {
        await EnsureProviderIsInitialized();
        return await _storageReader.GetSnapshotDataContentLength(snapshotVersion);
    }

    public async IAsyncEnumerable<(ulong Version, ReadOnlyMemory<byte> IndexBytes)> ReadDeltaIndexRange(
        ulong fromVersionExclusive,
        ulong toVersionInclusive,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await EnsureProviderIsInitialized();
        await foreach (var item in _storageReader.ReadDeltaIndexRange(fromVersionExclusive, toVersionInclusive, cancellationToken))
        {
            yield return item;
        }
    }

    protected async Task EnsureProviderIsInitialized()
    {
        if (InitTask is { IsFaulted: true })
        {
            _logger.LogError(InitTask.Exception, "Received a request, but the dataset initialization failed");
            throw new InvalidOperationException("Initialization failed", InitTask.Exception);
        }

        if (InitTask is { IsCompleted: true })
        {
            return;
        }

        if (InitTask is null)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(100));
            if (InitTask is null)
            {
                _logger.LogError("Received a request before dataset started initializing");
                throw new InvalidOperationException("Initialization is not yet completed");
            }
        }

        if (!InitTask.IsCompleted)
        {
            try
            {
                await InitTask.WaitAsync(TimeSpan.FromSeconds(1));
            }
            catch (TimeoutException)
            {
                _logger.LogError("Received a request before dataset is initialized");
                throw new InvalidOperationException("Initialization is not yet completed");
            }
        }
    }

    protected virtual async Task Initialize(CancellationToken cancellationToken)
    {
        var previousDeltaInfo = DeltaInfo.Clone();
        var previousSnapshots = Snapshots.ToList();

        InitTask = DoInitialize(cancellationToken);
        await InitTask;
        await SendDatasetUpdatedNotificationsIfNeeded(previousDeltaInfo, previousSnapshots);
    }

    protected async Task NotifyReadiness(ServiceStatus status)
    {
        if (_notificationsHub != null)
        {
            await _notificationsHub.Notify(new StorageProviderReadinessNotifcation(status));
        }
    }

    private async Task DoInitialize(CancellationToken cancellationToken)
    {
        var versions = await _storageReader.ListVersions();
        DeltaInfo.EarliestDeltaVersion = versions.OldestDelta;
        DeltaInfo.LatestDeltaVersion = versions.NewestDelta;
        Snapshots = versions.Snapshots;
    }

    private async Task InitLoop(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Initialize(cancellationToken);
                await NotifyReadiness(ServiceStatus.Ready);
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize read-only hybrid storage provider");
                await Task.Delay(5 * 1000, cancellationToken);
            }
        }
    }

    private async Task InitAndUpdateLoop(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(5 * 1000, cancellationToken);
            try
            {
                await Initialize(cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to refresh read-only hybrid storage provider");
            }
        }
    }

    private async Task SendDatasetUpdatedNotificationsIfNeeded(DeltaInfo oldDeltaInfo, List<SnapshotType> oldSnapshots)
    {
        if (_notificationsHub == null)
        {
            return;
        }

        if (!oldDeltaInfo.Equals(DeltaInfo))
        {
            await _notificationsHub.Notify(new DatasetUpdatedNotification(DeltaInfo.LatestDeltaVersion));
        }

        switch (oldSnapshots.Count)
        {
            case 0 when Snapshots.Count > 0:
                await _notificationsHub.Notify(new SnapshotCreatedNotification(Snapshots.Max(s => s.Version)));
                break;
            case > 0 when Snapshots.Count > 0 && !oldSnapshots.Max(s => s.Version).Equals(Snapshots.Max(s => s.Version)):
                await _notificationsHub.Notify(new SnapshotCreatedNotification(Snapshots.Max(s => s.Version)));
                break;
        }
    }
}
