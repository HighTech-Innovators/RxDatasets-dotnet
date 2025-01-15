using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Notifications;

namespace RxDatasets.StorageProviders.ReadOnly;

public class ReadOnlyObjectStorageProvider<T>(
        ILogger<ReadOnlyObjectStorageProvider<T>> logger,
        IOptions<StorageConfig> storageConfig,
        IDiskStorage<T> storage,
        IStorageReader<T> storageReader,
        INotificationsHub<T>? notificationsHub,
        bool retryInit = true,
        bool pollForChanges = true
    )
    : IReadOnlyStorageProvider<T>, IHostedService
{

    private Task? _initTask;
    
    protected readonly DeltaInfo DeltaInfo = new();
    protected List<SnapshotType> Snapshots = [];
    private readonly CancellationTokenSource _cts = new();

    
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _initTask = retryInit ? InitLoop(_cts.Token) : Initialize(_cts.Token);
        if (pollForChanges)
        {
            _ = Task.Run(async () =>
            {
                while (!_cts.Token.IsCancellationRequested)
                {
                    await CheckForVersionsUpdate();
                }
            }, cancellationToken);
        }
        await _initTask;
        
    }

    private async Task CheckForVersionsUpdate()
    {
        await Task.Delay(5000, _cts.Token);
        _initTask = Initialize(_cts.Token);
        await _initTask;
    }

    private async Task SendDatasetUpdatedNotificationsIfNeeded(DeltaInfo oldDeltaInfo, List<SnapshotType> oldSnapshots)
    {
        if (notificationsHub == null || !pollForChanges)
            return;        
        
        if (!oldDeltaInfo.Equals(DeltaInfo))
        {
            await notificationsHub.Notify(new DatasetUpdatedNotification(DeltaInfo.LatestDeltaVersion));
        }
        
        switch (oldSnapshots.Count)
        {
            case 0 when Snapshots.Count > 0:
                await notificationsHub.Notify(new SnapshotCreatedNotification(Snapshots.Max(s=>s.Version)));
                break;
            case > 0 when !oldSnapshots.Max(s => s.Version).Equals(Snapshots.Max(s => s.Version)):
                await notificationsHub.Notify(new SnapshotCreatedNotification(Snapshots.Max(s=>s.Version)));
                break;
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _cts.CancelAsync();
    }


    public Task ReadSnapshotDataAsync(ulong snapshotVersion, Func<Stream, Task> asyncCallback)
    {
        throw new NotImplementedException();
    }

    public Task<ReadOnlyMemory<byte>> ReadSnapshotData(ulong snapshotVersion)
    {
        return storageReader.ReadSnapshotData(snapshotVersion);
    }
    public Task ReadSnapshotData(ulong version, Action<Stream> callback)
    {
        return storageReader.ReadSnapshotData(version, callback);
    }
    
    public Task<ReadOnlyMemory<byte>> ReadDeltaIndex(ulong version)
    {
        return storageReader.ReadDeltaIndex(version);
    }

    public Task<ReadOnlyMemory<byte>> ReadSnapshotIndex(ulong version)
    {
        return storageReader.ReadSnapshotIndex(version);
    }

    public async Task<(ulong OldestDelta, ulong NewestDelta, List<SnapshotType> Snapshots)> ListVersions()
    {
        await EnsureProviderIsInitialized();
        
        return (DeltaInfo.EarliestDeltaVersion,
            DeltaInfo.LatestDeltaVersion, Snapshots);
    }

   

    public async Task<ReadOnlyMemory<byte>> ReadDataFromSources(
        List<(ObjectNameTypeEnum type, ulong version, long position)> sources, int length)
    {
        await EnsureProviderIsInitialized();
        sources = sources.Where(source =>
        {
            switch (source.type)
            {
                case ObjectNameTypeEnum.SNAPSHOT
                    when Snapshots.All(s => s.Version != source.version):
                case ObjectNameTypeEnum.DELTA when source.version < DeltaInfo.EarliestDeltaVersion:
                    // Skip, we don't think we can handle this version request
                    return false;
                default:
                    return true;
            }
        }).ToList();
       return await storageReader.ReadDataFromSources(sources, length);
    }

    private async Task EnsureProviderIsInitialized()
    {
        if (_initTask is { IsFaulted: true })
        {
            logger.LogError(_initTask.Exception, "Received a request, but the dataset initialization failed");
            throw new InvalidOperationException("Initialization failed", _initTask.Exception);
        }

        if (_initTask is { IsCompleted: true })
            return;

        if (_initTask is null)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(100));
            if (_initTask is null)
            {
                logger.LogError("Received a request before dataset started initializing");
                throw new InvalidOperationException("Initialization is not yet completed");
            }
        }

        if (!_initTask.IsCompleted)
        {
            try
            {
                await _initTask.WaitAsync(TimeSpan.FromSeconds(1));
            }
            catch (TimeoutException)
            {
                logger.LogError("Received a request before dataset is initialized");
                throw new InvalidOperationException("Initialization is not yet completed");
            }
        }
    }


    private async Task InitLoop(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Initialize(cancellationToken);
                break;
            }
            catch (SynchronizationLockException ex)
            {
                logger.LogError(ex, "Failed to initialize object storage provider because no lock to write version files was available");
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to initialize object storage provider");
                await Task.Delay(5 * 1000, cancellationToken);
            }
        }
    }

    private async Task Initialize(CancellationToken cancellationToken)
    {
        if (storage == null) throw new InvalidOperationException("Storage Provider was not initialized");
        
        var tempDeltaInfo = DeltaInfo.Clone();
        var tempSnapshots = Snapshots;
        var versions = await storageReader.ListVersions();
        DeltaInfo.EarliestDeltaVersion = versions.OldestDelta;
        DeltaInfo.LatestDeltaVersion = versions.NewestDelta;
        Snapshots = versions.Snapshots;  
        await SendDatasetUpdatedNotificationsIfNeeded(tempDeltaInfo, tempSnapshots);
    }
    
    public Task<long> GetSnapshotDataContentLength(ulong snapshotVersion)
    {
        var objectName = DatasetHelper.GetObjectName<T>(snapshotVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT,
            storageConfig.Value);
        return storage.GetObjectContentLength(objectName);
    }
    
}