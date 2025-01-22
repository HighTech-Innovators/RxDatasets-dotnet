using System.Reactive.Disposables;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Notifications;

namespace RxDatasets.StorageProviders.ReadOnly;

public class ReadOnlyCacheStorageProvider<T, TStorageProvider>(
    TStorageProvider readOnlyStorageProvider,
    IOptions<DtoStorageCacheConfig> cacheConfig,
    INotificationsHub<T>? notificationsHub,
    ILogger<ReadOnlyCacheStorageProvider<T, TStorageProvider>> logger)
    : IReadOnlyStorageProvider<T>, IHostedService where TStorageProvider : IReadOnlyStorageProvider<T>
{
    private const int MB_BYTES = 1024 * 1024;
    protected readonly IMemoryCache MemoryCache = new MemoryCache(new MemoryCacheOptions()
    {
        SizeLimit = cacheConfig.Value.CacheSizeMB * MB_BYTES
    });
    private ulong _lastCachedSnapshot;
    private readonly CompositeDisposable _subscriptions = new();

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if(notificationsHub == null) 
            return InitLoop(cancellationToken);
        
        _subscriptions.Add(notificationsHub.Subscribe<StorageProviderReadinessNotifcation>(async (notification) =>
        {
            if(notification.ServiceStatus == ServiceStatus.NotReady)
                return;

            await InitCache(cancellationToken);

        }));

        _subscriptions.Add(notificationsHub.Subscribe<SnapshotCreatedNotification>(async _ =>
        {
            var (_, _, snapshots) = await ListVersions();

            if (snapshots.Count == 0 || snapshots.Max(s => s.Version) <= _lastCachedSnapshot)
                return;

            await InitCache(cancellationToken);

        }));
        
        return Task.CompletedTask;
    }
    
    private async Task InitLoop(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await InitCache(cancellationToken);
                break;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to initialize object storage provider");
                await Task.Delay(5 * 1000, cancellationToken);
            }
        }
    }
    private async Task InitCache(CancellationToken ct)
    {
        var (_, _, snapshots) = await ListVersions();
        foreach (var version in snapshots.Select(s => s.Version).OrderDescending())
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                var oldSnapshotVersion = _lastCachedSnapshot;
                await TryCacheSnapshot(version);
                _lastCachedSnapshot = version;
                InvalidateSnapshotCache(oldSnapshotVersion);

                logger.LogInformation($"Initialized cache with snapshot v{version}");
                break;
            }
            catch (FileNotFoundException e)
            {
                logger.LogInformation(e,
                    $"Skipping snapshot v{version} because {e.FileName} was not found, we will try the next available snapshot for initial caching.");
            }
            catch (Exception e)
            {
                logger.LogWarning(e,
                    $"Skipping snapshot v{version} because we got {e} , we will try the next available snapshot for initial caching.");
            }
        }
    }

    private void InvalidateSnapshotCache(ulong oldSnapshotVersion)
    {
        if (oldSnapshotVersion <= 0) return;
        var cacheKey = GetCacheKey(oldSnapshotVersion, ObjectNameIndexOrDataEnum.DATA,
            ObjectNameTypeEnum.SNAPSHOT);
        MemoryCache.Remove(cacheKey);
    }

    private async Task TryCacheSnapshot(ulong snapshotVersion)
    {
        var index = await ReadSnapshotIndex(snapshotVersion);
        var mutableSnapshotIndex = SnapshotIndex.Parser.ParseFrom(index.Span);
        var totalLength = mutableSnapshotIndex.Entities.Values.Sum(entity => entity.Length);
        if (totalLength <= cacheConfig.Value.CacheSizeMB * MB_BYTES)
        {
            _ = await ReadSnapshotData(snapshotVersion); // This triggers a cache set
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _subscriptions.Dispose();
        return Task.CompletedTask;
    }
    protected string GetCacheKey(ulong version, ObjectNameIndexOrDataEnum indexData, ObjectNameTypeEnum deltaOrSnapshot) => $"{deltaOrSnapshot}_{indexData}_{version}";
    private string GetPositionedCacheKey(ulong version, ObjectNameIndexOrDataEnum indexData, ObjectNameTypeEnum deltaOrSnapshot, long position, int length) => $"{deltaOrSnapshot}_{indexData}_{version}_pos{position}_len{length}";

    public async Task<ReadOnlyMemory<byte>> ReadDeltaIndex(ulong version)
    {
        var cache = GetCacheKey(version, ObjectNameIndexOrDataEnum.INDEX, ObjectNameTypeEnum.DELTA);
        var value = await MemoryCache.GetOrCreateAsync(cache, async entry =>
        {
            var data =await readOnlyStorageProvider.ReadDeltaIndex(version);
            entry.Size = data.Length;
            return data;
        });
        return value;
    }


    private void SetCache(string cacheKey, ReadOnlyMemory<byte> data)
    {
        MemoryCache.Set(cacheKey, data, new MemoryCacheEntryOptions()
        {
            Size = data.Length,
            Priority = CacheItemPriority.Low,
        });
    }

    public async Task<ReadOnlyMemory<byte>> ReadSnapshotIndex(ulong version)
    {
        var cache = GetCacheKey(version, ObjectNameIndexOrDataEnum.INDEX, ObjectNameTypeEnum.SNAPSHOT);
        var value = await MemoryCache.GetOrCreateAsync(cache, async entry =>
        {
            var data = await readOnlyStorageProvider.ReadSnapshotIndex(version);
            entry.Size= data.Length;
            return data;
        });
        return value;
    }

    public async Task<ReadOnlyMemory<byte>> ReadDataFromSources(List<(ObjectNameTypeEnum type, ulong version, long position)> sources, int length)
    {
        if (sources.Count == 0)
        {
            throw new ArgumentException($"at least one source must be filled");
        }

        foreach (var (type, version, position) in sources)
        {
            var cacheKeyWhole = GetCacheKey(version, ObjectNameIndexOrDataEnum.DATA, type);
            var cacheKeySpecificRange = GetPositionedCacheKey(version, ObjectNameIndexOrDataEnum.DATA, type, position, length);
            
            if (MemoryCache.TryGetValue(cacheKeySpecificRange, out ReadOnlyMemory<byte> data))
            {
                return data;
            }

            if (MemoryCache.TryGetValue(cacheKeyWhole, out data))
            {
                return data.Slice((int)position, length);
            }
        }

        var dataFromSources = await readOnlyStorageProvider.ReadDataFromSources(sources, length);

        sources.ForEach(s =>
        {
            var cacheKeySpecificRange = GetPositionedCacheKey(s.version, ObjectNameIndexOrDataEnum.DATA, s.type, s.position, length);
            SetCache(cacheKeySpecificRange, dataFromSources);
        });

        return dataFromSources;
    }

    public async Task<long> GetSnapshotDataContentLength(ulong snapshotVersion)
    {
        var cacheKeyWholeSnapshot = GetCacheKey(snapshotVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT);

        if (!MemoryCache.TryGetValue(cacheKeyWholeSnapshot, out ReadOnlyMemory<byte> data))
        {
            return data.Length;
        }

        return await readOnlyStorageProvider.GetSnapshotDataContentLength(snapshotVersion);
    }


    public Task ReadSnapshotData(ulong version, Action<Stream> callback)
    {

        var cacheKeyWholeSnapshot = GetCacheKey(version, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT);
        
        if (!MemoryCache.TryGetValue(cacheKeyWholeSnapshot, out ReadOnlyMemory<byte> data))
            return readOnlyStorageProvider.ReadSnapshotData(version, callback);
       
        var memoryStream = new MemoryStream(data.ToArray());
        using (memoryStream)
        {
            callback(memoryStream);
            return Task.CompletedTask;
        }

    }

    public Task ReadSnapshotDataAsync(ulong snapshotVersion, Func<Stream, Task> asyncCallback)
    {
        throw new NotImplementedException();
    }

    public async Task<ReadOnlyMemory<byte>> ReadSnapshotData(ulong snapshotVersion)
    {
        var cacheKey = GetCacheKey(snapshotVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT);
        return await MemoryCache.GetOrCreateAsync(cacheKey, async entry =>
        {
            var readSnapshotData = await readOnlyStorageProvider.ReadSnapshotData(snapshotVersion);
            entry.Size = readSnapshotData.Length;
            return readSnapshotData;
        });
    }

    public Task<(ulong OldestDelta, ulong NewestDelta, List<SnapshotType> Snapshots)> ListVersions()
    {
        return readOnlyStorageProvider.ListVersions();
    }
}
