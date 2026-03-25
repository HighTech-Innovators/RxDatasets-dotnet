using System.Collections.Concurrent;
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
    private ulong _currentlyCachedSnapshot;
    private readonly CompositeDisposable _subscriptions = new();

    // Coalesces concurrent requests for the same cache key into a single RPC.
    // MemoryCache.GetOrCreateAsync races when multiple callers arrive before the first completes.
    private readonly ConcurrentDictionary<string, Lazy<Task<ReadOnlyMemory<byte>>>> _inflightRequests = new();

    private async Task<ReadOnlyMemory<byte>> GetOrCreateCoalesced(string cacheKey, Func<Task<ReadOnlyMemory<byte>>> factory)
    {
        if (MemoryCache.TryGetValue(cacheKey, out ReadOnlyMemory<byte> cached))
            return cached;

        var lazy = _inflightRequests.GetOrAdd(cacheKey,
            _ => new Lazy<Task<ReadOnlyMemory<byte>>>(factory, LazyThreadSafetyMode.ExecutionAndPublication));

        try
        {
            var result = await lazy.Value;
            MemoryCache.Set(cacheKey, result, new MemoryCacheEntryOptions { Size = result.Length });
            return result;
        }
        finally
        {
            _inflightRequests.TryRemove(cacheKey, out _);
        }
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (notificationsHub == null)
            return InitLoop(cancellationToken);

        _subscriptions.Add(notificationsHub.Subscribe<StorageProviderReadinessNotifcation>(async (notification) =>
        {
            if (notification.ServiceStatus == ServiceStatus.NotReady)
                return;

            await InitCache(cancellationToken);

        }));

        _subscriptions.Add(notificationsHub.Subscribe<SnapshotCreatedNotification>(async _ =>
        {
            var (_, _, snapshots) = await ListVersions();

            if (snapshots.Count == 0 || snapshots.Max(s => s.Version) <= _currentlyCachedSnapshot)
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
    private async Task<IDisposable> Lock(SemaphoreSlim semaphore)
    {
        await semaphore.WaitAsync();
        return new ActionDisposable(() => semaphore.Release());
    }

    private readonly SemaphoreSlim _initSemaphore = new(1);
    private readonly TStorageProvider _readOnlyStorageProvider = readOnlyStorageProvider;

    private async Task InitCache(CancellationToken ct)
    {
        using var initLock = await Lock(_initSemaphore);

        var (_, _, snapshots) = await ListVersions();
        foreach (var version in snapshots.Select(s => s.Version).Where(v => v > _currentlyCachedSnapshot).OrderDescending())
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                var oldSnapshotVersion = _currentlyCachedSnapshot;
                await CacheSnapshot(version);
                _currentlyCachedSnapshot = version;
                InvalidateSnapshotCache(oldSnapshotVersion);

                logger.LogInformation($"Initialized cache with snapshot v{version}");
                return;
            }
            catch (FileNotFoundException e)
            {
                logger.LogInformation(e,
                    $"Skipping snapshot v{version} because {e.FileName} was not found, we will try the next available snapshot for initial caching.");
            }
            catch (Exception e)
            {
                logger.LogWarning(e,
                    $"Skipping snapshot v{version} because we got {e}, we will try the next available snapshot for initial caching.");
            }
        }
    }

    private void InvalidateSnapshotCache(ulong oldSnapshotVersion)
    {
        if (oldSnapshotVersion <= 0)
            return;
        var cacheKey = GetCacheKey(oldSnapshotVersion, ObjectNameIndexOrDataEnum.Data,
            ObjectNameTypeEnum.Snapshot);
        MemoryCache.Remove(cacheKey);
    }

    private async Task CacheSnapshot(ulong snapshotVersion)
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

    public Task<ReadOnlyMemory<byte>> ReadDeltaIndex(ulong version)
    {
        var cacheKey = GetCacheKey(version, ObjectNameIndexOrDataEnum.Index, ObjectNameTypeEnum.Delta);
        return GetOrCreateCoalesced(cacheKey, () => _readOnlyStorageProvider.ReadDeltaIndex(version));
    }


    private void SetCache(string cacheKey, ReadOnlyMemory<byte> data)
    {
        MemoryCache.Set(cacheKey, data, new MemoryCacheEntryOptions()
        {
            Size = data.Length,
            Priority = CacheItemPriority.Low,
        });
    }

    public Task<ReadOnlyMemory<byte>> ReadSnapshotIndex(ulong version)
    {
        var cacheKey = GetCacheKey(version, ObjectNameIndexOrDataEnum.Index, ObjectNameTypeEnum.Snapshot);
        return GetOrCreateCoalesced(cacheKey, () => _readOnlyStorageProvider.ReadSnapshotIndex(version));
    }

    public async Task<ReadOnlyMemory<byte>> ReadDataFromSources(List<(ObjectNameTypeEnum type, ulong version, long position)> sources, int length)
    {
        if (sources.Count == 0)
        {
            throw new ArgumentException($"at least one source must be filled");
        }

        foreach (var (type, version, position) in sources)
        {
            var cacheKeyWhole = GetCacheKey(version, ObjectNameIndexOrDataEnum.Data, type);
            var cacheKeySpecificRange = GetPositionedCacheKey(version, ObjectNameIndexOrDataEnum.Data, type, position, length);

            if (MemoryCache.TryGetValue(cacheKeySpecificRange, out ReadOnlyMemory<byte> data))
            {
                return data;
            }

            if (MemoryCache.TryGetValue(cacheKeyWhole, out data))
            {
                return data.Slice((int)position, length);
            }
        }

        var dataFromSources = await _readOnlyStorageProvider.ReadDataFromSources(sources, length);

        sources.ForEach(s =>
        {
            var cacheKeySpecificRange = GetPositionedCacheKey(s.version, ObjectNameIndexOrDataEnum.Data, s.type, s.position, length);
            SetCache(cacheKeySpecificRange, dataFromSources);
        });

        return dataFromSources;
    }

    private readonly ConcurrentDictionary<string, Lazy<Task<long>>> _inflightContentLengthRequests = new();

    public async Task<long> GetSnapshotDataContentLength(ulong snapshotVersion)
    {
        var cacheKeyWholeSnapshot = GetCacheKey(snapshotVersion, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Snapshot);

        if (MemoryCache.TryGetValue(cacheKeyWholeSnapshot, out ReadOnlyMemory<byte> data))
        {
            return data.Length;
        }

        var lengthCacheKey = $"contentlength_{snapshotVersion}";
        var lazy = _inflightContentLengthRequests.GetOrAdd(lengthCacheKey,
            _ => new Lazy<Task<long>>(() => _readOnlyStorageProvider.GetSnapshotDataContentLength(snapshotVersion),
                LazyThreadSafetyMode.ExecutionAndPublication));

        try
        {
            return await lazy.Value;
        }
        finally
        {
            _inflightContentLengthRequests.TryRemove(lengthCacheKey, out _);
        }
    }


    public Task ReadSnapshotData(ulong version, Action<Stream> callback)
    {

        var cacheKeyWholeSnapshot = GetCacheKey(version, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Snapshot);

        if (!MemoryCache.TryGetValue(cacheKeyWholeSnapshot, out ReadOnlyMemory<byte> data))
            return _readOnlyStorageProvider.ReadSnapshotData(version, callback);

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

    public Task<ReadOnlyMemory<byte>> ReadSnapshotData(ulong snapshotVersion)
    {
        var cacheKey = GetCacheKey(snapshotVersion, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Snapshot);
        return GetOrCreateCoalesced(cacheKey, () => _readOnlyStorageProvider.ReadSnapshotData(snapshotVersion));
    }

    public async IAsyncEnumerable<(ulong Version, ReadOnlyMemory<byte> IndexBytes)> ReadDeltaIndexRange(
        ulong fromVersionExclusive,
        ulong toVersionInclusive,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var (version, indexBytes) in _readOnlyStorageProvider
            .ReadDeltaIndexRange(fromVersionExclusive, toVersionInclusive, cancellationToken))
        {
            var cacheKey = GetCacheKey(version, ObjectNameIndexOrDataEnum.Index, ObjectNameTypeEnum.Delta);
            MemoryCache.Set(cacheKey, indexBytes, new MemoryCacheEntryOptions { Size = indexBytes.Length });
            yield return (version, indexBytes);
        }
    }

    public Task<(ulong OldestDelta, ulong NewestDelta, List<SnapshotType> Snapshots)> ListVersions()
    {
        return _readOnlyStorageProvider.ListVersions();
    }
}
