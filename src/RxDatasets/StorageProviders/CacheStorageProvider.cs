using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProviders.ReadOnly;

namespace RxDatasets.StorageProviders;

public class CacheStorageProvider<T, TStorageProvider> : ReadOnlyCacheStorageProvider<T, TStorageProvider>, IStorageProvider<T> where TStorageProvider : IStorageProvider<T>
{
    private const long MB_BYTES = 1024 * 1024;

    private readonly IOptions<DtoStorageCacheConfig> _cacheConfig;
    private readonly IStorageProvider<T> _storageProvider;
    private readonly ILogger<CacheStorageProvider<T, TStorageProvider>> _logger;

    public CacheStorageProvider(TStorageProvider storageProvider,
        IOptions<DtoStorageCacheConfig> cacheConfig,
        ILogger<CacheStorageProvider<T, TStorageProvider>> logger,
        INotificationsHub<T>? notificationsHub) : base(storageProvider, cacheConfig, notificationsHub, logger)
    {
        _storageProvider = storageProvider;
        _logger = logger;
        _cacheConfig = cacheConfig;
        StartAsync(CancellationToken.None);
    }

    public async Task StoreSnapshot(ulong snapshotVersion, byte[] index, byte[] data)
    {
        var indexCacheKey = GetCacheKey(snapshotVersion, ObjectNameIndexOrDataEnum.Index, ObjectNameTypeEnum.Snapshot);
        var dataCacheKey = GetCacheKey(snapshotVersion, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Snapshot);
        try
        {
            SetCache(indexCacheKey, index);
            var bytesAsSpan = new ReadOnlyMemory<byte>(data);
            SetCache(dataCacheKey, bytesAsSpan);
            await _storageProvider.StoreSnapshot(snapshotVersion, index, data);
            TrackSnapshotAndScheduleEviction(snapshotVersion);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Something went wrong while storing snapshot data!");
            MemoryCache.Remove(indexCacheKey);
            MemoryCache.Remove(dataCacheKey);
            throw;
        }
    }

    public async Task StoreDelta(ulong deltaVersion, byte[] index, byte[] data)
    {
        var indexCacheKey = GetCacheKey(deltaVersion, ObjectNameIndexOrDataEnum.Index, ObjectNameTypeEnum.Delta);
        var dataCacheKey = GetCacheKey(deltaVersion, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Delta);

        try
        {
            SetCache(indexCacheKey, new ReadOnlyMemory<byte>(index));
            SetCache(dataCacheKey, new ReadOnlyMemory<byte>(data));

            await _storageProvider.StoreDelta(deltaVersion, index, data);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Something went wrong while storing delta data cache!");
            MemoryCache.Remove(indexCacheKey);
            MemoryCache.Remove(dataCacheKey);
            throw;
        }
    }


    private void SetCache(string cacheKey, ReadOnlyMemory<byte> data)
    {
        MemoryCache.Set(cacheKey, data, new MemoryCacheEntryOptions()
        {
            Size = data.Length,
            Priority = CacheItemPriority.Low,
        });
    }

    public Task DeleteOlderThan(ulong version, bool keepAllDeltas = false)
    {
        return _storageProvider.DeleteOlderThan(version, keepAllDeltas);
    }

    public async Task StoreSnapshot(ulong snapshotVersion, byte[] index, Stream data)
    {
        var indexCacheKey = GetCacheKey(snapshotVersion, ObjectNameIndexOrDataEnum.Index, ObjectNameTypeEnum.Snapshot);
        var dataCacheKey = GetCacheKey(snapshotVersion, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Snapshot);
        try
        {
            SetCache(indexCacheKey, index);
            if (data.Length <= _cacheConfig.Value.CacheSizeMB * MB_BYTES && data.Length <= int.MaxValue)
            {
                var byteArray = new byte[data.Length];
                _ = await data.ReadAsync(byteArray, 0, (int)data.Length);
                var bytesAsSpan = new ReadOnlyMemory<byte>(byteArray);
                SetCache(dataCacheKey, bytesAsSpan);
                await _storageProvider.StoreSnapshot(snapshotVersion, index, byteArray);
                TrackSnapshotAndScheduleEviction(snapshotVersion);
            }
            else if (data.Length <= _cacheConfig.Value.CacheSizeMB * MB_BYTES)
            {
                // >2 GB but within cache limit: read into 1 GB blocks for caching,
                // then pass the stream (reset to start) to the underlying provider.
                var blocks = new List<ReadOnlyMemory<byte>>();
                var buffer = new byte[BlockSize];
                while (true)
                {
                    int totalRead = 0;
                    while (totalRead < BlockSize)
                    {
                        int read = await data.ReadAsync(buffer, totalRead, BlockSize - totalRead);
                        if (read == 0) break;
                        totalRead += read;
                    }
                    if (totalRead == 0) break;

                    var block = new byte[totalRead];
                    Buffer.BlockCopy(buffer, 0, block, 0, totalRead);
                    blocks.Add(block);
                }

                for (int i = 0; i < blocks.Count; i++)
                {
                    var blockCacheKey = GetBlockCacheKey(snapshotVersion, ObjectNameTypeEnum.Snapshot, i);
                    SetCache(blockCacheKey, blocks[i]);
                }

                // Reset stream and pass to underlying provider for upload
                data.Position = 0;
                await _storageProvider.StoreSnapshot(snapshotVersion, index, data);
                TrackSnapshotAndScheduleEviction(snapshotVersion, blocks.Count);
            }
            else
            {
                await _storageProvider.StoreSnapshot(snapshotVersion, index, data);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Something went wrong while storing snapshot data!");
            MemoryCache.Remove(indexCacheKey);
            MemoryCache.Remove(dataCacheKey);
            for (int i = 0; i < _currentlyCachedBlockCount; i++)
                MemoryCache.Remove(GetBlockCacheKey(snapshotVersion, ObjectNameTypeEnum.Snapshot, i));
            throw;
        }
    }

    private const int EvictionDelaySeconds = 30;

    /// <summary>
    /// Tracks the new snapshot version so <c>InitCache</c> won't re-download it,
    /// and schedules a delayed eviction of the previous snapshot's cache entries.
    /// The delay gives in-flight readers time to finish serving from the old data.
    /// </summary>
    private void TrackSnapshotAndScheduleEviction(ulong snapshotVersion, int newBlockCount = 0)
    {
        var oldVersion = _currentlyCachedSnapshot;
        var oldBlockCount = _currentlyCachedBlockCount;
        _currentlyCachedSnapshot = snapshotVersion;
        _currentlyCachedBlockCount = newBlockCount;

        if (oldVersion > 0 && oldVersion != snapshotVersion)
        {
            _ = Task.Run(async () =>
            {
                await Task.Delay(TimeSpan.FromSeconds(EvictionDelaySeconds));
                MemoryCache.Remove(GetCacheKey(oldVersion, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Snapshot));
                MemoryCache.Remove(GetCacheKey(oldVersion, ObjectNameIndexOrDataEnum.Index, ObjectNameTypeEnum.Snapshot));
                for (int i = 0; i < oldBlockCount; i++)
                    MemoryCache.Remove(GetBlockCacheKey(oldVersion, ObjectNameTypeEnum.Snapshot, i));
                _logger.LogInformation("Evicted old snapshot v{Version} from cache after {Delay}s delay",
                    oldVersion, EvictionDelaySeconds);
            });
        }
    }

    public async Task StoreDelta(ulong deltaVersion, byte[] index, Stream data)
    {
        var indexCacheKey = GetCacheKey(deltaVersion, ObjectNameIndexOrDataEnum.Index, ObjectNameTypeEnum.Delta);
        var dataCacheKey = GetCacheKey(deltaVersion, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Delta);
        try
        {
            SetCache(indexCacheKey, index);
            if (data.Length <= _cacheConfig.Value.CacheSizeMB * MB_BYTES && data.Length <= int.MaxValue)
            {
                var byteArray = new byte[data.Length];
                _ = await data.ReadAsync(byteArray, 0, (int)data.Length);
                var bytesAsSpan = new ReadOnlyMemory<byte>(byteArray);
                SetCache(dataCacheKey, bytesAsSpan);
                await _storageProvider.StoreDelta(deltaVersion, index, byteArray);
            }
            else
            {
                await _storageProvider.StoreDelta(deltaVersion, index, data);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Something went wrong while storing delta data cache!");
            MemoryCache.Remove(indexCacheKey);
            MemoryCache.Remove(dataCacheKey);
            throw;
        }
    }

    public async Task StoreProcessedVersion(string who, ulong version)
    {
        await _storageProvider.StoreProcessedVersion(who, version);
    }

    public Task<ulong> GetProcessedVersion(string who)
    {
        return _storageProvider.GetProcessedVersion(who);
    }

    public Task<ulong> GetLowestProcessedVersion()
    {
        return _storageProvider.GetLowestProcessedVersion();
    }
}
