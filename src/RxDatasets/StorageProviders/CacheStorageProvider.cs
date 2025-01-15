using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProviders.ReadOnly;

namespace RxDatasets.StorageProviders;

public class CacheStorageProvider<T> : ReadOnlyCacheStorageProvider<T>, IStorageProvider<T>
{
    private const int MB_BYTES = 1024 * 1024;

    private readonly IOptions<DtoStorageCacheConfig> _cacheConfig;
    private readonly IStorageProvider<T> _storageProvider;
    private readonly ILogger<CacheStorageProvider<T>> _logger;

    public CacheStorageProvider(IStorageProvider<T> storageProvider,
        IOptions<DtoStorageCacheConfig> cacheConfig,
        ILogger<CacheStorageProvider<T>> logger,
        INotificationsHub<T>? notificationsHub,
        IReadOnlyStorageProvider<T> readOnlyStorageProvider) : base(readOnlyStorageProvider, cacheConfig, notificationsHub, logger)
    {
        _storageProvider = storageProvider;
        _logger = logger;
        _cacheConfig = cacheConfig;
        StartAsync(CancellationToken.None);
    }

    private string GetCacheKey(ulong version, ObjectNameIndexOrDataEnum indexData, ObjectNameTypeEnum deltaOrSnapshot) => $"{deltaOrSnapshot}_{indexData}_{version}";

    public async Task StoreSnapshot(ulong snapshotVersion, byte[] index, byte[] data)
    {
        var cacheKey = GetCacheKey(snapshotVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT);
        try
        { 
            var bytesAsSpan = new ReadOnlyMemory<byte>(data);
            SetCache(cacheKey, bytesAsSpan);
            await _storageProvider.StoreSnapshot(snapshotVersion, index, data);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Something went wrong while storing snapshot data!");
            MemoryCache.Remove(cacheKey);
            throw;
        }
    }

    public async Task StoreDelta(ulong deltaVersion, byte[] index, byte[] data)
    {
        var cacheKey = GetCacheKey(deltaVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.DELTA);
       
        try
        {
            SetCache(cacheKey, new ReadOnlyMemory<byte>(data));

            await _storageProvider.StoreDelta(deltaVersion, index, data);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Something went wrong while storing delta data cache!");
            MemoryCache.Remove(cacheKey);
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
        var cacheKey = GetCacheKey(snapshotVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT);
        try
        {
            if (data.Length <= _cacheConfig.Value.CacheSizeMB * MB_BYTES)
            {
                var byteArray = new byte[data.Length];
                _ = await data.ReadAsync(byteArray, 0, (int)data.Length);
                var bytesAsSpan = new ReadOnlyMemory<byte>(byteArray);
                SetCache(cacheKey, bytesAsSpan);
                await _storageProvider.StoreSnapshot(snapshotVersion, index, byteArray);
            }
            else
            {
                await _storageProvider.StoreSnapshot(snapshotVersion, index, data);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Something went wrong while storing delta data cache!");
            MemoryCache.Remove(cacheKey);
            throw;
        }
    }

    public async Task StoreDelta(ulong deltaVersion, byte[] index, Stream data)
    {
        var cacheKey = GetCacheKey(deltaVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.DELTA);
        try
        {
            if (data.Length <= _cacheConfig.Value.CacheSizeMB * MB_BYTES)
            {
                var byteArray = new byte[data.Length];
                _ = await data.ReadAsync(byteArray, 0, (int)data.Length);
                var bytesAsSpan = new ReadOnlyMemory<byte>(byteArray);
                SetCache(cacheKey, bytesAsSpan);
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
            MemoryCache.Remove(cacheKey);
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
