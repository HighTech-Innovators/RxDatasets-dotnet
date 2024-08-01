using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.Models;

namespace RxDatasets.StorageProviders;

public class CacheStorageProvider<T>(IStorageProvider<T> storageProvider, IOptions<DtoStorageCacheConfig> cacheConfig, ILogger<CacheStorageProvider<T>> logger) : IStorageProvider<T>
{
    private const int MB_BYTES = 1024 * 1024;
    private readonly IMemoryCache _memoryCache = new MemoryCache(new MemoryCacheOptions()
    {
        SizeLimit = cacheConfig.Value.CacheSizeMB * MB_BYTES
    });

    private string GetCacheKey(ulong version, ObjectNameIndexOrDataEnum indexData, ObjectNameTypeEnum deltaOrSnapshot) => $"{deltaOrSnapshot}_{indexData}_{version}";
    private string GetPositionedCacheKey(ulong version, ObjectNameIndexOrDataEnum indexData, ObjectNameTypeEnum deltaOrSnapshot, long position, int length) => $"{deltaOrSnapshot}_{indexData}_{version}_pos{position}_len{length}";

    public async Task StoreDelta(ulong deltaVersion, byte[] index, byte[] data)
    {
        await storageProvider.StoreDelta(deltaVersion, index, data);

        string cache = GetCacheKey(deltaVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.DELTA);
        _memoryCache.Set(cache, new ReadOnlyMemory<byte>(data), new MemoryCacheEntryOptions()
        {
            Size = data.Length,
            Priority = CacheItemPriority.Low
        });
    }

    public Task<ReadOnlyMemory<byte>> ReadDeltaIndex(ulong version)
    {
        return storageProvider.ReadDeltaIndex(version);
    }

    public async Task<ReadOnlyMemory<byte>> ReadDeltaData(ulong version, long position, int length)
    {
        string cache = GetCacheKey(version, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.DELTA);
        string cacheWithParams = GetPositionedCacheKey(version, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.DELTA, position, length);

        if (_memoryCache.TryGetValue(cacheWithParams, out ReadOnlyMemory<byte>? cachedStream) && cachedStream != null)
        {
            logger.LogInformation($"Cache hit for delta data {version}");
            return cachedStream.Value;
        }

        if (_memoryCache.TryGetValue(cache, out cachedStream) && cachedStream != null)
        {
            logger.LogInformation($"Cache hit for delta data {version}");
            var result = cachedStream.Value.Slice((int)position, length);

            return result;
        }

        ReadOnlyMemory<byte> data = await storageProvider.ReadDeltaData(version, position, length);

        _memoryCache.Set(cacheWithParams, data, new MemoryCacheEntryOptions()
        {
            Size = data.Length,
            Priority = CacheItemPriority.Low,
        });

        return data;
    }

    public async Task<ReadOnlyMemory<byte>> ReadSnapshotIndex(ulong version)
    {
        string cache = GetCacheKey(version, ObjectNameIndexOrDataEnum.INDEX, ObjectNameTypeEnum.SNAPSHOT);
        var value = await _memoryCache.GetOrCreateAsync(cache, async entry =>
        {
            ReadOnlyMemory<byte> data = await storageProvider.ReadSnapshotIndex(version);
            entry.Size= data.Length;
            return data;
        });
        return value;
    }

    public async Task<ReadOnlyMemory<byte>> ReadSnapshotData(ulong version, long position, int length)
    {
        string cache = GetPositionedCacheKey(version, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT, position, length);
        
        var value = await _memoryCache.GetOrCreateAsync(cache, async entry =>
        {
            return await GetCustomSnapshotData(version, position, length, entry);
        });
        return value;
    }

    private async Task<ReadOnlyMemory<byte>> GetCustomSnapshotData( ulong version, long position, int length, ICacheEntry entry)
    {
        ReadOnlyMemory<byte> data = await storageProvider.ReadSnapshotData(version, position, length);
        entry.Size = data.Length;
        entry.Priority = CacheItemPriority.Low;
        return data;
    }

    public Task ReadSnapshotData(ulong version, Action<Stream> callback)
    {
        return storageProvider.ReadSnapshotData(version, callback);
    }

    public Task DeleteOlderThan(ulong version)
    {
        return storageProvider.DeleteOlderThan(version);
    }

    public Task StoreSnapshot(ulong snapshotVersion, byte[] index, FileStream data)
    {
        return storageProvider.StoreSnapshot(snapshotVersion, index, data);
    }

    public Task<(ulong oldestDelta, ulong newestDelta, List<(ulong version, DateTimeOffset DateTime)> snapshots)> ListVersions()
    {
        return storageProvider.ListVersions();
    }

    public async Task StoreDelta(ulong deltaVersion, byte[] index, FileStream data)
    {
        await storageProvider.StoreDelta(deltaVersion, index, data);

        string cache = GetCacheKey(deltaVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.DELTA);
        _memoryCache.Set(cache, data, new MemoryCacheEntryOptions()
        {
            Size = data.Length,
            Priority = CacheItemPriority.Low
        });
    }

    public async Task StoreProcessedVersion(string who, ulong version)
    {
        await storageProvider.StoreProcessedVersion(who, version);
   }

    public Task<ulong> GetProcessedVersion(string who)
    {
        return storageProvider.GetProcessedVersion(who);
    }

    public Task<ulong> GetLowestProcessedVersion()
    {
        return storageProvider.GetLowestProcessedVersion();
    }

    public Task<ulong> GetLastPresentVersion()
    {
        return storageProvider.GetLastPresentVersion();
    }
}
