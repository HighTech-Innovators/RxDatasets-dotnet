using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;

namespace RxDatasets.StorageProviders;

public class CacheStorageProvider<T>(IStorageProvider<T> storageProvider, IOptions<DtoStorageCacheConfig> cacheConfig, ILogger<CacheStorageProvider<T>> logger) : IStorageProvider<T>, IHostedService
{
    private const int MB_BYTES = 1024 * 1024;
    private readonly IMemoryCache _memoryCache = new MemoryCache(new MemoryCacheOptions()
    {
        SizeLimit = cacheConfig.Value.CacheSizeMB * MB_BYTES
    });
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var (_, _, snapshots) = await ListVersions();
        foreach (var version in snapshots.Select(s => s.version).OrderDescending())
        {
            try
            {
                await TryCacheSnapshot(version);
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

    private async Task TryCacheSnapshot(ulong snapshotVersion)
    {
        var index = await ReadSnapshotIndex(snapshotVersion);
        var mutableSnapshotIndex = SnapshotIndex.Parser.ParseFrom(index.Span);
        var totalLength = mutableSnapshotIndex.Entities.Values.Sum(entity => entity.Length);
        if (totalLength <= cacheConfig.Value.CacheSizeMB * MB_BYTES)
        {
            _ = await ReadSnapshotData(snapshotVersion);
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
    private string GetCacheKey(ulong version, ObjectNameIndexOrDataEnum indexData, ObjectNameTypeEnum deltaOrSnapshot) => $"{deltaOrSnapshot}_{indexData}_{version}";
    private string GetPositionedCacheKey(ulong version, ObjectNameIndexOrDataEnum indexData, ObjectNameTypeEnum deltaOrSnapshot, long position, int length) => $"{deltaOrSnapshot}_{indexData}_{version}_pos{position}_len{length}";

    public async Task StoreSnapshot(ulong snapshotVersion, byte[] index, byte[] data)
    {
        var cacheKey = GetCacheKey(snapshotVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT);
        try
        { 
            var bytesAsSpan = new ReadOnlyMemory<byte>(data);
            SetCache(cacheKey, bytesAsSpan);
            await storageProvider.StoreSnapshot(snapshotVersion, index, data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Something went wrong while storing snapshot data!");
            _memoryCache.Remove(cacheKey);
            throw;
        }
    }

    public async Task StoreDelta(ulong deltaVersion, byte[] index, byte[] data)
    {
        var cacheKey = GetCacheKey(deltaVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.DELTA);
       
        try
        {
            SetCache(cacheKey, new ReadOnlyMemory<byte>(data));

            await storageProvider.StoreDelta(deltaVersion, index, data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Something went wrong while storing delta data cache!");
            _memoryCache.Remove(cacheKey);
            throw;
        }
    }

    public async Task<ReadOnlyMemory<byte>> ReadDeltaIndex(ulong version)
    {
        var cache = GetCacheKey(version, ObjectNameIndexOrDataEnum.INDEX, ObjectNameTypeEnum.DELTA);
        var value = await _memoryCache.GetOrCreateAsync(cache, async entry =>
        {
            var data =await storageProvider.ReadDeltaIndex(version);
            entry.Size = data.Length;
            return data;
        });
        return value;
    }


    private void SetCache(string cacheKey, ReadOnlyMemory<byte> data)
    {
        _memoryCache.Set(cacheKey, data, new MemoryCacheEntryOptions()
        {
            Size = data.Length,
            Priority = CacheItemPriority.Low,
        });
    }

    public async Task<ReadOnlyMemory<byte>> ReadSnapshotIndex(ulong version)
    {
        var cache = GetCacheKey(version, ObjectNameIndexOrDataEnum.INDEX, ObjectNameTypeEnum.SNAPSHOT);
        var value = await _memoryCache.GetOrCreateAsync(cache, async entry =>
        {
            var data = await storageProvider.ReadSnapshotIndex(version);
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
            
            if (_memoryCache.TryGetValue(cacheKeySpecificRange, out ReadOnlyMemory<byte> data))
            {
                return data;
            }

            if (_memoryCache.TryGetValue(cacheKeyWhole, out data))
            {
                return data.Slice((int)position, length);
            }
        }

        var dataFromSources = await storageProvider.ReadDataFromSources(sources, length);

        sources.ForEach(s =>
        {
            var cacheKeySpecificRange = GetPositionedCacheKey(s.version, ObjectNameIndexOrDataEnum.DATA, s.type, s.position, length);
            SetCache(cacheKeySpecificRange, dataFromSources);
        });

        return dataFromSources;
    }

    public Task ReadSnapshotData(ulong version, Action<Stream> callback)
    {

        var cacheKeyWholeSnapshot = GetCacheKey(version, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT);
        
        if (!_memoryCache.TryGetValue(cacheKeyWholeSnapshot, out ReadOnlyMemory<byte> data))
            return storageProvider.ReadSnapshotData(version, callback);
       
        var memoryStream = new MemoryStream(data.ToArray());
        using (memoryStream)
        {
            callback(memoryStream);
            return Task.CompletedTask;
        }

    }

    public async Task<ReadOnlyMemory<byte>> ReadSnapshotData(ulong snapshotVersion)
    {
        var cacheKey = GetCacheKey(snapshotVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT);
        return await _memoryCache.GetOrCreateAsync(cacheKey, async entry =>
        {
            var readSnapshotData = await storageProvider.ReadSnapshotData(snapshotVersion);
            entry.Size = readSnapshotData.Length;
            return readSnapshotData;
        });
    }

    public Task DeleteOlderThan(ulong version, bool keepAllDeltas = false)
    {
        return storageProvider.DeleteOlderThan(version, keepAllDeltas);
    }

    public async Task StoreSnapshot(ulong snapshotVersion, byte[] index, Stream data)
    {
        var cacheKey = GetCacheKey(snapshotVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT);
        try
        {
            if (data.Length <= cacheConfig.Value.CacheSizeMB * MB_BYTES)
            {
                var byteArray = new byte[data.Length];
                _ = await data.ReadAsync(byteArray, 0, (int)data.Length);
                var bytesAsSpan = new ReadOnlyMemory<byte>(byteArray);
                SetCache(cacheKey, bytesAsSpan);
                await storageProvider.StoreSnapshot(snapshotVersion, index, byteArray);
            }
            else
            {
                await storageProvider.StoreSnapshot(snapshotVersion, index, data);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Something went wrong while storing delta data cache!");
            _memoryCache.Remove(cacheKey);
            throw;
        }
    }

    public Task<(ulong oldestDelta, ulong newestDelta, List<(ulong version, DateTimeOffset DateTime)> snapshots)> ListVersions()
    {
        return storageProvider.ListVersions();
    }

    public async Task StoreDelta(ulong deltaVersion, byte[] index, Stream data)
    {
        var cacheKey = GetCacheKey(deltaVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.DELTA);
        try
        {
            if (data.Length <= cacheConfig.Value.CacheSizeMB * MB_BYTES)
            {
                var byteArray = new byte[data.Length];
                _ = await data.ReadAsync(byteArray, 0, (int)data.Length);
                var bytesAsSpan = new ReadOnlyMemory<byte>(byteArray);
                SetCache(cacheKey, bytesAsSpan);
                await storageProvider.StoreDelta(deltaVersion, index, byteArray);
            }
            else
            {
                await storageProvider.StoreDelta(deltaVersion, index, data);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Something went wrong while storing delta data cache!");
            _memoryCache.Remove(cacheKey);
            throw;
        }
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
}
