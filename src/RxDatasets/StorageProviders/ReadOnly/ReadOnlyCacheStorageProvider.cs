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
    private const long MB_BYTES = 1024 * 1024;
    protected const int BlockSize = 1024 * 1024 * 1024; // 1 GB blocks for >2 GB snapshot data
    protected readonly IMemoryCache MemoryCache = new MemoryCache(new MemoryCacheOptions()
    {
        SizeLimit = (long)cacheConfig.Value.CacheSizeMB * MB_BYTES
    });
    protected ulong _currentlyCachedSnapshot;
    protected int _currentlyCachedBlockCount;
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

        // Evict block-based cache entries
        for (int i = 0; i < _currentlyCachedBlockCount; i++)
            MemoryCache.Remove(GetBlockCacheKey(oldSnapshotVersion, ObjectNameTypeEnum.Snapshot, i));
        _currentlyCachedBlockCount = 0;
    }

    private async Task CacheSnapshot(ulong snapshotVersion)
    {
        var dataLength = await GetSnapshotDataContentLength(snapshotVersion);
        if (dataLength > (long)cacheConfig.Value.CacheSizeMB * MB_BYTES)
            return;

        if (dataLength <= int.MaxValue)
        {
            // Small enough for a single byte[] — use the simple path
            _ = await ReadSnapshotData(snapshotVersion); // triggers cache set via GetOrCreateCoalesced
            return;
        }

        // >2 GB: stream-read into 1 GB blocks to stay within byte[] limits.
        // Uses the Stream-based overload which supports streaming decompression (lz4frame).
        var blocks = new List<ReadOnlyMemory<byte>>();
        await _readOnlyStorageProvider.ReadSnapshotData(snapshotVersion, stream =>
        {
            var buffer = new byte[BlockSize];
            while (true)
            {
                int totalRead = 0;
                while (totalRead < BlockSize)
                {
                    int read = stream.Read(buffer, totalRead, BlockSize - totalRead);
                    if (read == 0) break;
                    totalRead += read;
                }
                if (totalRead == 0) break;

                var block = new byte[totalRead];
                Buffer.BlockCopy(buffer, 0, block, 0, totalRead);
                blocks.Add(block);
            }
        });

        for (int i = 0; i < blocks.Count; i++)
        {
            var cacheKey = GetBlockCacheKey(snapshotVersion, ObjectNameTypeEnum.Snapshot, i);
            SetCache(cacheKey, blocks[i]);
        }
        _currentlyCachedBlockCount = blocks.Count;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _subscriptions.Dispose();
        return Task.CompletedTask;
    }
    protected string GetCacheKey(ulong version, ObjectNameIndexOrDataEnum indexData, ObjectNameTypeEnum deltaOrSnapshot) => $"{deltaOrSnapshot}_{indexData}_{version}";
    private string GetPositionedCacheKey(ulong version, ObjectNameIndexOrDataEnum indexData, ObjectNameTypeEnum deltaOrSnapshot, long position, int length) => $"{deltaOrSnapshot}_{indexData}_{version}_pos{position}_len{length}";
    protected static string GetBlockCacheKey(ulong version, ObjectNameTypeEnum type, int blockIndex) => $"{type}_Data_{version}_blk{blockIndex}";

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
            var cacheKeySpecificRange = GetPositionedCacheKey(version, ObjectNameIndexOrDataEnum.Data, type, position, length);

            if (MemoryCache.TryGetValue(cacheKeySpecificRange, out ReadOnlyMemory<byte> data))
            {
                return data;
            }

            var cacheKeyWhole = GetCacheKey(version, ObjectNameIndexOrDataEnum.Data, type);
            if (MemoryCache.TryGetValue(cacheKeyWhole, out data))
            {
                return data.Slice((int)position, length);
            }

            // Block-based cache: check if the requested range falls within cached block(s)
            int blockIndex = (int)(position / BlockSize);
            var blockCacheKey = GetBlockCacheKey(version, type, blockIndex);
            if (MemoryCache.TryGetValue(blockCacheKey, out data))
            {
                int offsetInBlock = (int)(position - (long)blockIndex * BlockSize);
                if (offsetInBlock + length <= data.Length)
                {
                    return data.Slice(offsetInBlock, length);
                }

                // Read spans a block boundary — copy tail of this block + head of next
                int fromFirst = data.Length - offsetInBlock;
                var nextBlockKey = GetBlockCacheKey(version, type, blockIndex + 1);
                if (MemoryCache.TryGetValue(nextBlockKey, out ReadOnlyMemory<byte> nextData))
                {
                    var result = new byte[length];
                    data.Slice(offsetInBlock, fromFirst).CopyTo(result);
                    nextData.Slice(0, length - fromFirst).CopyTo(result.AsMemory(fromFirst));
                    return result;
                }
            }
        }

        var dataFromSources = await _readOnlyStorageProvider.ReadDataFromSources(sources, length);

        if (dataFromSources.Length > length)
        {
            // Provider returned the full blob (partial read not efficient) — cache it whole
            // so subsequent reads for other entities in the same version hit the cache.
            sources.ForEach(s =>
            {
                var cacheKeyWhole = GetCacheKey(s.version, ObjectNameIndexOrDataEnum.Data, s.type);
                SetCache(cacheKeyWhole, dataFromSources);
            });
            return dataFromSources.Slice((int)sources[0].position, length);
        }

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

        if (MemoryCache.TryGetValue(cacheKeyWholeSnapshot, out ReadOnlyMemory<byte> data))
        {
            using var memoryStream = new MemoryStream(data.ToArray());
            callback(memoryStream);
            return Task.CompletedTask;
        }

        // Check block-based cache for >2 GB snapshots
        if (_currentlyCachedSnapshot == version && _currentlyCachedBlockCount > 0)
        {
            var blocks = new List<ReadOnlyMemory<byte>>(_currentlyCachedBlockCount);
            bool allBlocksFound = true;
            for (int i = 0; i < _currentlyCachedBlockCount; i++)
            {
                var blockKey = GetBlockCacheKey(version, ObjectNameTypeEnum.Snapshot, i);
                if (!MemoryCache.TryGetValue(blockKey, out ReadOnlyMemory<byte> block))
                {
                    allBlocksFound = false;
                    break;
                }
                blocks.Add(block);
            }

            if (allBlocksFound)
            {
                using var stream = new ConcatenatingBlockStream(blocks);
                callback(stream);
                return Task.CompletedTask;
            }
        }

        return _readOnlyStorageProvider.ReadSnapshotData(version, callback);
    }

    /// <summary>
    /// Read-only, forward-only stream that reads sequentially through a list of memory blocks.
    /// Used to serve block-cached >2 GB snapshot data as a single stream.
    /// </summary>
    private sealed class ConcatenatingBlockStream(List<ReadOnlyMemory<byte>> blocks) : Stream
    {
        private int _currentBlock;
        private int _positionInBlock;

        public override int Read(byte[] buffer, int offset, int count)
        {
            int totalRead = 0;
            while (count > 0 && _currentBlock < blocks.Count)
            {
                var block = blocks[_currentBlock];
                int available = block.Length - _positionInBlock;
                int toCopy = Math.Min(available, count);
                block.Span.Slice(_positionInBlock, toCopy).CopyTo(buffer.AsSpan(offset, toCopy));
                offset += toCopy;
                count -= toCopy;
                totalRead += toCopy;
                _positionInBlock += toCopy;
                if (_positionInBlock >= block.Length)
                {
                    _currentBlock++;
                    _positionInBlock = 0;
                }
            }
            return totalRead;
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => blocks.Sum(b => (long)b.Length);
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
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
