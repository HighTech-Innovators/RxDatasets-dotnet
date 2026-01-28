using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using RxDatasets.Abstractions;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Notifications;

namespace RxDatasets.Tests;

/// <summary>
/// Simple in-memory implementation of IStorageProvider for testing.
/// Stores all snapshots, deltas, and processed versions in memory dictionaries.
/// No persistence, no caching complexity - purely for E2E testing with real storage semantics.
/// </summary>
public class InMemoryStorageProvider<T> : IStorageProvider<T>
{
    private readonly ILogger<InMemoryStorageProvider<T>> _logger;
    private readonly ConcurrentDictionary<string, ReadOnlyMemory<byte>> _storage = new();
    private readonly ConcurrentDictionary<string, ulong> _processedVersions = new();
    private readonly INotificationsHub<T>? _notificationsHub;

    public InMemoryStorageProvider(ILogger<InMemoryStorageProvider<T>> logger, INotificationsHub<T>? notificationsHub = null)
    {
        _logger = logger;
        _notificationsHub = notificationsHub;
        
        // Broadcast readiness
        _ = _notificationsHub?.Notify(new StorageProviderReadinessNotifcation { ServiceStatus = ServiceStatus.Ready });
    }

    #region IStorageProviderReader Implementation

    public Task<ReadOnlyMemory<byte>> ReadDeltaIndex(ulong version)
    {
        var key = $"delta:{version}:index";
        if (_storage.TryGetValue(key, out var data))
        {
            return Task.FromResult(data);
        }
        throw new FileNotFoundException($"Delta index not found: v{version}");
    }

    public Task<ReadOnlyMemory<byte>> ReadSnapshotIndex(ulong version)
    {
        var key = $"snapshot:{version}:index";
        if (_storage.TryGetValue(key, out var data))
        {
            return Task.FromResult(data);
        }
        throw new FileNotFoundException($"Snapshot index not found: v{version}");
    }

    public Task ReadSnapshotData(ulong snapshotVersion, Action<Stream> callback)
    {
        var key = $"snapshot:{snapshotVersion}:data";
        if (_storage.TryGetValue(key, out var data))
        {
            using var stream = new MemoryStream(data.ToArray());
            callback(stream);
            return Task.CompletedTask;
        }
        throw new FileNotFoundException($"Snapshot data not found: v{snapshotVersion}");
    }

    public async Task ReadSnapshotDataAsync(ulong snapshotVersion, Func<Stream, Task> asyncCallback)
    {
        var key = $"snapshot:{snapshotVersion}:data";
        if (!_storage.TryGetValue(key, out var data))
        {
            throw new FileNotFoundException($"Snapshot data not found: v{snapshotVersion}");
        }
        using var stream = new MemoryStream(data.ToArray());
        await asyncCallback(stream);
    }

    public Task<ReadOnlyMemory<byte>> ReadSnapshotData(ulong snapshotVersion)
    {
        var key = $"snapshot:{snapshotVersion}:data";
        if (_storage.TryGetValue(key, out var data))
        {
            return Task.FromResult(data);
        }
        throw new FileNotFoundException($"Snapshot data not found: v{snapshotVersion}");
    }

    public Task<(ulong OldestDelta, ulong NewestDelta, List<SnapshotType> Snapshots)> ListVersions()
    {
        var deltas = _storage.Keys
            .Where(k => k.StartsWith("delta:", StringComparison.Ordinal))
            .Select(k => ulong.Parse(k.Split(':')[1]))
            .Distinct()
            .ToList();

        var snapshots = _storage.Keys
            .Where(k => k.StartsWith("snapshot:", StringComparison.Ordinal))
            .Select(k => ulong.Parse(k.Split(':')[1]))
            .Distinct()
            .Select(v => new SnapshotType(v, null))
            .ToList();

        var oldestDelta = deltas.Count > 0 ? deltas.Min() : 0;
        var newestDelta = deltas.Count > 0 ? deltas.Max() : 0;

        return Task.FromResult((oldestDelta, newestDelta, snapshots));
    }

    public Task<ReadOnlyMemory<byte>> ReadDataFromSources(
        List<(ObjectNameTypeEnum type, ulong version, long position)> sources, int length)
    {
        var buffer = new byte[length];
        var position = 0;

        foreach (var (type, version, offset) in sources)
        {
            var key = type switch
            {
                ObjectNameTypeEnum.Delta => $"delta:{version}:data",
                ObjectNameTypeEnum.Snapshot => $"snapshot:{version}:data",
                _ => throw new InvalidOperationException($"Unknown type: {type}")
            };

            if (!_storage.TryGetValue(key, out var data))
            {
                throw new FileNotFoundException($"Data not found: {key}");
            }

            var dataArray = data.ToArray();
            var toCopy = Math.Min(length - position, dataArray.Length - (int)offset);
            Array.Copy(dataArray, offset, buffer, position, toCopy);
            position += toCopy;
        }

        return Task.FromResult(new ReadOnlyMemory<byte>(buffer, 0, position));
    }

    public Task<long> GetSnapshotDataContentLength(ulong snapshotVersion)
    {
        var key = $"snapshot:{snapshotVersion}:data";
        if (_storage.TryGetValue(key, out var data))
        {
            return Task.FromResult((long)data.Length);
        }
        throw new FileNotFoundException($"Snapshot data not found: v{snapshotVersion}");
    }

    #endregion

    #region IStorageProviderWriter Implementation

    public Task StoreSnapshot(ulong snapshotVersion, byte[] index, byte[] data)
    {
        _storage[$"snapshot:{snapshotVersion}:index"] = new ReadOnlyMemory<byte>(index);
        _storage[$"snapshot:{snapshotVersion}:data"] = new ReadOnlyMemory<byte>(data);
        _logger.LogInformation($"Stored snapshot v{snapshotVersion}");
        return Task.CompletedTask;
    }

    public async Task StoreSnapshot(ulong snapshotVersion, byte[] index, Stream data)
    {
        using var ms = new MemoryStream();
        await data.CopyToAsync(ms);
        await StoreSnapshot(snapshotVersion, index, ms.ToArray());
    }

    public Task StoreDelta(ulong deltaVersion, byte[] index, byte[] data)
    {
        _storage[$"delta:{deltaVersion}:index"] = new ReadOnlyMemory<byte>(index);
        _storage[$"delta:{deltaVersion}:data"] = new ReadOnlyMemory<byte>(data);
        _logger.LogInformation($"Stored delta v{deltaVersion}");
        return Task.CompletedTask;
    }

    public async Task StoreDelta(ulong deltaVersion, byte[] index, Stream data)
    {
        using var ms = new MemoryStream();
        await data.CopyToAsync(ms);
        await StoreDelta(deltaVersion, index, ms.ToArray());
    }

    public Task DeleteOlderThan(ulong snapshotVersion, bool keepAllDeltas = false)
    {
        var keysToDelete = _storage.Keys
            .Where(k =>
            {
                if (k.StartsWith("snapshot:", StringComparison.Ordinal))
                {
                    var version = ulong.Parse(k.Split(':')[1]);
                    return version < snapshotVersion;
                }
                if (k.StartsWith("delta:", StringComparison.Ordinal) && !keepAllDeltas)
                {
                    var version = ulong.Parse(k.Split(':')[1]);
                    return version < snapshotVersion;
                }
                return false;
            })
            .ToList();

        foreach (var key in keysToDelete)
        {
            _storage.TryRemove(key, out _);
        }
        _logger.LogInformation($"Deleted versions older than {snapshotVersion}");
        return Task.CompletedTask;
    }

    public Task StoreProcessedVersion(string who, ulong version)
    {
        _processedVersions[who] = version;
        return Task.CompletedTask;
    }

    #endregion

    #region IStorageProvider (non-Reader/Writer) Implementation

    public Task<ulong> GetProcessedVersion(string who)
    {
        if (_processedVersions.TryGetValue(who, out var version))
        {
            return Task.FromResult(version);
        }
        return Task.FromResult(0UL);
    }

    public Task<ulong> GetLowestProcessedVersion()
    {
        if (_processedVersions.IsEmpty)
        {
            return Task.FromResult(0UL);
        }
        return Task.FromResult(_processedVersions.Values.Min());
    }

    #endregion
}
