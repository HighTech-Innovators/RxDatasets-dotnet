using System.Collections.Concurrent;
using System.IO;
using Microsoft.Extensions.Logging;
using RxDatasets.Abstractions;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Notifications;

namespace RxDatasets.StorageProviders;

public class InMemoryStorageProvider<T> : IStorageProvider<T>
{
    private readonly ConcurrentDictionary<ulong, (ReadOnlyMemory<byte> index, ReadOnlyMemory<byte> data)> _deltas = new();
    private readonly ConcurrentDictionary<ulong, (ReadOnlyMemory<byte> index, ReadOnlyMemory<byte> data, DateTimeOffset creationTime)> _snapshots = new();
    private readonly ConcurrentDictionary<string, ulong> _processedVersions = new();
    private readonly ILogger<InMemoryStorageProvider<T>> _logger;
    private readonly INotificationsHub<T> _notificationsHub;

    public InMemoryStorageProvider(ILogger<InMemoryStorageProvider<T>> Logger, INotificationsHub<T> notificationsHub)
    {
        _logger = Logger;
        _notificationsHub = notificationsHub;
        _notificationsHub.Notify(new StorageProviderReadinessNotifcation(ServiceStatus.Ready)).Wait();

    }

    public Task<ReadOnlyMemory<byte>> ReadSnapshotData(ulong snapshotVersion)
    {
        if (_deltas.TryGetValue(snapshotVersion, out var data))
        {
            return Task.FromResult(data.data);
        }

        throw new FileNotFoundException($"snapshot does not exist {snapshotVersion}");
    }

    public Task DeleteOlderThan(ulong version, bool keepAllDeltas = false)
    {
        if (!keepAllDeltas)
        {
            foreach (var key in _deltas.Keys.Where(k => k <= version))
            {
                _deltas.Remove(key, out _);
            }
        }
    

        foreach (var key in _snapshots.Keys.Where(k => k < version))
        {
            _snapshots.Remove(key, out _);
        }

        return Task.CompletedTask;
    }

    public async Task StoreDelta(ulong deltaVersion, byte[] index, Stream stream)
    {
        byte[] dataBytes;
        await using (stream)
        {
            dataBytes = new byte[stream.Length];  // Initialize the byte array to the length of the file
            stream.Read(dataBytes);
        }

        _deltas[deltaVersion] = (index, dataBytes);

        await _notificationsHub.Notify(new DatasetUpdatedNotification(Version: deltaVersion));
    }

    public Task<(ulong oldestDelta, ulong newestDelta, List<(ulong version, DateTimeOffset DateTime)> snapshots)> ListVersions()
    {
        if (_deltas.IsEmpty)
            return Task.FromResult((0ul, 0ul, new List<(ulong, DateTimeOffset)>()));

        var oldestDelta = _deltas.Keys.Min();
        var newestDelta = _deltas.Keys.Max();
        var snapshots = _snapshots.Select(x => (x.Key, x.Value.creationTime)).GroupBy((g) => g.Key).Select((s) => s.First()).ToList();

        return Task.FromResult((oldestDelta, newestDelta, snapshots));
    }

    public Task<ReadOnlyMemory<byte>> ReadDeltaIndex(ulong version)
    {
        if (_deltas.TryGetValue(version, out var delta))
        {
            return Task.FromResult(delta.index);
        }

        throw new FileNotFoundException($"Delta does not exist {version}");
    }

    public Task ReadSnapshotData(ulong version, Action<Stream> callback)
    {
        if (_snapshots.TryGetValue(version, out var data))
        {
            using (var stream = new MemoryStream(data.data.ToArray()))
            {
                callback(stream);
            }
            return Task.CompletedTask;
        }

        throw new FileNotFoundException($"Snapshot does not exist {version}");
    }

    public Task<ReadOnlyMemory<byte>> ReadSnapshotIndex(ulong version)
    {
        if (_snapshots.TryGetValue(version, out var snapshot))
        {
            return Task.FromResult(snapshot.index);
        }

        throw new FileNotFoundException($"Snapshot does not exist {version}");
    }

    public async Task StoreSnapshot(ulong snapshotVersion, byte[] index, byte[] data)
    {
        _snapshots[snapshotVersion] = (index, data, DateTimeOffset.UtcNow);
        await _notificationsHub.Notify(new SnapshotCreatedNotification(Version: snapshotVersion));
    }

    public async Task StoreDelta(ulong deltaVersion, byte[] index, byte[] data)
    {
        _deltas[deltaVersion] = (index, data);
        await _notificationsHub.Notify(new DatasetUpdatedNotification(Version: deltaVersion));
    }

    public async Task StoreSnapshot(ulong snapshotVersion, byte[] index, Stream data)
    {
        data.Position = 0;
        MemoryStream memoryStream = new MemoryStream();
        await data.CopyToAsync(memoryStream);
        _snapshots[snapshotVersion] = (index, memoryStream.ToArray(), DateTimeOffset.UtcNow);
        await _notificationsHub.Notify(new SnapshotCreatedNotification(Version: snapshotVersion));
    }

    public async Task StoreProcessedVersion(string who, ulong version)
    {
        if (_processedVersions.ContainsKey(who))
        {
            _processedVersions[who] = version;
        }
        else
        {
            _processedVersions.TryAdd(who, version);
        };

        await _notificationsHub.Notify(new VersionProcessedNotification(NewVersion: version, Who: who));
    }

    public Task<ReadOnlyMemory<byte>> ReadDataFromSources(List<(ObjectNameTypeEnum type, ulong version, long position)> sources, int length)
    {
        if (sources.Count == 0)
        {
            throw new ArgumentException($"at least one source must be filled");
        }

        foreach (var (type, version, position) in sources)
        {
            switch (type)
            {
                case ObjectNameTypeEnum.SNAPSHOT when _snapshots.TryGetValue(version, out var snapshot):
                    return Task.FromResult(snapshot.data.Slice((int)position, length));
                case ObjectNameTypeEnum.DELTA when _deltas.TryGetValue(version, out var delta):
                    return Task.FromResult(delta.data.Slice((int)position, length));
            }
        }

        throw new ArgumentException($"No Source was read");
    }

    public Task<ulong> GetProcessedVersion(string who)
    {
        var versionFound = _processedVersions.TryGetValue(who, out var version);
        if (versionFound) {
            _logger.LogInformation($"we found version {versionFound} for {who}");
            return Task.FromResult(version); 
        }
        else return Task.FromResult(0ul);
    }

    public Task<ulong> GetLowestProcessedVersion()
    {
        return Task.FromResult(_processedVersions.Count > 0 ? _processedVersions.Min(v => v.Value) : 0ul);
    }

    public Task<long> GetSnapshotDataContentLength(ulong snapshotVersion)
    {
        return Task.FromResult((long)_snapshots[snapshotVersion].data.Length);
    }
}
