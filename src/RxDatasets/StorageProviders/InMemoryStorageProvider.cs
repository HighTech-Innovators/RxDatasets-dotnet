using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using RxDatasets.Abstractions;
using RxDatasets.DatasetUpdateNotifications;

namespace RxDatasets.StorageProviders;

public class InMemoryStorageProvider<T>(ILogger<InMemoryStorageProvider<T>> _logger, IDatasetUpdateNotifier<T> datasetUpdateNotifier) : IStorageProvider<T>
{
    private readonly ConcurrentDictionary<ulong, (ReadOnlyMemory<byte> index, ReadOnlyMemory<byte> data)> _deltas = new();
    private readonly ConcurrentDictionary<ulong, (ReadOnlyMemory<byte> index, ReadOnlyMemory<byte> data, DateTimeOffset creationTime)> _snapshots = new();
    private readonly ConcurrentDictionary<string, ulong> _processedVersions = new();

    public Task DeleteOlderThan(ulong version)
    {
        foreach (var key in _deltas.Keys.Where(k => k <= version))
        {
            _deltas.Remove(key, out _);
        }

        foreach (var key in _snapshots.Keys.Where(k => k < version))
        {
            _snapshots.Remove(key, out _);
        }

        return Task.CompletedTask;
    }

    public Task StoreDelta(ulong deltaVersion, byte[] index, FileStream stream)
    {
        byte[] dataBytes;
        using (stream)
        {
            dataBytes = new byte[stream.Length];  // Initialize the byte array to the length of the file
            stream.Read(dataBytes);
        }

        _deltas[deltaVersion] = (index, dataBytes);

        datasetUpdateNotifier.Notify(new DatasetUpdateNotification()
        {
            DtoName = typeof(T).Name,
            Version = deltaVersion,
            SnapshotVersion = _snapshots.Keys.Count > 0 ? _snapshots.Keys.Max() : 0ul
        });
        return Task.CompletedTask;
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


    public Task<ReadOnlyMemory<byte>> ReadDeltaData(ulong version, long position, int length)
    {
        if (_deltas.TryGetValue(version, out var data))
        {
            return Task.FromResult(data.data.Slice((int)position, length));
        }

        throw new FileNotFoundException($"Delta does not exist {version}");
    }

    public Task<ReadOnlyMemory<byte>> ReadDeltaIndex(ulong version)
    {
        if (_deltas.TryGetValue(version, out var delta))
        {
            return Task.FromResult(delta.index);
        }

        throw new FileNotFoundException($"Delta does not exist {version}");
    }

    public Task<ReadOnlyMemory<byte>> ReadSnapshotData(ulong version, long position, int length)
    {
        if (_snapshots.TryGetValue(version, out var data))
        {
            return Task.FromResult(data.data.Slice((int)position, length));
        }

        throw new FileNotFoundException($"Snapshot does not exist {version}");
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

    public Task StoreDelta(ulong deltaVersion, byte[] index, byte[] data)
    {
        _deltas[deltaVersion] = (index, data);
        datasetUpdateNotifier.Notify(new DatasetUpdateNotification()
        {
            DtoName = typeof(T).Name,
            Version = deltaVersion,
            SnapshotVersion = _snapshots.Keys.Count > 0 ? _snapshots.Keys.Max() : 0ul
        });
        return Task.CompletedTask;
    }

    public Task StoreSnapshot(ulong snapshotVersion, byte[] index, FileStream data)
    {
        data.Position = 0;
        MemoryStream memoryStream = new MemoryStream();
        data.CopyTo(memoryStream);
        _snapshots[snapshotVersion] = (index, memoryStream.ToArray(), DateTimeOffset.UtcNow);
        datasetUpdateNotifier.Notify(new DatasetUpdateNotification()
        {
            DtoName = typeof(T).Name,
            Version = _deltas.Keys.Count > 0 ? _deltas.Keys.Max() : 0ul,
            SnapshotVersion = snapshotVersion
        });
        return Task.CompletedTask;
    }

    public Task StoreProcessedVersion(string who, ulong version)
    {
        if (_processedVersions.ContainsKey(who))
        {
            _processedVersions[who] = version;
        }
        else
        {
            _processedVersions.TryAdd(who, version);
        };
        return Task.CompletedTask;
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
        ;
    }

    public Task<ulong> GetLastPresentVersion()
    {
        if (_deltas.Keys.Count > 0)
        {
            return Task.FromResult(_deltas.Keys.Min());
        }
        return Task.FromResult(0ul);
    }
}
