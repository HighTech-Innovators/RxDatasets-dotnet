using System.Collections.Concurrent;
using HTI.RxDatasets.Abstractions;
using HTI.RxDatasets.DatasetUpdateNotifications;
using Microsoft.Extensions.Logging;

namespace HTI.RxDatasets.StorageProviders;

public class InMemoryStorageProvider<T>(ILogger<InMemoryStorageProvider<T>> _logger, IDatasetUpdateNotifier<T> datasetUpdateNotifier) : IStorageProvider<T>
{
    private readonly ConcurrentDictionary<ulong, (ReadOnlyMemory<byte> index, ReadOnlyMemory<byte> data)> _Deltas = new();
    private readonly ConcurrentDictionary<ulong, (ReadOnlyMemory<byte> index, ReadOnlyMemory<byte> data, DateTimeOffset creationTime)> _Snapshots = new();
    private readonly ConcurrentDictionary<string, ulong> ProcessedVersions = new();
    private readonly IDatasetUpdateNotifier<T> _datasetUpdateNotifier = datasetUpdateNotifier;

    public Task DeleteOlderThan(ulong version)
    {
        foreach (var key in _Deltas.Keys.Where(k => k <= version))
        {
            _Deltas.Remove(key, out _);
        }

        foreach (var key in _Snapshots.Keys.Where(k => k < version))
        {
            _Snapshots.Remove(key, out _);
        }

        return Task.CompletedTask;
    }

    public Task Initialize()
    {
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

        _Deltas[deltaVersion] = (index, dataBytes);

        _datasetUpdateNotifier.Notify(new DatasetUpdateNotification()
        {
            DtoName = typeof(T).Name,
            Version = deltaVersion,
            SnapshotVersion = _Snapshots.Keys.Count > 0 ? _Snapshots.Keys.Max() : 0ul
        });
        return Task.CompletedTask;
    }

    public Task<(ulong oldestDelta, ulong newestDelta, List<(ulong version, DateTimeOffset DateTime)> snapshots)> ListVersions()
    {
        if (_Deltas.IsEmpty)
            return Task.FromResult((0ul, 0ul, new List<(ulong, DateTimeOffset)>()));

        var oldestDelta = _Deltas.Keys.Min();
        var newestDelta = _Deltas.Keys.Max();
        var snapshots = _Snapshots.Select(x => (x.Key, x.Value.creationTime)).GroupBy((g) => g.Key).Select((s) => s.First()).ToList();

        return Task.FromResult((oldestDelta, newestDelta, snapshots));
    }

    public Task<ReadOnlyMemory<byte>> ReadDeltaData(ulong version)
    {
        if (_Deltas.TryGetValue(version, out var data))
        {
            return Task.FromResult(data.data);
        }

        throw new FileNotFoundException($"Delta does not exist {version}");
    }

    public Task<ReadOnlyMemory<byte>> ReadDeltaData(ulong version, long position, int length)
    {
        if (_Deltas.TryGetValue(version, out var data))
        {
            return Task.FromResult(data.data.Slice((int)position, length));
        }

        throw new FileNotFoundException($"Delta does not exist {version}");
    }

    public Task<ReadOnlyMemory<byte>> ReadDeltaIndex(ulong version)
    {
        if (_Deltas.TryGetValue(version, out var delta))
        {
            return Task.FromResult(delta.index);
        }

        throw new FileNotFoundException($"Delta does not exist {version}");
    }

    public Task<ReadOnlyMemory<byte>> ReadSnapshotData(ulong version, long position, int length)
    {
        if (_Snapshots.TryGetValue(version, out var data))
        {
            return Task.FromResult(data.data.Slice((int)position, length));
        }

        throw new FileNotFoundException($"Snapshot does not exist {version}");
    }

    public Task ReadSnapshotData(ulong version, Action<Stream> callback)
    {
        if (_Snapshots.TryGetValue(version, out var data))
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
        if (_Snapshots.TryGetValue(version, out var snapshot))
        {
            return Task.FromResult(snapshot.index);
        }

        throw new FileNotFoundException($"Snapshot does not exist {version}");
    }

    public Task StoreDelta(ulong deltaVersion, byte[] index, byte[] data)
    {
        _Deltas[deltaVersion] = (index, data);
        _datasetUpdateNotifier.Notify(new DatasetUpdateNotification()
        {
            DtoName = typeof(T).Name,
            Version = deltaVersion,
            SnapshotVersion = _Snapshots.Keys.Count > 0 ? _Snapshots.Keys.Max() : 0ul
        });
        return Task.CompletedTask;
    }

    public Task StoreSnapshot(ulong snapshotVersion, byte[] index, FileStream data)
    {
        data.Position = 0;
        MemoryStream memoryStream = new MemoryStream();
        data.CopyTo(memoryStream);
        _Snapshots[snapshotVersion] = (index, memoryStream.ToArray(), DateTimeOffset.UtcNow);
        _datasetUpdateNotifier.Notify(new DatasetUpdateNotification()
        {
            DtoName = typeof(T).Name,
            Version = _Deltas.Keys.Count > 0 ? _Deltas.Keys.Max() : 0ul,
            SnapshotVersion = snapshotVersion
        });
        return Task.CompletedTask;
    }

    public Task StoreProcessedVersion(string who, ulong version)
    {
        if (ProcessedVersions.ContainsKey(who))
        {
            ProcessedVersions[who] = version;
        }
        else
        {
            ProcessedVersions.TryAdd(who, version);
        };
        return Task.CompletedTask;
    }

    public Task<ulong> GetProcessedVersion(string who)
    {
        var versionFound = ProcessedVersions.TryGetValue(who, out var version);
        if (versionFound) {
            _logger.LogInformation($"we found version {versionFound} for {who}");
            return Task.FromResult(version); 
        }
        else return Task.FromResult(0ul);
    }

    public Task<ulong> GetLowestProcessedVersion()
    {
        return Task.FromResult(ProcessedVersions.Count > 0 ? ProcessedVersions.Min(v => v.Value) : 0ul);
        ;
    }

    public Task<ulong> GetLastPresentVersion()
    {
        if (_Deltas.Keys.Count > 0)
        {
            return Task.FromResult(_Deltas.Keys.Min());
        }
        return Task.FromResult(0ul);
    }
}
