using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.DatasetUpdateNotifications;
using RxDatasets.Models;

namespace RxDatasets.StorageProviders;

public class ObjectStorageProvider<T>(
    ILogger<ObjectStorageProvider<T>> logger,
    IOptions<StorageConfig> storageConfig,
    IObjectStorageProvider<T> storageProvider,
    IDatasetUpdateNotifier<T> datasetUpdateNotifier,
    bool retryInit = true)
    : IStorageProvider<T>, IHostedService
{ 
    private VersionInfo _versionIndex = new();
    private Task? _initTask;
    private ulong _lowestAvailableDeltaVersion;
    private bool _cleanupDone;
    private readonly SemaphoreSlim _semaphore = new(1);

    public static string GetObjectNamePrefix(StorageConfig storageConfig) =>
        $"{storageConfig.CustomRootFolder}{typeof(T).Name}/";

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _initTask = retryInit ? InitLoop(cancellationToken) : Initialize(cancellationToken);
        return _initTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }


    public static string GetObjectName (ulong version, ObjectNameIndexOrDataEnum indexOrData, ObjectNameTypeEnum deltaOrSnapshot, StorageConfig storageConfig)
    {
        var extension = indexOrData == ObjectNameIndexOrDataEnum.INDEX ? "pb" : "pbs";

        var deltaOrSnapshotString = deltaOrSnapshot switch
        {
            ObjectNameTypeEnum.DELTA => "deltas",
            ObjectNameTypeEnum.SNAPSHOT => "snapshot",
            _ => throw new ArgumentOutOfRangeException(nameof(ObjectNameTypeEnum))
        };
        
        var indexOrDataString = indexOrData switch
        { 
            ObjectNameIndexOrDataEnum.INDEX => "index",
            ObjectNameIndexOrDataEnum.DATA => "data",
            _ => throw new ArgumentOutOfRangeException(nameof(ObjectNameIndexOrDataEnum))
        };

        return
            GetObjectNamePrefix(storageConfig) + $"{deltaOrSnapshotString}/{version}/{indexOrDataString}.{extension}";
    }

    private string GetLatestVersionFileName() => GetObjectNamePrefix(storageConfig.Value) + "VersionIndex.pb";

    public async Task DeleteOlderThan(ulong version)
    {
        using var slot = await Lock();
        if (_versionIndex.SnapshotVersions.All(x => x.Version != version))
            throw new InvalidOperationException($"Snapshot version {version} does not exist");

        //Doesn't contain the minimum age logic, but I am also not sure if that should be done here? Since the background task Subhi made does that.
        foreach (var snapshot in _versionIndex.SnapshotVersions.Where(k => k.Version < version))
        {
            string indexKey = GetObjectName(snapshot.Version, ObjectNameIndexOrDataEnum.INDEX, ObjectNameTypeEnum.SNAPSHOT, storageConfig.Value);
            string dataKey = GetObjectName(snapshot.Version, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT, storageConfig.Value);
            await storageProvider.DeleteFile(indexKey);
            await storageProvider.DeleteFile(dataKey);
        }

        for(ulong deltaToDelete = _versionIndex.EarliestDeltaVersion; deltaToDelete <= version; deltaToDelete++)
        {
            string indexKey = GetObjectName(deltaToDelete, ObjectNameIndexOrDataEnum.INDEX, ObjectNameTypeEnum.DELTA, storageConfig.Value);
            string dataKey = GetObjectName(deltaToDelete, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.DELTA, storageConfig.Value);
            await storageProvider.DeleteFile(indexKey);
            await storageProvider.DeleteFile(dataKey);
        }

        _versionIndex.EarliestDeltaVersion = version + 1;
        foreach(var deletedSnapshot in _versionIndex.SnapshotVersions.Where(k => k.Version < version))
            _versionIndex.SnapshotVersions.Remove(deletedSnapshot);

        await UpdateLatestVersion(_versionIndex);
        _cleanupDone = true;
    }

    public Task<ReadOnlyMemory<byte>> ReadDeltaData(ulong version)
    {
        string objectName = GetObjectName(version, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.DELTA, storageConfig.Value);
        return storageProvider.DownloadBytes(objectName);
    }

    public Task<ReadOnlyMemory<byte>> ReadDeltaData(ulong version, long position, int length)
    {
        var objectName = GetObjectName(version, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.DELTA, storageConfig.Value);
        return storageProvider.DownloadBytes(objectName, position, length);
    }

    public Task<ReadOnlyMemory<byte>> ReadSnapshotData(ulong version, long position, int length)
    {
        var objectName = GetObjectName(version, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT, storageConfig.Value);
        return storageProvider.DownloadBytes(objectName, position, length);
    }

    public Task ReadSnapshotData(ulong version, Action<Stream> callback)
    {
        string objectName = GetObjectName(version, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT, storageConfig.Value);
        return storageProvider.DownloadWithCallBack(objectName, callback);
    }

    public async Task StoreDelta(ulong deltaVersion, byte[] index, byte[] data)
    {
        using var slot = await Lock();
        string indexObjectName = GetObjectName(deltaVersion, ObjectNameIndexOrDataEnum.INDEX, ObjectNameTypeEnum.DELTA, storageConfig.Value);
        string dataObjectName = GetObjectName(deltaVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.DELTA, storageConfig.Value);

        //Only upload the data file in case it is not empty (which can be the case in case the delta only contains deletes)
        if(data.Length > 0)
        {
            await Task.WhenAll(
                storageProvider.UploadBytes(indexObjectName, index),
                storageProvider.UploadBytes(dataObjectName, data)
            );
        }
        else
        {
            await storageProvider.UploadBytes(indexObjectName, index);
        }
         
        _versionIndex.LatestDeltaVersion++;
        await UpdateLatestVersion(_versionIndex);
        ulong knownSnapshotVersion = (_versionIndex.SnapshotVersions.Count > 0 ? _versionIndex.SnapshotVersions.Max((s) => s.Version) : 0ul);
        datasetUpdateNotifier.Notify(new DatasetUpdateNotification()
        {
            DtoName = typeof(T).Name,
            Version = deltaVersion,
            SnapshotVersion = knownSnapshotVersion
        });
    }

    public async Task StoreSnapshot(ulong snapshotVersion, byte[] index, FileStream data)
    {
        using var slot = await Lock();
        string indexObjectName = GetObjectName(snapshotVersion, ObjectNameIndexOrDataEnum.INDEX, ObjectNameTypeEnum.SNAPSHOT, storageConfig.Value);
        string dataObjectName = GetObjectName(snapshotVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT, storageConfig.Value);

        await Task.WhenAll(
            storageProvider.UploadBytes(indexObjectName, index),
            storageProvider.UploadFileStream(dataObjectName, data)
        );

       if(_versionIndex.SnapshotVersions.All((s)=>s.Version != snapshotVersion))
        {
            _versionIndex.SnapshotVersions.Add(new VersionInfo.Types.snapshotVersion()
            {
                Timestamp = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow),
                Version = snapshotVersion
            });

            await UpdateLatestVersion(_versionIndex);
        }
        
        datasetUpdateNotifier.Notify(new DatasetUpdateNotification()
        {
            DtoName = typeof(T).Name,
            Version = _versionIndex.LatestDeltaVersion,
            SnapshotVersion = _versionIndex.SnapshotVersions.Max(sv => sv.Version),
        });
    }

    public async Task<ReadOnlyMemory<byte>> ReadDeltaIndex(ulong version)
    {
        string objectName = GetObjectName(version, ObjectNameIndexOrDataEnum.INDEX, ObjectNameTypeEnum.DELTA, storageConfig.Value);
        var bytes = await storageProvider.DownloadBytes(objectName);

        return bytes;
    }

    public async Task<ReadOnlyMemory<byte>> ReadSnapshotIndex(ulong version)
    {
        string objectName = GetObjectName(version, ObjectNameIndexOrDataEnum.INDEX, ObjectNameTypeEnum.SNAPSHOT, storageConfig.Value);
        var bytes = await storageProvider.DownloadBytes(objectName);
        return bytes;
    }

    private async Task UpdateLatestVersion(VersionInfo latestVersion)
    {
        if (latestVersion.CalculateSize() == 0) return;
        await storageProvider.UploadBytes(GetLatestVersionFileName(), latestVersion.ToByteArray());
    }

    private async Task<VersionInfo> GetLatestVersion()
    {
        //In case the latestVersion file does not exist, return an empty LatestVersion object (this happens in case of a new dataset)
        VersionInfo versionInfo;
        try
        {
            var bytes = await storageProvider.DownloadBytes(GetLatestVersionFileName());
            versionInfo = VersionInfo.Parser.ParseFrom(bytes.Span);
        }
        catch (FileNotFoundException)
        {
            versionInfo = await CreateLatestVersion();
        }
        _lowestAvailableDeltaVersion = versionInfo.EarliestDeltaVersion;
        return versionInfo;
    }

    private async Task<VersionInfo> CreateLatestVersion()
    {
        var deltas = (await storageProvider.ListObjects(GetObjectNamePrefix(storageConfig.Value)+"deltas/")).OrderBy(d => SharedMethods.ExtractNumber(d.ObjectKey)).GroupBy(g => SharedMethods.ExtractNumber(g.ObjectKey)).Select(g => g.First()).ToList();
        var listOfSnapshots = await storageProvider.ListObjects(GetObjectNamePrefix(storageConfig.Value) + "snapshot/");
        var snapshots = listOfSnapshots.OrderBy(d => SharedMethods.ExtractNumber(d.ObjectKey)).GroupBy(g=> SharedMethods.ExtractNumber(g.ObjectKey)).Select(g => g.First()).ToList();
        if (!deltas.Any()) return new VersionInfo();
        var latestVersion = new VersionInfo
        {
            LatestDeltaVersion = (ulong)SharedMethods.ExtractNumber(deltas.Last().ObjectKey),
            EarliestDeltaVersion = (ulong)SharedMethods.ExtractNumber(deltas.First().ObjectKey)
        };
        latestVersion.SnapshotVersions.AddRange(snapshots.Select(s=> new VersionInfo.Types.snapshotVersion()
        {
            Timestamp = Timestamp.FromDateTime(s.DateTime.UtcDateTime),
            Version = (ulong) SharedMethods.ExtractNumber(s.ObjectKey)
        }));
        await UpdateLatestVersion(latestVersion);
        return latestVersion;
    }

    public Task<(ulong oldestDelta, ulong newestDelta, List<(ulong version, DateTimeOffset DateTime)> snapshots)> ListVersions()
    {
        var snapshots = _versionIndex.SnapshotVersions.Select(v => (v.Version, v.Timestamp.ToDateTimeOffset())).ToList();
        return Task.FromResult((_versionIndex.EarliestDeltaVersion, _versionIndex.LatestDeltaVersion, snapshots));
    }

    private Task EnsureProviderIsInitialized()
    {
        if (_initTask is null || !_initTask.IsCompleted)
        {
            logger.LogError("Received a request before dataset it was initialized");
            throw new InvalidOperationException("Initialization is not yet completed");
        }

        if (_initTask.IsFaulted)
        {
            logger.LogError(_initTask.Exception, "Received a request, but the dataset initialization failed");
            throw new InvalidOperationException("Initialization failed", _initTask.Exception);
        }

        return Task.CompletedTask;
    }


    private async Task InitLoop(CancellationToken cancellationToken)
    {
       
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Initialize(cancellationToken);
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to initialize object storage provider");
                await Task.Delay(5 * 1000, cancellationToken);
            }
        }
        
    }

    public async Task Initialize(CancellationToken cancellationToken)
    {
        if (storageProvider == null)
        {
            throw new InvalidOperationException("Storage Provider was not initialized");
        }
        using var slot = await Lock(cancellationToken);
        _versionIndex = await GetLatestVersion();
    }

    public async Task StoreDelta(ulong deltaVersion, byte[] index, FileStream data)
    {
        using var slot = await Lock();
        var indexObjectName = GetObjectName(deltaVersion, ObjectNameIndexOrDataEnum.INDEX, ObjectNameTypeEnum.DELTA, storageConfig.Value);
        var dataObjectName = GetObjectName(deltaVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.DELTA, storageConfig.Value);

        if (data.Length > 0)
        {
            await Task.WhenAll(
                storageProvider.UploadBytes(indexObjectName, index),
                storageProvider.UploadFileStream(dataObjectName, data)
            );
        }
        
        await UpdateLatestVersion(_versionIndex);
        ulong KnownSnapshotVersion = (_versionIndex.SnapshotVersions.Count > 0 ? _versionIndex.SnapshotVersions.Max((s) => s.Version) : 0ul);

        datasetUpdateNotifier.Notify(new DatasetUpdateNotification()
        {
            DtoName = typeof(T).Name,
            Version = deltaVersion,
            SnapshotVersion = KnownSnapshotVersion
        });
    }

    private async Task<IDisposable> Lock()
    {
        await _semaphore.WaitAsync().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        return new ActionDisposable(() => _semaphore.Release());
    }

    private async Task<IDisposable> Lock(CancellationToken cancellationToken)
    {
        await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        return new ActionDisposable(() => _semaphore.Release());
    }

    

    public async Task StoreProcessedVersion(string who, ulong version)
    {
        if (_versionIndex.ProcessedVersions.ContainsKey(who))
        {
            _versionIndex.ProcessedVersions[who] = version;
        }
        else
        {
            _versionIndex.ProcessedVersions.TryAdd(who, version);
        };
        await UpdateLatestVersion(_versionIndex);
    }

    public Task<ulong> GetProcessedVersion(string who)
    {
        var versionFound = _versionIndex.ProcessedVersions.TryGetValue(who, out var version);
        if (versionFound) { return Task.FromResult(version); }
        else return Task.FromResult(0ul);
    }

    public Task<ulong> GetLowestProcessedVersion()
    {
        if (_versionIndex.ProcessedVersions.Count > 0)
        {
            return Task.FromResult(_versionIndex.ProcessedVersions.Min(v => v.Value));
        }
        else return Task.FromResult(0ul);
    }

    public async Task<ulong> GetLastPresentVersion()
    {
        if (_cleanupDone)
        {
            var allVersions = (await storageProvider.ListObjects(GetObjectNamePrefix(storageConfig.Value) + "deltas/")).Select(d => SharedMethods.ExtractNumber(d.ObjectKey)).ToList();
            _lowestAvailableDeltaVersion = allVersions.Count > 0 ? (ulong)allVersions.Min() : 0ul;
            _cleanupDone = false;
        }
        return _lowestAvailableDeltaVersion;
    }
}
