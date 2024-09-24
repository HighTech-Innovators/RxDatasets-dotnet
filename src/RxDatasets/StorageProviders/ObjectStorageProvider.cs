using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Notifications;

namespace RxDatasets.StorageProviders;

public class ObjectStorageProvider<T>(
    ILogger<ObjectStorageProvider<T>> logger,
    IOptions<StorageConfig> storageConfig,
    IObjectStorageProvider<T> storageProvider,
    INotificationsHub<T> notificationsHub,
    bool retryInit = true)
    : IStorageProvider<T>, IHostedService
{
    private readonly SemaphoreSlim _semaphore = new(1);
    private readonly SemaphoreSlim _versionInfoSemaphore = new(1);
    private Task? _initTask;

    private DeltaInfo _deltaInfo = new();
    private SnapshotInfo _snapshotInfo = new();
    private ProcessedVersionsInfo _processedVersionsInfo = new();

    public static string GetObjectNamePrefix(StorageConfig storageConfig) =>
        $"{storageConfig.CustomRootFolder}{(string.IsNullOrEmpty(storageConfig.DtoName) ? typeof(T).Name : storageConfig.DtoName)}/";

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _initTask = retryInit ? InitLoop(cancellationToken) : Initialize(cancellationToken);
        return _initTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public Task<ReadOnlyMemory<byte>> ReadSnapshotData(ulong snapshotVersion)
    {
        var objectName = GetObjectName(snapshotVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT,
            storageConfig.Value);
        return storageProvider.DownloadBytes(objectName);
    }

    public async Task DeleteOlderThan(ulong version, bool keepAllDeltas = false)
    {
        using var slot = await Lock();
        if (_snapshotInfo.SnapshotVersions.All(x => x.Version != version))
            throw new InvalidOperationException($"Snapshot version {version} does not exist");

        foreach (var snapshot in _snapshotInfo.SnapshotVersions.Where(k => k.Version < version).ToList())
        {
            var indexKey = GetObjectName(snapshot.Version, ObjectNameIndexOrDataEnum.INDEX, ObjectNameTypeEnum.SNAPSHOT,
                storageConfig.Value);
            var dataKey = GetObjectName(snapshot.Version, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT,
                storageConfig.Value);
            await storageProvider.DeleteFile(indexKey);
            await storageProvider.DeleteFile(dataKey);
            _snapshotInfo.SnapshotVersions.Remove(snapshot);

            await SaveSnapshotInfo();
        }

        if (!keepAllDeltas)
        {
            for (var deltaToDelete = _deltaInfo.EarliestDeltaVersion;
                 deltaToDelete <= version;
                 deltaToDelete++)
            {
                var indexKey = GetObjectName(deltaToDelete, ObjectNameIndexOrDataEnum.INDEX,
                    ObjectNameTypeEnum.DELTA, storageConfig.Value);
                var dataKey = GetObjectName(deltaToDelete, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.DELTA,
                    storageConfig.Value);
                await storageProvider.DeleteFile(indexKey);
                await storageProvider.DeleteFile(dataKey);
            }

            _deltaInfo.EarliestDeltaVersion = version + 1;
            await SaveDeltaInfo();
        }
    }

    public Task ReadSnapshotData(ulong version, Action<Stream> callback)
    {
        var objectName = GetObjectName(version, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT,
            storageConfig.Value);
        return storageProvider.DownloadWithCallBack(objectName, callback);
    }

    public async Task StoreDelta(ulong deltaVersion, byte[] index, byte[] data)
    {
        using var slot = await Lock();
        var indexObjectName = GetObjectName(deltaVersion, ObjectNameIndexOrDataEnum.INDEX, ObjectNameTypeEnum.DELTA,
            storageConfig.Value);
        var dataObjectName = GetObjectName(deltaVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.DELTA,
            storageConfig.Value);

        //Only upload the data file in case it is not empty (which can be the case in case the delta only contains deletes)
        if (data.Length > 0)
            await Task.WhenAll(
                storageProvider.UploadBytes(indexObjectName, index),
                storageProvider.UploadBytes(dataObjectName, data)
            );
        else
            await storageProvider.UploadBytes(indexObjectName, index);

        _deltaInfo.LatestDeltaVersion++;
        await SaveDeltaInfo();
        await notificationsHub.Notify(new DatasetUpdatedNotification(deltaVersion));
    }

    public async Task StoreSnapshot(ulong snapshotVersion, byte[] index, Stream data)
    {
        using var slot = await Lock();
        var indexObjectName = GetObjectName(snapshotVersion, ObjectNameIndexOrDataEnum.INDEX,
            ObjectNameTypeEnum.SNAPSHOT, storageConfig.Value);
        var dataObjectName = GetObjectName(snapshotVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT,
            storageConfig.Value);
        if (data.Length > 0)
        {
            await Task.WhenAll(
                storageProvider.UploadBytes(indexObjectName, index),
                storageProvider.UploadFileStream(dataObjectName, data)
            );
        }
        else
        {
            await storageProvider.UploadBytes(indexObjectName, index);
        }

        if (_snapshotInfo.SnapshotVersions.All(s => s.Version != snapshotVersion))
        {
            _snapshotInfo.SnapshotVersions.Add(new SnapshotInfo.Types.snapshotVersion
            {
                Timestamp = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow),
                Version = snapshotVersion
            });

            await SaveSnapshotInfo();
        }

        await notificationsHub.Notify(
            new SnapshotCreatedNotification(_snapshotInfo.SnapshotVersions.Max(sv => sv.Version)));
    }

    public async Task StoreSnapshot(ulong snapshotVersion, byte[] index, byte[] data)
    {
        using var slot = await Lock();
        string indexObjectName = GetObjectName(snapshotVersion, ObjectNameIndexOrDataEnum.INDEX, ObjectNameTypeEnum.SNAPSHOT, storageConfig.Value);
        string dataObjectName = GetObjectName(snapshotVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT, storageConfig.Value);

        //Only upload the data file in case it is not empty (which can be the case in case the delta only contains deletes)
        if (data.Length > 0)
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

        if (_snapshotInfo.SnapshotVersions.All((s) => s.Version != snapshotVersion))
        {
            _snapshotInfo.SnapshotVersions.Add(new SnapshotInfo.Types.snapshotVersion()
            {
                Timestamp = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow),
                Version = snapshotVersion
            });

            await SaveSnapshotInfo();
        }

        await notificationsHub.Notify(
            new SnapshotCreatedNotification(_snapshotInfo.SnapshotVersions.Max(sv => sv.Version)));
    }


    public async Task<ReadOnlyMemory<byte>> ReadDeltaIndex(ulong version)
    {
        var objectName = GetObjectName(version, ObjectNameIndexOrDataEnum.INDEX, ObjectNameTypeEnum.DELTA,
            storageConfig.Value);
        var bytes = await storageProvider.DownloadBytes(objectName);

        return bytes;
    }

    public async Task<ReadOnlyMemory<byte>> ReadSnapshotIndex(ulong version)
    {
        var objectName = GetObjectName(version, ObjectNameIndexOrDataEnum.INDEX, ObjectNameTypeEnum.SNAPSHOT,
            storageConfig.Value);
        var bytes = await storageProvider.DownloadBytes(objectName);
        return bytes;
    }

    public async Task<(ulong oldestDelta, ulong newestDelta, List<(ulong version, DateTimeOffset DateTime)> snapshots)>
        ListVersions()
    {
        await EnsureProviderIsInitialized();

        var snapshots = _snapshotInfo.SnapshotVersions
            .Select(v => (v.Version, v.Timestamp.ToDateTimeOffset()))
            .ToList();
        return (_deltaInfo.EarliestDeltaVersion,
            _deltaInfo.LatestDeltaVersion, snapshots);
    }

    public async Task StoreDelta(ulong deltaVersion, byte[] index, Stream data)
    {
        using var slot = await Lock();
        var indexObjectName = GetObjectName(deltaVersion, ObjectNameIndexOrDataEnum.INDEX, ObjectNameTypeEnum.DELTA,
            storageConfig.Value);
        var dataObjectName = GetObjectName(deltaVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.DELTA,
            storageConfig.Value);

        if (data.Length > 0)
            await Task.WhenAll(
                storageProvider.UploadBytes(indexObjectName, index),
                storageProvider.UploadFileStream(dataObjectName, data)
            );
        else // Deleted Delta data
            await storageProvider.UploadBytes(indexObjectName, index);
        _deltaInfo.LatestDeltaVersion++;
        await SaveDeltaInfo();
        await notificationsHub.Notify(new DatasetUpdatedNotification(deltaVersion));
    }

    public async Task StoreProcessedVersion(string who, ulong version)
    {
        await EnsureProviderIsInitialized();

        // TODO add a lock here too!

        if (_processedVersionsInfo.ProcessedVersions.ContainsKey(who))
            _processedVersionsInfo.ProcessedVersions[who] = version;
        else
            _processedVersionsInfo.ProcessedVersions.TryAdd(who, version);

        await SaveProcessedVersionsInfo();
        await notificationsHub.Notify(new VersionProcessedNotification(who, version));
    }

    public async Task<ReadOnlyMemory<byte>> ReadDataFromSources(
        List<(ObjectNameTypeEnum type, ulong version, long position)> sources, int length)
    {
        await EnsureProviderIsInitialized();

        if (sources.Count == 0)
            throw new ArgumentException("at least one source must be filled");

        foreach (var (type, version, position) in sources)
            switch (type)
            {
                case ObjectNameTypeEnum.SNAPSHOT
                    when _snapshotInfo.SnapshotVersions.All(s => s.Version != version):
                case ObjectNameTypeEnum.DELTA when version < _deltaInfo.EarliestDeltaVersion:
                    // Skip, we don't think we can handle this version request
                    continue;
                default:
                {
                    var objectName = GetObjectName(version, ObjectNameIndexOrDataEnum.DATA, type, storageConfig.Value);
                    return await storageProvider.DownloadBytes(objectName, position, length);
                }
            }

        throw new ArgumentException("None of the sources is available");
    }

    public async Task<ulong> GetProcessedVersion(string who)
    {
        await EnsureProviderIsInitialized();

        var versionFound = _processedVersionsInfo.ProcessedVersions.TryGetValue(who, out var version);
        return versionFound ? version : 0ul;
    }

    public async Task<ulong> GetLowestProcessedVersion()
    {
        await EnsureProviderIsInitialized();

        return _processedVersionsInfo.ProcessedVersions.Count > 0
            ? _processedVersionsInfo.ProcessedVersions.Min(v => v.Value)
            : 0ul;
    }

    public static string GetObjectName(ulong version, ObjectNameIndexOrDataEnum indexOrData,
        ObjectNameTypeEnum deltaOrSnapshot, StorageConfig storageConfig)
    {
        var extension = indexOrData == ObjectNameIndexOrDataEnum.INDEX ? "pb" : "pbs";

        var deltaOrSnapshotString = deltaOrSnapshot switch
        {
            ObjectNameTypeEnum.DELTA => "deltas",
            ObjectNameTypeEnum.SNAPSHOT => "snapshots",
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

    private string GetProcessedVersionsFilename()
    {
        return GetObjectNamePrefix(storageConfig.Value) + "ProcessedVersions.pb";
    }

    private string GetDeltaInfoFilename()
    {
        return GetObjectNamePrefix(storageConfig.Value) + "DeltaInfo.pb";
    }

    private string GetSnapshotInfoFilename()
    {
        return GetObjectNamePrefix(storageConfig.Value) + "SnapshotInfo.pb";
    }

    private async Task SaveDeltaInfo()
    {
        if (_deltaInfo.CalculateSize() == 0)
            return;

        var filename = GetDeltaInfoFilename();
        var data = _deltaInfo.ToByteArray();
        await storageProvider.UploadBytes(filename, data);
    }

    private async Task SaveSnapshotInfo()
    {
        if (_snapshotInfo.CalculateSize() == 0)
            return;

        var filename = GetSnapshotInfoFilename();
        var data = _snapshotInfo.ToByteArray();
        await storageProvider.UploadBytes(filename, data);
      
    }

    private async Task SaveProcessedVersionsInfo()
    {
        if (_processedVersionsInfo.CalculateSize() == 0)
            return;

        var filename = GetProcessedVersionsFilename();
        var data = _processedVersionsInfo.ToByteArray();
        await storageProvider.UploadBytes(filename, data);
       
    }

    private async Task InitVersionInfos()
    {
        logger.LogInformation("Starting LoadLatestVersion");

        if (await TryGetLatestVersionFromFiles())
        {
            logger.LogInformation("Successfully retrieved latest version from primary files.");
            return;
        }

        logger.LogInformation(
            "Primary files not found or could not retrieve latest version, falling back to alternative methods.");

        await TryGetVersionFromFallbacks();
    }

    private async Task<bool> TryGetLatestVersionFromFiles()
    {
        try
        {
            logger.LogInformation("Attempting to retrieve latest version from Delta and Snapshot Info files.");

            var deltaTask = DownloadAndParseProtobufFileAsync<DeltaInfo>(GetDeltaInfoFilename(), DeltaInfo.Parser);
            var snapshotTask = DownloadAndParseProtobufFileAsync<SnapshotInfo>(GetSnapshotInfoFilename(), SnapshotInfo.Parser);

            // Use Task.WhenAll to perform file reads concurrently
            //  This ensures that we don't pass when only one of them succeeds
            await Task.WhenAll(deltaTask, snapshotTask);

            _deltaInfo = await deltaTask;
            _snapshotInfo = await snapshotTask;

            logger.LogInformation("Successfully loaded DeltaInfo and SnapshotInfo files.");

            await TryLoadProcessedVersionsInfo();

            return true;
        }
        catch (FileNotFoundException ex)
        {
            logger.LogInformation($"FileNotFoundException: {ex.FileName}. Delta or Snapshot file not found.");
            // Return null to signal fallback usage
            return false;
        }
    }

    private async Task TryLoadProcessedVersionsInfo()
    {
        try
        {
            logger.LogInformation("Attempting to update with processed versions file.");

            _processedVersionsInfo =
                await DownloadAndParseProtobufFileAsync<ProcessedVersionsInfo>(GetProcessedVersionsFilename(),
                    ProcessedVersionsInfo.Parser);
            logger.LogInformation("Successfully updated with processed versions.");
        }
        catch (FileNotFoundException)
        {
            logger.LogInformation("Processed versions file not found, continuing with current version.");
            // No processed versions found, return the current version as-is
        }
    }

    private async Task TryGetVersionFromFallbacks()
    {
        logger.LogInformation("Attempting to retrieve version from fallback files.");

        var versionV2 = await TryParseVersionFileAsync<ObsoleteV2VersionInfo>("VersionIndex.pb", ObsoleteV2VersionInfo.Parser);
        if (versionV2 != null)
        {
            logger.LogInformation("Successfully retrieved version from VersionIndexV2.");
            await CreateVersionFromV2Data(versionV2);
            return;
        }

        logger.LogInformation("VersionIndexV2 not found, attempting to retrieve version from VersionIndexV1.");
        var versionV1 = await TryParseVersionFileAsync<ObsoleteV1LatestVersion>("latestVersion.pb", ObsoleteV1LatestVersion.Parser);
        if (versionV1 != null)
        {
            logger.LogInformation("Successfully retrieved version from VersionIndexV1.");
            await CreateVersionFromV1Data(versionV1);
            return;
        }

        logger.LogInformation("No valid fallback versions found. Creating new latest version.");
        await CreateLatestVersionFromObjectLists();
    }

    private async Task<TMessage?> TryParseVersionFileAsync<TMessage>(string filenameSuffix,
        MessageParser<TMessage> parser)
        where TMessage : IMessage<TMessage>
    {
        var filename = GetObjectNamePrefix(storageConfig.Value) + filenameSuffix;
        try
        {
            logger.LogInformation($"Attempting to parse file: {filename}");
            return await DownloadAndParseProtobufFileAsync(filename, parser);
        }
        catch (FileNotFoundException ex)
        {
            logger.LogInformation($"FileNotFoundException: {ex.Message}. File {filename} not found.");
            return default;
        }
    }

    private async Task CreateVersionFromV2Data(ObsoleteV2VersionInfo versionIndexDataV2)
    {
        logger.LogInformation("Creating version from VersionIndexV2 data.");

        _processedVersionsInfo = new ProcessedVersionsInfo();
        _processedVersionsInfo.ProcessedVersions.MergeFrom(versionIndexDataV2.ProcessedVersions);

        _deltaInfo = new DeltaInfo
        {
            LatestDeltaVersion = versionIndexDataV2.LatestDeltaVersion,
            EarliestDeltaVersion = versionIndexDataV2.EarliestDeltaVersion
        };

        _snapshotInfo = new SnapshotInfo
            {
                SnapshotVersions =
                {
                    versionIndexDataV2.SnapshotVersions.Select(s => new SnapshotInfo.Types.snapshotVersion
                        { Timestamp = s.Timestamp, Version = s.Version })
                }
            };

        await SaveDeltaInfo();
        await SaveSnapshotInfo();
        await SaveProcessedVersionsInfo();

        logger.LogInformation("Version created successfully from VersionIndexV2.");
    }

    private async Task CreateVersionFromV1Data(ObsoleteV1LatestVersion versionIndexDataV1)
    {
        logger.LogInformation("Creating version from VersionIndexV1 data.");

        _deltaInfo =
            new DeltaInfo
            {
                LatestDeltaVersion = versionIndexDataV1.LatestDeltaVersion
            };
        _snapshotInfo = new SnapshotInfo
        {
            SnapshotVersions =
            {
                new SnapshotInfo.Types.snapshotVersion
                {
                    Timestamp = DateTimeOffset.UtcNow.ToTimestamp(),
                    Version = versionIndexDataV1.LatestSnapshotVersion
                }
            }
        };
        _processedVersionsInfo = new();

        await SaveDeltaInfo();
        await SaveSnapshotInfo();
        await SaveProcessedVersionsInfo();

        logger.LogInformation("Version created successfully from VersionIndexV1.");
    }

    private async Task<TMessage> DownloadAndParseProtobufFileAsync<TMessage>(string filename, MessageParser<TMessage> parser)
        where TMessage : IMessage<TMessage>
    {
        logger.LogInformation($"Downloading and parsing file: {filename}");

        var fileBytes = await storageProvider.DownloadBytes(filename);
        return parser.ParseFrom(fileBytes.Span);
    }

    private async Task CreateLatestVersionFromObjectLists()
    {
        var deltas = (await storageProvider.ListObjects(GetObjectNamePrefix(storageConfig.Value) + "deltas/"))
            .OrderBy(d => SharedMethods.ExtractNumber(d.ObjectKey))
            .GroupBy(g => SharedMethods.ExtractNumber(g.ObjectKey))
            .Select(g => g.First())
            .ToList();

        var listOfSnapshots = await storageProvider.ListObjects(GetObjectNamePrefix(storageConfig.Value) + "snapshot/");
        var snapshots = listOfSnapshots.OrderBy(d => SharedMethods.ExtractNumber(d.ObjectKey))
            .GroupBy(g => SharedMethods.ExtractNumber(g.ObjectKey))
            .Select(g => g.First())
            .ToList();

        _deltaInfo = new();
        _snapshotInfo = new();
        _processedVersionsInfo = new();

        _deltaInfo.EarliestDeltaVersion = deltas.Count > 0 ? (ulong)SharedMethods.ExtractNumber(deltas.First().ObjectKey): 0ul;
        _deltaInfo.LatestDeltaVersion = deltas.Count > 0 ? (ulong)SharedMethods.ExtractNumber(deltas.Last().ObjectKey): 0ul;

        _snapshotInfo.SnapshotVersions.AddRange(snapshots.Select(s =>
            new SnapshotInfo.Types.snapshotVersion
            {
                Timestamp = Timestamp.FromDateTime(s.DateTime.UtcDateTime),
                Version = (ulong)SharedMethods.ExtractNumber(s.ObjectKey)
            }));

        await SaveDeltaInfo();
        await SaveSnapshotInfo();
        await SaveProcessedVersionsInfo();
    }

    private Task EnsureProviderIsInitialized()
    {
        // TODO if not initialized but the task is there, at least wait for a second before throwing.
        // This might save end-users the trouble of having to retry in a 'quick call after startup' scenario

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

    public async Task Initialize(CancellationToken cancellationToken)
    {
        if (storageProvider == null) throw new InvalidOperationException("Storage Provider was not initialized");
        await _versionInfoSemaphore.WaitAsync(cancellationToken).ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        try
        {
            await InitVersionInfos();
        }
        finally
        {
            _versionInfoSemaphore.Release();

        }
    }

    private async Task<IDisposable> Lock(CancellationToken cancellationToken = default)
    {
        await EnsureProviderIsInitialized();

        await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        return new ActionDisposable(() => _semaphore.Release());
    }

}