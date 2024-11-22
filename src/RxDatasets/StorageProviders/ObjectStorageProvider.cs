using System.Runtime.InteropServices;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Notifications;

namespace RxDatasets.StorageProviders;

public class ObjectStorageProvider<T>(
    ILogger<ObjectStorageProvider<T>> logger,
    IOptions<StorageConfig> storageConfig,
    IObjectStorageProvider<T> storageProvider,
    INotificationsHub<T> notificationsHub,
    bool retryInit = true,
    DistributedLockService<T>? lockService = null
    )
    : IStorageProvider<T>, IHostedService
{
    private readonly SemaphoreSlim _deltaSemaphore = new(1);
    private readonly SemaphoreSlim _snapshotSemaphore = new(1);
    private readonly SemaphoreSlim _processedVersionsSemaphore = new(1);
    private Task? _initTask;
    private IDisposable _disposableSubscription;
    private DeltaInfo _deltaInfo = new();
    private SnapshotInfo _snapshotInfo = new();
    private ProcessedVersionsInfo _processedVersionsInfo = new();

    public static string GetObjectNamePrefix(StorageConfig storageConfig) =>
        $"{storageConfig.CustomRootFolder}{(string.IsNullOrEmpty(storageConfig.DtoName) ? typeof(T).Name : storageConfig.DtoName)}/";

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if(lockService is null)
            _initTask = retryInit ? InitLoop(cancellationToken) : Initialize(cancellationToken);

        _disposableSubscription = notificationsHub.Subscribe<DistributedLockNotification>(async (notification) => {
            if (notification.LockStatus == DistirbutedLockStatus.Acquried)
            {
                _initTask = InitLoop(cancellationToken);
                await _initTask;
                return;
            }

            _initTask = null;
            await notificationsHub.Notify(new StorageProviderReadinessNotifcation(ServiceStatus.NotReady));
        });

        return _initTask ?? Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _disposableSubscription?.Dispose();
        notificationsHub.Notify(new StorageProviderReadinessNotifcation(ServiceStatus.NotReady));

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
        ThrowIfNoLockIsAcquired();

        using var slotSnapshot = await SnapshotLock();

        if (_snapshotInfo.SnapshotVersions.All(x => x.Version != version))
            throw new InvalidOperationException($"Snapshot version {version} does not exist");
        
        logger.LogInformation($"Storage Cleaning is happening, deleting anything older than version {version}!");

        foreach (var snapshot in _snapshotInfo.SnapshotVersions.Where(k => k.Version < version).ToList())
        {
            var folderKey = GetObjectFolder(snapshot.Version, ObjectNameTypeEnum.SNAPSHOT,
                storageConfig.Value);
            await storageProvider.DeleteFolder(folderKey);
            _snapshotInfo.SnapshotVersions.Remove(snapshot);
            await SaveSnapshotInfo();
        }

        if (!keepAllDeltas)
        {
            using var slotDelta = await DeltaLock();

            for (var deltaToDelete = _deltaInfo.EarliestDeltaVersion;
                 deltaToDelete <= version;
                 deltaToDelete++)
            {
                var folderKey = GetObjectFolder(deltaToDelete, ObjectNameTypeEnum.DELTA, storageConfig.Value);
                await storageProvider.DeleteFolder(folderKey);
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
        ThrowIfNoLockIsAcquired();

        using var slot = await DeltaLock();
        var indexObjectName = GetObjectName(deltaVersion, ObjectNameIndexOrDataEnum.INDEX, ObjectNameTypeEnum.DELTA,
            storageConfig.Value);
        var dataObjectName = GetObjectName(deltaVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.DELTA,
            storageConfig.Value);

        //Only upload the data file in case it is not empty (which can be the case in case the delta only contains deletes)
        if (data.Length > 0)
        {
            await Task.WhenAll(
                storageProvider.UploadBytes(indexObjectName, index),
                storageProvider.UploadBytes(dataObjectName, data)
            );
            await VerifyArrayUploadIfConfigured(dataObjectName, data.Length);
        }
        else
            await storageProvider.UploadBytes(indexObjectName, index);

        if (_deltaInfo.EarliestDeltaVersion == 0)
        {
            _deltaInfo.EarliestDeltaVersion = deltaVersion;
        }
        _deltaInfo.LatestDeltaVersion++;
        await SaveDeltaInfo();
        await notificationsHub.Notify(new DatasetUpdatedNotification(deltaVersion));
    }
    public async Task StoreDelta(ulong deltaVersion, byte[] index, Stream data)
    {
        ThrowIfNoLockIsAcquired();
        
        using var slot = await DeltaLock();
        var indexObjectName = GetObjectName(deltaVersion, ObjectNameIndexOrDataEnum.INDEX, ObjectNameTypeEnum.DELTA,
            storageConfig.Value);
        var dataObjectName = GetObjectName(deltaVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.DELTA,
            storageConfig.Value);

        if (data.Length > 0)
        {
            await Task.WhenAll(
                storageProvider.UploadBytes(indexObjectName, index),
                storageProvider.UploadFileStream(dataObjectName, data)
            );
            await VerifyStreamUploadIfConfigured(dataObjectName, data.Length);
        }
        else // Deleted Delta data
            await storageProvider.UploadBytes(indexObjectName, index);
        
        if (_deltaInfo.EarliestDeltaVersion == 0)
        {
            _deltaInfo.EarliestDeltaVersion = deltaVersion;
        }
        
        _deltaInfo.LatestDeltaVersion++;
        await SaveDeltaInfo();
        await notificationsHub.Notify(new DatasetUpdatedNotification(deltaVersion));
    }
    private async Task VerifyArrayUploadIfConfigured(string dataObjectName, int expectedDataLength)
    {
        if (storageConfig.Value.UploadVerificationMode == UploadVerificationMode.VerifyAll)
        {
            await VerifyDataUpload(dataObjectName, expectedDataLength);
        }
    }

    private async Task VerifyDataUpload(string dataObjectName, long expectedDataLength)
    {
        try
        {
            var actualUploadedDataLength = await storageProvider.GetObjectContentLength(dataObjectName);
            if (actualUploadedDataLength != expectedDataLength)
                throw new ExternalException(
                    $"Delta {dataObjectName}: Expected uploaded data length to be {expectedDataLength}, but actual length is {actualUploadedDataLength}");
        }
        catch (FileNotFoundException)
        {
            throw new ExternalException(
                $"Delta {dataObjectName}: Expected an uploaded data file, but nothing was uploaded");
        }
    }

    private void ThrowIfNoLockIsAcquired()
    {
        if (lockService != null && !lockService.HasLock)
            throw new SynchronizationLockException("We can't write to storage as we have to get a lock first");
    }

    public async Task StoreSnapshot(ulong snapshotVersion, byte[] index, Stream data)
    {
        ThrowIfNoLockIsAcquired();
        using var slot = await SnapshotLock();
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
            await VerifyStreamUploadIfConfigured(dataObjectName, data.Length);

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

    private async Task VerifyStreamUploadIfConfigured(string dataObjectName, long expectedDataLength)
    {
        if (storageConfig.Value.UploadVerificationMode is UploadVerificationMode.VerifyAll or UploadVerificationMode.VerifyLargeDataOnly)
        {
            await VerifyDataUpload(dataObjectName, expectedDataLength);
        }    
    }

    public async Task StoreSnapshot(ulong snapshotVersion, byte[] index, byte[] data)
    {
        ThrowIfNoLockIsAcquired();

        using var slot = await SnapshotLock();
        string indexObjectName = GetObjectName(snapshotVersion, ObjectNameIndexOrDataEnum.INDEX, ObjectNameTypeEnum.SNAPSHOT, storageConfig.Value);
        string dataObjectName = GetObjectName(snapshotVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT, storageConfig.Value);

        //Only upload the data file in case it is not empty (which can be the case in case the delta only contains deletes)
        if (data.Length > 0)
        {
            await Task.WhenAll(
                storageProvider.UploadBytes(indexObjectName, index),
                storageProvider.UploadBytes(dataObjectName, data)
            );
            await VerifyArrayUploadIfConfigured(dataObjectName, data.Length);
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

   

    public async Task StoreProcessedVersion(string who, ulong version)
    {
        ThrowIfNoLockIsAcquired();
        await EnsureProviderIsInitialized();

        await _processedVersionsSemaphore.WaitAsync().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        try
        {
            if (_processedVersionsInfo.ProcessedVersions.ContainsKey(who))
                _processedVersionsInfo.ProcessedVersions[who] = version;
            else
                _processedVersionsInfo.ProcessedVersions.TryAdd(who, version);

            await SaveProcessedVersionsInfo();
            await notificationsHub.Notify(new VersionProcessedNotification(who, version));

        }
        finally
        {
            _processedVersionsSemaphore.Release();
        }
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

    public static string GetObjectFolder(ulong version, ObjectNameTypeEnum deltaOrSnapshot, StorageConfig storageConfig)
    {
        var deltaOrSnapshotString = deltaOrSnapshot switch
        {
            ObjectNameTypeEnum.DELTA => "deltas",
            ObjectNameTypeEnum.SNAPSHOT => "snapshots",
            _ => throw new ArgumentOutOfRangeException(nameof(ObjectNameTypeEnum))
        };

        return GetObjectNamePrefix(storageConfig) + $"{deltaOrSnapshotString}/{version}";
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

        ThrowIfNoLockIsAcquired();

        await TryGetVersionFromFallbacks();
    }

    private async Task<bool> TryGetLatestVersionFromFiles()
    {
        try
        {
            logger.LogInformation($"Attempting to retrieve latest version from Delta and Snapshot Info files. for {typeof(T).Name}");

            var deltaTask = DownloadAndParseProtobufFileAsync<DeltaInfo>(GetDeltaInfoFilename(), DeltaInfo.Parser);
            var snapshotTask = DownloadAndParseProtobufFileAsync<SnapshotInfo>(GetSnapshotInfoFilename(), SnapshotInfo.Parser);

            // Use Task.WhenAll to perform file reads concurrently
            //  This ensures that we don't pass when only one of them succeeds
            await Task.WhenAll(deltaTask, snapshotTask);

            _deltaInfo = await deltaTask;
            _snapshotInfo = await snapshotTask;

            logger.LogInformation($"Successfully loaded DeltaInfo and SnapshotInfo files for {typeof(T).Name}.");

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
            logger.LogInformation($"Successfully retrieved version from VersionIndexV2 for {typeof(T).Name}.");
            await CreateVersionFromV2Data(versionV2);
            return;
        }

        logger.LogInformation("VersionIndexV2 not found, attempting to retrieve version from VersionIndexV1.");
        var versionV1 = await TryParseVersionFileAsync<ObsoleteV1LatestVersion>("latestVersion.pb", ObsoleteV1LatestVersion.Parser);
        if (versionV1 != null)
        {
            logger.LogInformation($"Successfully retrieved version from VersionIndexV1 for {typeof(T).Name}.");
            await CreateVersionFromV1Data(versionV1);
            return;
        }

        logger.LogInformation($"No valid fallback versions found. Creating new latest version for {typeof(T).Name}.");
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

    private async Task EnsureProviderIsInitialized()
    {
        if (_initTask is { IsFaulted: true })
        {
            logger.LogError(_initTask.Exception, "Received a request, but the dataset initialization failed");
            throw new InvalidOperationException("Initialization failed", _initTask.Exception);
        }

        if (_initTask is { IsCompleted: true })
            return;

        if (_initTask is null)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(100));
            if (_initTask is null)
            {
                logger.LogError("Received a request before dataset started initializing");
                throw new InvalidOperationException("Initialization is not yet completed");
            }
        }

        if (!_initTask.IsCompleted)
        {
            try
            {
                await _initTask.WaitAsync(TimeSpan.FromSeconds(1));
            }
            catch (TimeoutException)
            {
                logger.LogError("Received a request before dataset is initialized");
                throw new InvalidOperationException("Initialization is not yet completed");
            }
        }
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
            catch (SynchronizationLockException ex)
            {
                logger.LogError(ex, "Failed to initialize object storage provider because no lock to write version files was available");
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
        if (storageProvider == null) throw new InvalidOperationException("Storage Provider was not initialized");
        await InitVersionInfos();
        await notificationsHub.Notify(new StorageProviderReadinessNotifcation(ServiceStatus.Ready));
    }

    private async Task<IDisposable> DeltaLock(CancellationToken cancellationToken = default)
    {
        await EnsureProviderIsInitialized();

        await _deltaSemaphore.WaitAsync(cancellationToken).ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        return new ActionDisposable(() => _deltaSemaphore.Release());
    }

    private async Task<IDisposable> SnapshotLock(CancellationToken cancellationToken = default)
    {
        await EnsureProviderIsInitialized();

        await _snapshotSemaphore.WaitAsync(cancellationToken).ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        return new ActionDisposable(() => _snapshotSemaphore.Release());
    }

    public Task<long> GetSnapshotDataContentLength(ulong snapshotVersion)
    {
        var objectName = GetObjectName(snapshotVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT,
            storageConfig.Value);
        return storageProvider.GetObjectContentLength(objectName);
    }
}