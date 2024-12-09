using System.Runtime.InteropServices;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
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
    IDiskStorage<T> storage,
    INotificationsHub<T> notificationsHub, 
    IStorageReader<T> storageReader,
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
    private readonly DeltaInfo _deltaInfo = new();
    private ProcessedVersionsInfo _processedVersionsInfo = new();
    private List<SnapshotType> _snapshots = [];


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
    

    public async Task DeleteOlderThan(ulong version, bool keepAllDeltas)
    {
        ThrowIfNoLockIsAcquired();

        using var slotSnapshot = await SnapshotLock();

        if (_snapshots.All(x => x.Version != version))
            throw new InvalidOperationException($"Snapshot version {version} does not exist");
        
        logger.LogInformation($"Storage Cleaning is happening, deleting anything older than version {version}!");

        foreach (var snapshot in _snapshots.Where(k => k.Version < version).ToList())
        {
            var folderKey = DatasetHelper.GetObjectFolder<T>(snapshot.Version, ObjectNameTypeEnum.SNAPSHOT,
                storageConfig.Value);
            await storage.DeleteFolder(folderKey);
            _snapshots.Remove(snapshot);
            await SaveSnapshotInfo();
        }

        if (!keepAllDeltas)
        {
            using var slotDelta = await DeltaLock();

            for (var deltaToDelete = _deltaInfo.EarliestDeltaVersion;
                 deltaToDelete <= version;
                 deltaToDelete++)
            {
                var folderKey = DatasetHelper.GetObjectFolder<T>(deltaToDelete, ObjectNameTypeEnum.DELTA, storageConfig.Value);
                await storage.DeleteFolder(folderKey);
            }

            _deltaInfo.EarliestDeltaVersion = version + 1;
            await SaveDeltaInfo();
        }
    }
    public Task<ReadOnlyMemory<byte>> ReadSnapshotData(ulong snapshotVersion)
    {
        return storageReader.ReadSnapshotData(snapshotVersion);
    }
    public Task ReadSnapshotData(ulong version, Action<Stream> callback)
    {
        return storageReader.ReadSnapshotData(version, callback);
    }

    public async Task StoreDelta(ulong deltaVersion, byte[] index, byte[] data)
    {
        ThrowIfNoLockIsAcquired();

        using var slot = await DeltaLock();
        var indexObjectName = DatasetHelper.GetObjectName<T>(deltaVersion, ObjectNameIndexOrDataEnum.INDEX, ObjectNameTypeEnum.DELTA,
            storageConfig.Value);
        var dataObjectName = DatasetHelper.GetObjectName<T>(deltaVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.DELTA,
            storageConfig.Value);

        //Only upload the data file in case it is not empty (which can be the case in case the delta only contains deletes)
        if (data.Length > 0)
        {
            await Task.WhenAll(
                storage.UploadBytes(indexObjectName, index),
                storage.UploadBytes(dataObjectName, data)
            );
            await VerifyArrayUploadIfConfigured(dataObjectName, data.Length);
        }
        else
            await storage.UploadBytes(indexObjectName, index);

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
        var indexObjectName = DatasetHelper.GetObjectName<T>(deltaVersion, ObjectNameIndexOrDataEnum.INDEX, ObjectNameTypeEnum.DELTA,
            storageConfig.Value);
        var dataObjectName = DatasetHelper.GetObjectName<T>(deltaVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.DELTA,
            storageConfig.Value);

        if (data.Length > 0)
        {
            await Task.WhenAll(
                storage.UploadBytes(indexObjectName, index),
                storage.UploadFileStream(dataObjectName, data)
            );
            await VerifyStreamUploadIfConfigured(dataObjectName, data.Length);
        }
        else // Deleted Delta data
            await storage.UploadBytes(indexObjectName, index);
        
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
            var actualUploadedDataLength = await storage.GetObjectContentLength(dataObjectName);
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
        var indexObjectName = DatasetHelper.GetObjectName<T>(snapshotVersion, ObjectNameIndexOrDataEnum.INDEX,
            ObjectNameTypeEnum.SNAPSHOT, storageConfig.Value);
        var dataObjectName = DatasetHelper.GetObjectName<T>(snapshotVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT,
            storageConfig.Value);
        if (data.Length > 0)
        {
            await Task.WhenAll(
                storage.UploadBytes(indexObjectName, index),
                storage.UploadFileStream(dataObjectName, data)
            );
            await VerifyStreamUploadIfConfigured(dataObjectName, data.Length);

        }
        else
        {
            await storage.UploadBytes(indexObjectName, index);
        }

        if (_snapshots.All(s => s.Version != snapshotVersion))
        {
            _snapshots.Add(new SnapshotType
            (
                snapshotVersion,
                DateTimeOffset.UtcNow
            ));

            await SaveSnapshotInfo();
        }

        await notificationsHub.Notify(
            new SnapshotCreatedNotification(_snapshots.Max(sv => sv.Version)));
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
        string indexObjectName = DatasetHelper.GetObjectName<T>(snapshotVersion, ObjectNameIndexOrDataEnum.INDEX, ObjectNameTypeEnum.SNAPSHOT, storageConfig.Value);
        string dataObjectName = DatasetHelper.GetObjectName<T>(snapshotVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT, storageConfig.Value);

        //Only upload the data file in case it is not empty (which can be the case in case the delta only contains deletes)
        if (data.Length > 0)
        {
            await Task.WhenAll(
                storage.UploadBytes(indexObjectName, index),
                storage.UploadBytes(dataObjectName, data)
            );
            await VerifyArrayUploadIfConfigured(dataObjectName, data.Length);
        }
        else
        {
            await storage.UploadBytes(indexObjectName, index);
        }

        if (_snapshots.All((s) => s.Version != snapshotVersion))
        {
            _snapshots.Add(new SnapshotType
            (
                snapshotVersion,
                DateTimeOffset.UtcNow
            ));
            
            await SaveSnapshotInfo();
        }

        await notificationsHub.Notify(
            new SnapshotCreatedNotification(_snapshots.Max(sv => sv.Version)));
    }


    public Task<ReadOnlyMemory<byte>> ReadDeltaIndex(ulong version)
    {
        return storageReader.ReadDeltaIndex(version);
    }

    public Task<ReadOnlyMemory<byte>> ReadSnapshotIndex(ulong version)
    {
        return storageReader.ReadSnapshotIndex(version);
    }

    public async Task<(ulong oldestDelta, ulong newestDelta, List<SnapshotType> snapshots)>
        ListVersions()
    {
        await EnsureProviderIsInitialized();
        
        return (_deltaInfo.EarliestDeltaVersion,
            _deltaInfo.LatestDeltaVersion, _snapshots);
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
        sources = sources.Where(source =>
        {
            switch (source.type)
            {
                case ObjectNameTypeEnum.SNAPSHOT
                    when _snapshots.All(s => s.Version != source.version):
                case ObjectNameTypeEnum.DELTA when source.version < _deltaInfo.EarliestDeltaVersion:
                    // Skip, we don't think we can handle this version request
                    return false;
                default:
                    return true;
            }
        }).ToList();
       return await storageReader.ReadDataFromSources(sources, length);
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

    
    
    private string GetProcessedVersionsFilename()
    {
        return DatasetHelper.GetObjectNamePrefix<T>(storageConfig.Value) + "ProcessedVersions.pb";
    }

    private string GetDeltaInfoFilename()
    {
        return DatasetHelper.GetObjectNamePrefix<T>(storageConfig.Value) + "DeltaInfo.pb";
    }

    private string GetSnapshotInfoFilename()
    {
        return DatasetHelper.GetObjectNamePrefix<T>(storageConfig.Value) + "SnapshotInfo.pb";
    }

    private async Task SaveDeltaInfo()
    {

        if (_deltaInfo.CalculateSize() == 0)
            return;

        var filename = GetDeltaInfoFilename();
        var data = _deltaInfo.ToByteArray();
        await storage.UploadBytes(filename, data);
    }

    private async Task SaveSnapshotInfo()
    {
        if (_snapshots.Count == 0)
            return;

        var filename = GetSnapshotInfoFilename();
        var snapshotInfo = new SnapshotInfo();
        snapshotInfo.SnapshotVersions.AddRange(_snapshots.Select(s=> new SnapshotInfo.Types.snapshotVersion(){Timestamp = s.DateTime?.ToTimestamp(), Version = s.Version}));
        var data = snapshotInfo.ToByteArray();
        await storage.UploadBytes(filename, data);
    }

    private async Task SaveProcessedVersionsInfo()
    {
        if (_processedVersionsInfo.CalculateSize() == 0)
            return;

        var filename = GetProcessedVersionsFilename();
        var data = _processedVersionsInfo.ToByteArray();
        await storage.UploadBytes(filename, data);
       
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

    private async Task<TMessage?> TryParseVersionFileAsync<TMessage>(string filenameSuffix,
        MessageParser<TMessage> parser)
        where TMessage : IMessage<TMessage>
    {
        var filename = DatasetHelper.GetObjectNamePrefix<T>(storageConfig.Value) + filenameSuffix;
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
    

    private async Task<TMessage> DownloadAndParseProtobufFileAsync<TMessage>(string filename, MessageParser<TMessage> parser)
        where TMessage : IMessage<TMessage>
    {
        logger.LogInformation($"Downloading and parsing file: {filename}");

        var fileBytes = await storage.DownloadBytes(filename);
        return parser.ParseFrom(fileBytes.Span);
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

    private async Task Initialize(CancellationToken cancellationToken)
    {
        if (storage == null) throw new InvalidOperationException("Storage Provider was not initialized");
        var versions = await storageReader.ListVersions();
        _deltaInfo.EarliestDeltaVersion = versions.OldestDelta;
        _deltaInfo.LatestDeltaVersion = versions.NewestDelta;
        _snapshots = versions.Snapshots;
        await TryLoadProcessedVersionsInfo();
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
        var objectName = DatasetHelper.GetObjectName<T>(snapshotVersion, ObjectNameIndexOrDataEnum.DATA, ObjectNameTypeEnum.SNAPSHOT,
            storageConfig.Value);
        return storage.GetObjectContentLength(objectName);
    }
}