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
using RxDatasets.StorageProviders.ReadOnly;

namespace RxDatasets.StorageProviders;


public class ObjectStorageProvider<T>(
    ILogger<ObjectStorageProvider<T>> logger,
    IOptions<StorageConfig> storageConfig,
    IDiskStorage<T> storage,
    INotificationsHub<T> notificationsHub,
    IStorageReader<T> storageReader,
    bool startIfInitFails = true,
    DistributedLockService<T>? lockService = null
    )
    : ReadOnlyObjectStorageProvider<T>(logger, storageReader, notificationsHub, startIfInitFails), IStorageProvider<T>, IHostedService
{
    private readonly SemaphoreSlim _deltaSemaphore = new(1);
    private readonly SemaphoreSlim _snapshotSemaphore = new(1);
    private readonly SemaphoreSlim _processedVersionsSemaphore = new(1);
    private Task? _initTask;
    private IDisposable? _disposableSubscription;
    private ProcessedVersionsInfo _processedVersionsInfo = new();
    private readonly IOptions<StorageConfig> _storageConfig = storageConfig;
    private readonly IDiskStorage<T> _storage = storage;
    private readonly INotificationsHub<T> _notificationsHub = notificationsHub;
    private readonly bool _startIfInitFails = startIfInitFails;

    public new async Task StartAsync(CancellationToken cancellationToken)
    {
        if (lockService is null)
        {
            _initTask = _startIfInitFails ? InitLoop(cancellationToken) : Initialize(cancellationToken);
            await _initTask;
        }


        _disposableSubscription = _notificationsHub.Subscribe<DistributedLockNotification>(async (notification) =>
        {
            if (notification.LockStatus == DistirbutedLockStatus.Acquried)
            {
                _initTask = InitLoop(cancellationToken);
                await _initTask;
                return;
            }

            _initTask = null;
            await _notificationsHub.Notify(new StorageProviderReadinessNotifcation(ServiceStatus.NotReady));
        });

    }

    public new async Task StopAsync(CancellationToken cancellationToken)
    {
        _disposableSubscription?.Dispose();
        await base.StopAsync(cancellationToken);
        await _notificationsHub.Notify(new StorageProviderReadinessNotifcation(ServiceStatus.NotReady));
    }

    public virtual async Task DeleteOlderThan(ulong version, bool keepAllDeltas)
    {
        ThrowIfNoLockIsAcquired();

        using var slotSnapshot = await SnapshotLockAsync();

        if (Snapshots.All(x => x.Version != version))
            throw new InvalidOperationException($"Snapshot version {version} does not exist");

        logger.LogInformation($"Storage Cleaning is happening, deleting anything older than version {version}!");

        foreach (var snapshot in Snapshots.Where(k => k.Version < version).ToList())
        {
            var folderKey = DatasetHelper.GetObjectFolder<T>(snapshot.Version, ObjectNameTypeEnum.Snapshot,
                _storageConfig.Value);
            await _storage.DeleteFolder(folderKey + '/');
            Snapshots.Remove(snapshot);
            await SaveSnapshotInfo();
        }

        if (!keepAllDeltas)
        {
            using var slotDelta = await DeltaLockAsync();

            for (var deltaToDelete = DeltaInfo.EarliestDeltaVersion;
                 deltaToDelete <= version;
                 deltaToDelete++)
            {
                var folderKey = DatasetHelper.GetObjectFolder<T>(deltaToDelete, ObjectNameTypeEnum.Delta, _storageConfig.Value);
                await _storage.DeleteFolder(folderKey + '/');
            }

            DeltaInfo.EarliestDeltaVersion = version + 1;
            await SaveDeltaInfo();
        }
    }

    public virtual async Task StoreDelta(ulong deltaVersion, byte[] index, byte[] data)
    {
        ThrowIfNoLockIsAcquired();

        using var slot = await DeltaLockAsync();
        var indexObjectName = DatasetHelper.GetObjectName<T>(deltaVersion, ObjectNameIndexOrDataEnum.Index, ObjectNameTypeEnum.Delta,
            _storageConfig.Value);
        var dataObjectName = DatasetHelper.GetObjectName<T>(deltaVersion, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Delta,
            _storageConfig.Value);

        //Only upload the data file in case it is not empty (which can be the case in case the delta only contains deletes)
        if (data.Length > 0)
        {
            await Task.WhenAll(
                _storage.UploadBytes(indexObjectName, index),
                _storage.UploadBytes(dataObjectName, data)
            );
            await VerifyArrayUploadIfConfigured(dataObjectName, data.Length);
        }
        else
            await _storage.UploadBytes(indexObjectName, index);

        await PersistDeltaInfo(deltaVersion);
        await _notificationsHub.Notify(new DatasetUpdatedNotification(deltaVersion));
    }
    public virtual async Task StoreDelta(ulong deltaVersion, byte[] index, Stream data)
    {
        ThrowIfNoLockIsAcquired();

        using var slot = await DeltaLockAsync();
        var indexObjectName = DatasetHelper.GetObjectName<T>(deltaVersion, ObjectNameIndexOrDataEnum.Index, ObjectNameTypeEnum.Delta,
            _storageConfig.Value);
        var dataObjectName = DatasetHelper.GetObjectName<T>(deltaVersion, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Delta,
            _storageConfig.Value);

        if (data.Length > 0)
        {
            await Task.WhenAll(
                _storage.UploadBytes(indexObjectName, index),
                _storage.UploadFileStream(dataObjectName, data)
            );
            await VerifyStreamUploadIfConfigured(dataObjectName, data.Length);
        }
        else // Deleted Delta data
            await _storage.UploadBytes(indexObjectName, index);

        await PersistDeltaInfo(deltaVersion);
        await _notificationsHub.Notify(new DatasetUpdatedNotification(deltaVersion));
    }

    private async Task PersistDeltaInfo(ulong deltaVersion)
    {
        var previousLatest = DeltaInfo.LatestDeltaVersion;
        var previousEarliest = DeltaInfo.EarliestDeltaVersion;

        DeltaInfo.LatestDeltaVersion = deltaVersion;
        if (DeltaInfo.EarliestDeltaVersion == 0)
        {
            DeltaInfo.EarliestDeltaVersion = deltaVersion;
        }

        try
        {
            await SaveDeltaInfo();
        }
        catch
        {
            DeltaInfo.LatestDeltaVersion = previousLatest;
            DeltaInfo.EarliestDeltaVersion = previousEarliest;
            throw;
        }
    }
    private async Task VerifyArrayUploadIfConfigured(string dataObjectName, int expectedDataLength)
    {
        if (_storageConfig.Value.UploadVerificationMode == UploadVerificationMode.VerifyAll)
        {
            await VerifyDataUpload(dataObjectName, expectedDataLength);
        }
    }

    private async Task VerifyDataUpload(string dataObjectName, long expectedDataLength)
    {
        try
        {
            var actualUploadedDataLength = await _storage.GetObjectContentLength(dataObjectName);
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

    protected void ThrowIfNoLockIsAcquired()
    {
        if (lockService != null && !lockService.HasLock)
            throw new SynchronizationLockException("We can't write to storage as we have to get a lock first");
    }

    public virtual async Task StoreSnapshot(ulong snapshotVersion, byte[] index, Stream data)
    {
        ThrowIfNoLockIsAcquired();
        using var slot = await SnapshotLockAsync();
        var indexObjectName = DatasetHelper.GetObjectName<T>(snapshotVersion, ObjectNameIndexOrDataEnum.Index,
            ObjectNameTypeEnum.Snapshot, _storageConfig.Value);
        var dataObjectName = DatasetHelper.GetObjectName<T>(snapshotVersion, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Snapshot,
            _storageConfig.Value);
        if (data.Length > 0)
        {
            await Task.WhenAll(
                _storage.UploadBytes(indexObjectName, index),
                _storage.UploadFileStream(dataObjectName, data)
            );
            await VerifyStreamUploadIfConfigured(dataObjectName, data.Length);

        }
        else
        {
            await _storage.UploadBytes(indexObjectName, index);
        }

        if (Snapshots.All(s => s.Version != snapshotVersion))
        {
            Snapshots.Add(new SnapshotType
            (
                snapshotVersion,
                DateTimeOffset.UtcNow
            ));

            await SaveSnapshotInfo();
        }

        await _notificationsHub.Notify(
            new SnapshotCreatedNotification(Snapshots.Max(sv => sv.Version)));
    }

    private async Task VerifyStreamUploadIfConfigured(string dataObjectName, long expectedDataLength)
    {
        if (_storageConfig.Value.UploadVerificationMode is UploadVerificationMode.VerifyAll or UploadVerificationMode.VerifyLargeDataOnly)
        {
            await VerifyDataUpload(dataObjectName, expectedDataLength);
        }
    }

    public virtual async Task StoreSnapshot(ulong snapshotVersion, byte[] index, byte[] data)
    {
        ThrowIfNoLockIsAcquired();

        using var slot = await SnapshotLockAsync();
        string indexObjectName = DatasetHelper.GetObjectName<T>(snapshotVersion, ObjectNameIndexOrDataEnum.Index, ObjectNameTypeEnum.Snapshot, _storageConfig.Value);
        string dataObjectName = DatasetHelper.GetObjectName<T>(snapshotVersion, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Snapshot, _storageConfig.Value);

        //Only upload the data file in case it is not empty (which can be the case in case the delta only contains deletes)
        if (data.Length > 0)
        {
            await Task.WhenAll(
                _storage.UploadBytes(indexObjectName, index),
                _storage.UploadBytes(dataObjectName, data)
            );
            await VerifyArrayUploadIfConfigured(dataObjectName, data.Length);
        }
        else
        {
            await _storage.UploadBytes(indexObjectName, index);
        }

        if (Snapshots.All((s) => s.Version != snapshotVersion))
        {
            Snapshots.Add(new SnapshotType
            (
                snapshotVersion,
                DateTimeOffset.UtcNow
            ));

            await SaveSnapshotInfo();
        }

        await _notificationsHub.Notify(
            new SnapshotCreatedNotification(Snapshots.Max(sv => sv.Version)));
    }

    public async Task StoreProcessedVersion(string who, ulong version)
    {
        ThrowIfNoLockIsAcquired();
        await EnsureProviderIsInitialized();

        using (await _processedVersionsSemaphore.LockAsync())
        {
            if (_processedVersionsInfo.ProcessedVersions.ContainsKey(who))
                _processedVersionsInfo.ProcessedVersions[who] = version;
            else
                _processedVersionsInfo.ProcessedVersions.TryAdd(who, version);

            await SaveProcessedVersionsInfo();
            await _notificationsHub.Notify(new VersionProcessedNotification(who, version));
        }
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
        return DatasetHelper.GetObjectNamePrefix<T>(_storageConfig.Value) + "ProcessedVersions.pb";
    }

    private string GetDeltaInfoFilename()
    {
        return DatasetHelper.GetObjectNamePrefix<T>(_storageConfig.Value) + "DeltaInfo.pb";
    }

    private string GetSnapshotInfoFilename()
    {
        return DatasetHelper.GetObjectNamePrefix<T>(_storageConfig.Value) + "SnapshotInfo.pb";
    }

    private async Task SaveDeltaInfo()
    {

        if (DeltaInfo.CalculateSize() == 0)
            return;

        var filename = GetDeltaInfoFilename();
        var data = DeltaInfo.ToByteArray();
        await _storage.UploadBytes(filename, data);
    }

    private async Task SaveSnapshotInfo()
    {
        if (Snapshots.Count == 0)
            return;

        var filename = GetSnapshotInfoFilename();
        var snapshotInfo = new SnapshotInfo();
        snapshotInfo.SnapshotVersions.AddRange(Snapshots.Select(s => new SnapshotInfo.Types.snapshotVersion() { Timestamp = s.DateTime?.ToTimestamp(), Version = s.Version }));
        var data = snapshotInfo.ToByteArray();
        await _storage.UploadBytes(filename, data);
    }

    private async Task SaveProcessedVersionsInfo()
    {
        if (_processedVersionsInfo.CalculateSize() == 0)
            return;

        var filename = GetProcessedVersionsFilename();
        var data = _processedVersionsInfo.ToByteArray();
        await _storage.UploadBytes(filename, data);

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



    private async Task<TMessage> DownloadAndParseProtobufFileAsync<TMessage>(string filename, MessageParser<TMessage> parser)
        where TMessage : IMessage<TMessage>
    {
        logger.LogInformation($"Downloading and parsing file: {filename}");

        var fileBytes = await _storage.DownloadBytes(filename);
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

    protected override async Task Initialize(CancellationToken cancellationToken)
    {
        await base.Initialize(cancellationToken);
        if (_storage == null)
            throw new InvalidOperationException("Storage Provider was not initialized");
        await TryLoadProcessedVersionsInfo();
        await _notificationsHub.Notify(new StorageProviderReadinessNotifcation(ServiceStatus.Ready));
    }

    protected async Task<IDisposable> DeltaLockAsync(CancellationToken cancellationToken = default)
    {
        await EnsureProviderIsInitialized();

        return await _deltaSemaphore.LockAsync(cancellationToken);
    }

    protected async Task<IDisposable> SnapshotLockAsync(CancellationToken cancellationToken = default)
    {
        await EnsureProviderIsInitialized();

        return await _snapshotSemaphore.LockAsync(cancellationToken);
    }

}
