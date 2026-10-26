using System.Data;
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
    : ReadOnlyObjectStorageProvider<T>(logger, storageConfig, storage, storageReader, notificationsHub, startIfInitFails), IStorageProvider<T>, IHostedService
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
    private CancellationToken _stoppingToken;

    // How old the data of an uncommitted delta must be before a writer treats it as left by a crash and replaces it.
    // Measured with this host's clock against the storage's timestamp, so it also needs clocks within this of each other.
    // ponytail: a writer paused between its data and index uploads for longer than this, that resumes just as the next
    // writer replaces its data, can still commit its index over the other's data. A per-attempt data name in the index
    // would close that; it is a format change.
    private static readonly TimeSpan OrphanedDataAge = TimeSpan.FromMinutes(1);

    public new async Task StartAsync(CancellationToken cancellationToken)
    {
        _stoppingToken = cancellationToken;
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

    public async Task DeleteOlderThan(ulong version, bool keepAllDeltas)
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

    public async Task StoreDelta(ulong deltaVersion, byte[] index, byte[] data)
    {
        ThrowIfNoLockIsAcquired();

        using var slot = await DeltaLockAsync();
        var indexObjectName = DatasetHelper.GetObjectName<T>(deltaVersion, ObjectNameIndexOrDataEnum.Index, ObjectNameTypeEnum.Delta,
            _storageConfig.Value);
        var dataObjectName = DatasetHelper.GetObjectName<T>(deltaVersion, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Delta,
            _storageConfig.Value);

        if (_storage is IConditionalDiskStorage<T> conditional)
        {
            await CommitDelta(conditional, deltaVersion, indexObjectName, index, dataObjectName,
                data.Length == 0 ? null : async () => await conditional.TryUploadBytes(dataObjectName, data, null) != null,
                () => VerifyArrayUploadIfConfigured(dataObjectName, data.Length));
            return;
        }

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
    }
    public async Task StoreDelta(ulong deltaVersion, byte[] index, Stream data)
    {
        ThrowIfNoLockIsAcquired();

        using var slot = await DeltaLockAsync();
        var indexObjectName = DatasetHelper.GetObjectName<T>(deltaVersion, ObjectNameIndexOrDataEnum.Index, ObjectNameTypeEnum.Delta,
            _storageConfig.Value);
        var dataObjectName = DatasetHelper.GetObjectName<T>(deltaVersion, ObjectNameIndexOrDataEnum.Data, ObjectNameTypeEnum.Delta,
            _storageConfig.Value);

        if (_storage is IConditionalDiskStorage<T> conditional)
        {
            await CommitDelta(conditional, deltaVersion, indexObjectName, index, dataObjectName,
                data.Length == 0 ? null : () =>
                {
                    data.Position = 0;
                    return conditional.TryCreateFromStream(dataObjectName, data);
                },
                () => VerifyStreamUploadIfConfigured(dataObjectName, data.Length));
            return;
        }

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
    }

    // The index is the commit: readers find a delta by its index (ObjectStorageReader probes past DeltaInfo.pb, which is
    // only a hint), so the data goes first and both are created only if absent. A writer that is behind, such as one
    // that lost the lock without noticing yet, then fails instead of overwriting a stored delta (#79, item 4).
    private async Task CommitDelta(IConditionalDiskStorage<T> conditional, ulong deltaVersion, string indexObjectName,
        byte[] index, string dataObjectName, Func<Task<bool>>? createData, Func<Task> verifyData)
    {
        var previousIsStored = PreviousVersionIsStored(deltaVersion);
        if (createData != null)
        {
            if (!await createData())
            {
                if (await ObjectExists(indexObjectName))
                    throw Conflict(deltaVersion);
                // Not committed yet: another writer is about to, so this writer isn't behind and needs no reload
                if (!await IsOldData(dataObjectName))
                {
                    throw new DBConcurrencyException(
                        $"Delta {deltaVersion} of {typeof(T).Name} is being stored by another writer");
                }

                logger.LogWarning("Replacing the data of delta {Version} of {Type}, which was never committed",
                    deltaVersion, typeof(T).Name);
                await _storage.DeleteFile(dataObjectName);
                if (!await createData())
                    throw Conflict(deltaVersion);
            }

            await verifyData();
        }

        // A writer far enough behind that cleaning removed its previous version would otherwise re-create a delta
        // below the oldest one, which nobody reads
        if (!await previousIsStored)
        {
            if (createData != null)
                await _storage.DeleteFile(dataObjectName);
            throw Conflict(deltaVersion);
        }

        if (await conditional.TryUploadBytes(indexObjectName, index, null) == null
            && !await StoredIndexIs(indexObjectName, index))
        {
            // Data we created is ours alone: a winner with data would have made our create fail
            if (createData != null)
                await _storage.DeleteFile(dataObjectName);
            throw Conflict(deltaVersion);
        }

        DeltaInfo.LatestDeltaVersion = deltaVersion;
        if (DeltaInfo.EarliestDeltaVersion == 0)
            DeltaInfo.EarliestDeltaVersion = deltaVersion;
        try
        {
            await SaveDeltaInfo();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Delta {Version} of {Type} is stored, but DeltaInfo.pb was not updated", deltaVersion,
                typeof(T).Name);
        }
    }

    private async Task<bool> PreviousVersionIsStored(ulong deltaVersion)
    {
        if (deltaVersion <= 1)
            return true;

        // After cleaning without KeepAllDeltas, the snapshot of the previous version stands in for its delta
        return await ObjectExists(DatasetHelper.GetObjectName<T>(deltaVersion - 1, ObjectNameIndexOrDataEnum.Index,
                   ObjectNameTypeEnum.Delta, _storageConfig.Value))
               || await ObjectExists(DatasetHelper.GetObjectName<T>(deltaVersion - 1, ObjectNameIndexOrDataEnum.Index,
                   ObjectNameTypeEnum.Snapshot, _storageConfig.Value));
    }

    private async Task<bool> IsOldData(string dataObjectName)
    {
        var data = (await _storage.ListObjects(dataObjectName)).Where(o => o.ObjectKey == dataObjectName).ToList();
        return data.Count == 1 && DateTimeOffset.UtcNow - data[0].DateTime > OrphanedDataAge;
    }

    // A create whose response was lost (the SDK retried a write that had landed) finds its own index
    private async Task<bool> StoredIndexIs(string indexObjectName, byte[] index)
    {
        try
        {
            return (await _storage.DownloadBytes(indexObjectName)).Span.SequenceEqual(index);
        }
        catch (FileNotFoundException)
        {
            return false;
        }
    }

    private async Task<bool> ObjectExists(string objectName)
    {
        try
        {
            await _storage.GetObjectContentLength(objectName);
            return true;
        }
        catch (FileNotFoundException)
        {
            return false;
        }
    }

    // Throws to the caller, whose write is not stored; meanwhile the writer and reader reload, so they pick up the
    // versions the other writer stored
    private DBConcurrencyException Conflict(ulong deltaVersion)
    {
        _ = Task.Run(ReloadAfterConflict);
        return new DBConcurrencyException(
            $"Delta {deltaVersion} of {typeof(T).Name} was stored by another writer first; the dataset is being reloaded");
    }

    private async Task ReloadAfterConflict()
    {
        try
        {
            await _notificationsHub.Notify(new StorageProviderReadinessNotifcation(ServiceStatus.NotReady));
            if (lockService is { HasLock: false })
                return; // reloads when the lock is acquired again

            _initTask = InitLoop(_stoppingToken);
            await _initTask;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to reload {Type} after a version conflict", typeof(T).Name);
        }
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

    private void ThrowIfNoLockIsAcquired()
    {
        if (lockService != null && !lockService.HasLock)
            throw new SynchronizationLockException("We can't write to storage as we have to get a lock first");
    }

    public async Task StoreSnapshot(ulong snapshotVersion, byte[] index, Stream data)
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

    public async Task StoreSnapshot(ulong snapshotVersion, byte[] index, byte[] data)
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

    private async Task<IDisposable> DeltaLockAsync(CancellationToken cancellationToken = default)
    {
        await EnsureProviderIsInitialized();

        return await _deltaSemaphore.LockAsync(cancellationToken);
    }

    private async Task<IDisposable> SnapshotLockAsync(CancellationToken cancellationToken = default)
    {
        await EnsureProviderIsInitialized();

        return await _snapshotSemaphore.LockAsync(cancellationToken);
    }

}
