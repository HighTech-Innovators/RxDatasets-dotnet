using System.Diagnostics;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Notifications;
using RxDatasets.StorageProviders;
using System.Runtime.InteropServices;

namespace RxDatasets.StorageProvider.GoogleCloud;

public class HybridBigtableStorageProvider<T> : ReadOnlyHybridBigtableStorageProvider<T>, IStorageProvider<T>
{
    private readonly SemaphoreSlim _deltaSemaphore = new(1);
    private readonly SemaphoreSlim _snapshotSemaphore = new(1);
    private readonly SemaphoreSlim _processedVersionsSemaphore = new(1);
    private readonly ILogger<HybridBigtableStorageProvider<T>> _logger;
    private readonly IOptions<StorageConfig> _storageConfig;
    private readonly IDiskStorage<T> _blobStorage;
    private readonly IVersionedArtifactRowStore _rowStore;
    private readonly INotificationsHub<T> _notificationsHub;
    private readonly DistributedLockService<T>? _lockService;
    private readonly bool _startIfInitFails;
    private ProcessedVersionsInfo _processedVersionsInfo = new();
    private IDisposable? _distributedLockSubscription;

    public HybridBigtableStorageProvider(
        ILogger<HybridBigtableStorageProvider<T>> logger,
        IOptions<StorageConfig> storageConfig,
        IDiskStorage<T> blobStorage,
        IVersionedArtifactRowStore rowStore,
        INotificationsHub<T> notificationsHub,
        IStorageReader<T> storageReader,
        bool startIfInitFails = true,
        DistributedLockService<T>? lockService = null)
        : base(
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ReadOnlyHybridBigtableStorageProvider<T>>.Instance,
            storageReader,
            notificationsHub,
            startIfInitFails)
    {
        _logger = logger;
        _storageConfig = storageConfig;
        _blobStorage = blobStorage;
        _rowStore = rowStore;
        _notificationsHub = notificationsHub;
        _lockService = lockService;
        _startIfInitFails = startIfInitFails;
    }

    private bool Log => _storageConfig.Value.LogStorageAccess;

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        if (_lockService is null)
        {
            InitTask = _startIfInitFails ? InitLoop(cancellationToken) : InitializeAndNotifyReady(cancellationToken);
            await InitTask;
        }

        _distributedLockSubscription = _notificationsHub.Subscribe<DistributedLockNotification>(async notification =>
        {
            if (notification.LockStatus == DistirbutedLockStatus.Acquried)
            {
                InitTask = InitLoop(cancellationToken);
                await InitTask;
                return;
            }

            InitTask = null;
            await NotifyReadiness(ServiceStatus.NotReady);
        });
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _distributedLockSubscription?.Dispose();
        await base.StopAsync(cancellationToken);
    }

    public async Task DeleteOlderThan(ulong version, bool keepAllDeltas)
    {
        ThrowIfNoLockIsAcquired();

        using var slotSnapshot = await SnapshotLock();
        if (Snapshots.All(x => x.Version != version))
        {
            throw new InvalidOperationException($"Snapshot version {version} does not exist");
        }

        foreach (var snapshot in Snapshots.Where(k => k.Version < version).ToList())
        {
            await DeleteArtifact(snapshot.Version, ObjectNameTypeEnum.Snapshot);
            Snapshots.Remove(snapshot);
        }

        if (!keepAllDeltas)
        {
            using var slotDelta = await DeltaLock();
            for (var deltaToDelete = DeltaInfo.EarliestDeltaVersion; deltaToDelete <= version; deltaToDelete++)
            {
                await DeleteArtifact(deltaToDelete, ObjectNameTypeEnum.Delta);
            }

            DeltaInfo.EarliestDeltaVersion = version + 1;
        }
    }

    public async Task StoreSnapshot(ulong snapshotVersion, byte[] index, Stream data)
    {
        ThrowIfNoLockIsAcquired();

        using var slot = await SnapshotLock();
        if (data.Length > 0)
        {
            await WriteArtifact(ObjectNameTypeEnum.Snapshot, snapshotVersion, index, data);
            await VerifyStreamUploadIfConfigured(ObjectNameTypeEnum.Snapshot, snapshotVersion, data.Length);
        }
        else
        {
            await WriteArtifact(ObjectNameTypeEnum.Snapshot, snapshotVersion, index, []);
        }

        if (Snapshots.All(s => s.Version != snapshotVersion))
        {
            Snapshots.Add(new SnapshotType(snapshotVersion, DateTimeOffset.UtcNow));
        }

        await _notificationsHub.Notify(new SnapshotCreatedNotification(Snapshots.Max(sv => sv.Version)));
    }

    public async Task StoreSnapshot(ulong snapshotVersion, byte[] index, byte[] data)
    {
        ThrowIfNoLockIsAcquired();

        using var slot = await SnapshotLock();
        await WriteArtifact(ObjectNameTypeEnum.Snapshot, snapshotVersion, index, data);
        if (data.Length > 0)
        {
            await VerifyArrayUploadIfConfigured(ObjectNameTypeEnum.Snapshot, snapshotVersion, data.Length);
        }

        if (Snapshots.All(s => s.Version != snapshotVersion))
        {
            Snapshots.Add(new SnapshotType(snapshotVersion, DateTimeOffset.UtcNow));
        }

        await _notificationsHub.Notify(new SnapshotCreatedNotification(Snapshots.Max(sv => sv.Version)));
    }

    public async Task StoreDelta(ulong deltaVersion, byte[] index, byte[] data)
    {
        ThrowIfNoLockIsAcquired();

        using var slot = await DeltaLock();
        await WriteArtifact(ObjectNameTypeEnum.Delta, deltaVersion, index, data);
        if (data.Length > 0)
        {
            await VerifyArrayUploadIfConfigured(ObjectNameTypeEnum.Delta, deltaVersion, data.Length);
        }

        await PersistDeltaInfo(deltaVersion);
        await _notificationsHub.Notify(new DatasetUpdatedNotification(deltaVersion));
    }

    public async Task StoreDelta(ulong deltaVersion, byte[] index, Stream data)
    {
        ThrowIfNoLockIsAcquired();

        using var slot = await DeltaLock();
        if (data.Length > 0)
        {
            await WriteArtifact(ObjectNameTypeEnum.Delta, deltaVersion, index, data);
            await VerifyStreamUploadIfConfigured(ObjectNameTypeEnum.Delta, deltaVersion, data.Length);
        }
        else
        {
            await WriteArtifact(ObjectNameTypeEnum.Delta, deltaVersion, index, []);
        }

        await PersistDeltaInfo(deltaVersion);
        await _notificationsHub.Notify(new DatasetUpdatedNotification(deltaVersion));
    }

    public async Task StoreProcessedVersion(string who, ulong version)
    {
        ThrowIfNoLockIsAcquired();
        await EnsureProviderIsInitialized();

        await _processedVersionsSemaphore.WaitAsync().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        try
        {
            if (_processedVersionsInfo.ProcessedVersions.ContainsKey(who))
            {
                _processedVersionsInfo.ProcessedVersions[who] = version;
            }
            else
            {
                _processedVersionsInfo.ProcessedVersions.TryAdd(who, version);
            }

            var sw = Log ? Stopwatch.StartNew() : null;
            await _rowStore.PutProcessedVersion(GetDatasetPrefix(), who, version);
            if (Log)
            {
                _logger.LogInformation("Wrote ProcessedVersion ({Who}={Version}) to Bigtable in {ElapsedMs}ms", who, version, sw!.ElapsedMilliseconds);
            }
            await _notificationsHub.Notify(new VersionProcessedNotification(who, version));
        }
        finally
        {
            _processedVersionsSemaphore.Release();
        }
    }

    public async Task<ulong> GetProcessedVersion(string who)
    {
        await EnsureProviderIsInitialized();
        return _processedVersionsInfo.ProcessedVersions.TryGetValue(who, out var version) ? version : 0ul;
    }

    public async Task<ulong> GetLowestProcessedVersion()
    {
        await EnsureProviderIsInitialized();
        return _processedVersionsInfo.ProcessedVersions.Count > 0 ? _processedVersionsInfo.ProcessedVersions.Min(v => v.Value) : 0ul;
    }

    protected override async Task Initialize(CancellationToken cancellationToken)
    {
        await base.Initialize(cancellationToken);
        await TryLoadProcessedVersionsInfo();
    }

    private async Task InitializeAndNotifyReady(CancellationToken cancellationToken)
    {
        await Initialize(cancellationToken);
        await NotifyReadiness(ServiceStatus.Ready);
    }

    private async Task InitLoop(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Initialize(cancellationToken);
                await NotifyReadiness(ServiceStatus.Ready);
                break;
            }
            catch (SynchronizationLockException ex)
            {
                _logger.LogError(ex, "Failed to initialize hybrid storage provider because no lock was available");
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize hybrid storage provider");
                await Task.Delay(5 * 1000, cancellationToken);
            }
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
            await _rowStore.GetArtifact(GetDatasetPrefix(), ObjectNameTypeEnum.Delta, deltaVersion, ArtifactReadScope.Meta);
        }
        catch
        {
            DeltaInfo.LatestDeltaVersion = previousLatest;
            DeltaInfo.EarliestDeltaVersion = previousEarliest;
            throw;
        }
    }

    private async Task VerifyArrayUploadIfConfigured(ObjectNameTypeEnum type, ulong version, int expectedDataLength)
    {
        if (_storageConfig.Value.UploadVerificationMode == UploadVerificationMode.VerifyAll)
        {
            await VerifyArtifactUpload(type, version, expectedDataLength);
        }
    }

    private async Task VerifyStreamUploadIfConfigured(ObjectNameTypeEnum type, ulong version, long expectedDataLength)
    {
        if (_storageConfig.Value.UploadVerificationMode is UploadVerificationMode.VerifyAll or UploadVerificationMode.VerifyLargeDataOnly)
        {
            await VerifyArtifactUpload(type, version, expectedDataLength);
        }
    }

    private async Task VerifyArtifactUpload(ObjectNameTypeEnum type, ulong version, long expectedDataLength)
    {
        var artifact = await _rowStore.GetArtifact(GetDatasetPrefix(), type, version);
        if (artifact == null)
        {
            throw new ExternalException($"Expected a hybrid artifact row for {type} {version}, but none was found.");
        }

        if (artifact.DataLength != expectedDataLength)
        {
            throw new ExternalException(
                $"{type} {version}: Expected uploaded data length to be {expectedDataLength}, but actual length is {artifact.DataLength}");
        }

        if (artifact.InlineData != null && artifact.InlineData.LongLength != expectedDataLength)
        {
            throw new ExternalException(
                $"{type} {version}: Expected inline data length to be {expectedDataLength}, but actual length is {artifact.InlineData.LongLength}");
        }

        if (!string.IsNullOrEmpty(artifact.DataFallbackObjectKey))
        {
            var actualUploadedDataLength = await _blobStorage.GetObjectContentLength(artifact.DataFallbackObjectKey);
            if (actualUploadedDataLength != expectedDataLength)
            {
                throw new ExternalException(
                    $"{type} {version}: Expected fallback uploaded data length to be {expectedDataLength}, but actual length is {actualUploadedDataLength}");
            }
        }
    }

    private void ThrowIfNoLockIsAcquired()
    {
        if (_lockService != null && !_lockService.HasLock)
        {
            throw new SynchronizationLockException("We can't write to storage as we have to get a lock first");
        }
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

    private async Task DeleteArtifact(ulong version, ObjectNameTypeEnum type)
    {
        var sw = Log ? Stopwatch.StartNew() : null;
        var artifact = await _rowStore.GetArtifact(GetDatasetPrefix(), type, version, ArtifactReadScope.Meta);
        if (artifact != null)
        {
            await DeleteFallbackObject(artifact.IndexFallbackObjectKey);
            await DeleteFallbackObject(artifact.DataFallbackObjectKey);
            await _rowStore.DeleteArtifact(GetDatasetPrefix(), type, version);
            if (Log)
            {
                _logger.LogInformation("Deleted {Type} v{Version} from Bigtable in {ElapsedMs}ms", type, version, sw!.ElapsedMilliseconds);
            }
            return;
        }

        var folderKey = DatasetHelper.GetObjectFolder<T>(version, type, _storageConfig.Value);
        await _blobStorage.DeleteFolder(folderKey + '/');
        if (Log)
        {
            _logger.LogInformation("Deleted {Type} v{Version} from GCS (legacy) in {ElapsedMs}ms", type, version, sw!.ElapsedMilliseconds);
        }
    }

    private async Task DeleteFallbackObject(string? objectKey)
    {
        if (string.IsNullOrEmpty(objectKey))
        {
            return;
        }

        try
        {
            await _blobStorage.DeleteFile(objectKey);
        }
        catch (FileNotFoundException)
        {
        }
    }

    private async Task WriteArtifact(ObjectNameTypeEnum type, ulong version, byte[] index, byte[] data)
    {
        var sw = Log ? Stopwatch.StartNew() : null;
        var plan = _rowStore.CreateWritePlan(index.Length, data.LongLength);
        var indexFallbackObjectKey = plan.StoreIndexInline ? null : GetObjectName(version, ObjectNameIndexOrDataEnum.Index, type);
        var dataFallbackObjectKey = data.Length == 0 || plan.StoreDataInline ? null : GetObjectName(version, ObjectNameIndexOrDataEnum.Data, type);

        if (indexFallbackObjectKey != null)
        {
            await _blobStorage.UploadBytes(indexFallbackObjectKey, index);
            if (Log)
            {
                _logger.LogInformation("Wrote {Type} v{Version} Index ({Length} bytes) to GCS in {ElapsedMs}ms", type, version, index.Length, sw!.ElapsedMilliseconds);
            }
        }

        if (dataFallbackObjectKey != null)
        {
            await _blobStorage.UploadBytes(dataFallbackObjectKey, data);
            if (Log)
            {
                _logger.LogInformation("Wrote {Type} v{Version} Data ({Length} bytes) to GCS in {ElapsedMs}ms", type, version, data.Length, sw!.ElapsedMilliseconds);
            }
        }

        await _rowStore.UpsertArtifact(GetDatasetPrefix(), new HybridArtifactRecord(
            type,
            version,
            DateTimeOffset.UtcNow,
            plan.StoreIndexInline ? index : null,
            data.Length == 0 ? null : plan.StoreDataInline ? data : null,
            indexFallbackObjectKey,
            dataFallbackObjectKey,
            index.LongLength,
            data.LongLength));

        if (Log)
        {
            _logger.LogInformation("Wrote {Type} v{Version} (index: {IndexLen} bytes, data: {DataLen} bytes, inline: {Inline}) to Bigtable in {ElapsedMs}ms",
                type, version, index.Length, data.Length, plan.StoreDataInline, sw!.ElapsedMilliseconds);
        }
    }

    private async Task WriteArtifact(ObjectNameTypeEnum type, ulong version, byte[] index, Stream data)
    {
        var sw = Log ? Stopwatch.StartNew() : null;
        if (data.CanSeek)
        {
            data.Position = 0;
        }

        var plan = _rowStore.CreateWritePlan(index.Length, data.Length);
        var indexFallbackObjectKey = plan.StoreIndexInline ? null : GetObjectName(version, ObjectNameIndexOrDataEnum.Index, type);
        var dataFallbackObjectKey = data.Length == 0 || plan.StoreDataInline ? null : GetObjectName(version, ObjectNameIndexOrDataEnum.Data, type);

        if (indexFallbackObjectKey != null)
        {
            await _blobStorage.UploadBytes(indexFallbackObjectKey, index);
            if (Log)
            {
                _logger.LogInformation("Wrote {Type} v{Version} Index ({Length} bytes) to GCS in {ElapsedMs}ms", type, version, index.Length, sw!.ElapsedMilliseconds);
            }
        }

        byte[]? inlineData = null;
        if (data.Length > 0)
        {
            if (plan.StoreDataInline)
            {
                inlineData = await ReadAllBytes(data);
            }
            else
            {
                if (data.CanSeek)
                {
                    data.Position = 0;
                }

                await _blobStorage.UploadFileStream(dataFallbackObjectKey!, data);
                if (Log)
                {
                    _logger.LogInformation("Wrote {Type} v{Version} Data ({Length} bytes) to GCS (stream) in {ElapsedMs}ms", type, version, data.Length, sw!.ElapsedMilliseconds);
                }
            }
        }

        await _rowStore.UpsertArtifact(GetDatasetPrefix(), new HybridArtifactRecord(
            type,
            version,
            DateTimeOffset.UtcNow,
            plan.StoreIndexInline ? index : null,
            inlineData,
            indexFallbackObjectKey,
            dataFallbackObjectKey,
            index.LongLength,
            data.Length));

        if (Log)
        {
            _logger.LogInformation("Wrote {Type} v{Version} (index: {IndexLen} bytes, data: {DataLen} bytes, inline: {Inline}) to Bigtable in {ElapsedMs}ms",
                type, version, index.Length, data.Length, plan.StoreDataInline, sw!.ElapsedMilliseconds);
        }
    }

    private async Task<byte[]> ReadAllBytes(Stream stream)
    {
        if (stream.CanSeek)
        {
            stream.Position = 0;
        }

        using var buffer = new MemoryStream();
        await stream.CopyToAsync(buffer);
        return buffer.ToArray();
    }

    private async Task TryLoadProcessedVersionsInfo()
    {
        var processedVersions = await _rowStore.GetProcessedVersions(GetDatasetPrefix());
        if (processedVersions.Count > 0)
        {
            _processedVersionsInfo = new ProcessedVersionsInfo();
            foreach (var (who, version) in processedVersions)
            {
                _processedVersionsInfo.ProcessedVersions[who] = version;
            }

            return;
        }

        try
        {
            var fileBytes = await _blobStorage.DownloadBytes(GetProcessedVersionsFilename());
            _processedVersionsInfo = ProcessedVersionsInfo.Parser.ParseFrom(fileBytes.Span);
        }
        catch (FileNotFoundException)
        {
            _processedVersionsInfo = new ProcessedVersionsInfo();
        }
    }

    private string GetDatasetPrefix()
    {
        return DatasetHelper.GetObjectNamePrefix<T>(_storageConfig.Value);
    }

    private string GetProcessedVersionsFilename()
    {
        return GetDatasetPrefix() + "ProcessedVersions.pb";
    }

    private string GetObjectName(ulong version, ObjectNameIndexOrDataEnum indexOrData, ObjectNameTypeEnum type)
    {
        return DatasetHelper.GetObjectName<T>(version, indexOrData, type, _storageConfig.Value);
    }
}
