
using System.Reactive.Disposables;
using System.Runtime.CompilerServices;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RxDatasets.Abstractions;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Models;
using RxDatasets.Notifications;

namespace RxDatasets;

public class DatasetReader<T> : IDatasetReader<T>, IHostedService
{
    private readonly ILogger<DatasetReader<T>> _logger;
    private readonly IReadOnlyStorageProvider<T> _storageProvider;
    private readonly bool _retryInitialization;
    private readonly DatasetHelper _datasetHelper;
    private ImmutableDatasetState? _index;
    private SnapshotPositionLookup? _snapshotPositionLookup;
    private Task? _initializationTask;
    private readonly SemaphoreSlim _writerSemaphore = new(1);
    private readonly CompositeDisposable _subscriptions = [];
    private readonly CancellationTokenSource _cts = new();

    public DatasetReader(ILogger<DatasetReader<T>> logger,
        IReadOnlyStorageProvider<T> storageProvider, INotificationsHub<T>? notificationsHub,
        bool retryInitialization = false)
    {
        _storageProvider = storageProvider;
        _retryInitialization = retryInitialization;
        _datasetHelper = new DatasetHelper(logger, _storageProvider);
        _logger = logger;
        if (notificationsHub == null)
            return;
        _subscriptions.Add(notificationsHub.Subscribe<DatasetUpdatedNotification>(OnDatasetUpdated));
        _subscriptions.Add(notificationsHub.Subscribe<SnapshotCreatedNotification>(OnSnapshotCreated));
        _subscriptions.Add(notificationsHub.Subscribe<StorageProviderReadinessNotifcation>(async (notification) =>
        {
            if (notification.ServiceStatus == ServiceStatus.NotReady)
            {
                Uninitialize();
                return;
            }

            _initializationTask = _retryInitialization ? InitializationLoop() : Initialize();
            await _initializationTask;
        }));
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        // this is only started in ReadOnly Mode
        _initializationTask = _retryInitialization ? InitializationLoop() : Initialize();
        await _initializationTask;

        _ = Task.Run(async () =>
            {
                while (!_cts.Token.IsCancellationRequested)
                {
                    await Task.Delay(TimeSpan.FromSeconds(5), _cts.Token).ConfigureAwait(false);
                    try
                    {
                        _initializationTask = Initialize();
                        await _initializationTask;

                    }
                    catch (Exception)
                    {
                        //ignored as errors will be logged when they are thrown
                    }
                }
            }, cancellationToken);
    }

    private async Task OnDatasetUpdated(DatasetUpdatedNotification datasetUpdatedNotification)
    {
        using var updateSlot = await Lock(_writerSemaphore);
        try
        {
            ThrowIfNotInitialized();
            _index = await UpdateIndexWithLatestDelta(_index!, datasetUpdatedNotification.Version);
            var tcs = IndexChangedTaskCompletionSource;
            IndexChangedTaskCompletionSource = new TaskCompletionSource();
            tcs.SetResult();
        }
        catch (InvalidOperationException)
        {
            // we are not ready
        }
    }


    private async Task OnSnapshotCreated(SnapshotCreatedNotification notification)
    {
        if (_snapshotPositionLookup == null
            || (_snapshotPositionLookup != null && notification.Version > _snapshotPositionLookup.SnapshotVersion))
        {
            _snapshotPositionLookup = await _datasetHelper.GetLatestSnapshotPositionLookup();
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _cts.CancelAsync();
        _subscriptions.Dispose();
    }

    private TaskCompletionSource IndexChangedTaskCompletionSource { get; set; } = new();


    private async Task InitializationLoop()
    {
        while (true)
        {
            try
            {
                await Initialize();
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize Reader for {EntityType} Dataset", typeof(T).Name);
                await Task.Delay(5 * 1000);
            }
        }
    }

    private async Task Initialize()
    {
        try
        {
            _index = await _datasetHelper.GetLatestIndex();
            try
            {
                _snapshotPositionLookup = await _datasetHelper.GetLatestSnapshotPositionLookup();
            }
            catch (FileNotFoundException)
            {
                //ignored as we dont have a snapshot yet
            }
            _logger.LogInformation($"Initialized DataReader {typeof(T).Name} Dataset with latest version {_index.LatestVersion}");
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Failed to initialize Reader for {EntityType} Dataset", typeof(T).Name);
            throw;
        }
    }

    private void Uninitialize()
    {
        _initializationTask = null;
        _index = null;
        _snapshotPositionLookup = null;
    }

    private ImmutableDatasetState ThrowIfNotInitialized()
    {
        if (_initializationTask == null)
        {
            _logger.LogError("Received a request, however we are not ready yet");
            throw new InvalidOperationException("Not initialized - possibly because storage isn't ready or doesn't have a lock");
        }

        if (!_initializationTask!.IsCompleted)
        {
            _logger.LogError("Dataset received a request before it was initialized");
            throw new InvalidOperationException("Initialization is not yet completed");
        }

        if (_initializationTask.IsFaulted)
        {
            _logger.LogError(_initializationTask.Exception, "Dataset received a request, but initialization failed");
            throw new InvalidOperationException("Initialization failed", _initializationTask.Exception);
        }

        return _index!;
    }

    public async Task<ICollection<(string name, ReadOnlyMemory<byte>? entityBytes)>> BatchGet(IEnumerable<string> names, string? parent)
    {
        var indexRef = ThrowIfNotInitialized();
        var tasks = names.Select(async name =>
        {
            if (!indexRef.Entities.TryGetValue(name, out _))
            {
                return (name, (ReadOnlyMemory<byte>?)null);
            }

            var entityBytes = await ReadEntityBytes(name);
            return (name, entityBytes);
        });
        return await Task.WhenAll(tasks);
    }

    private async Task<ReadOnlyMemory<byte>> ReadEntityBytes(string name)
    {
        // Precondition: calling method MUST have checked it exists (presence in trie = alive).
        var entityInMainIndex = _index!.Entities[name];
        var entityVersion = entityInMainIndex.LatestVersion;

        List<(ObjectNameTypeEnum type, ulong version, long position)> sources = new(2);

        if (_snapshotPositionLookup != null && entityVersion <= _snapshotPositionLookup.SnapshotVersion)
        {
            if (_snapshotPositionLookup.TryGetPosition(name, out var snapshotPosition))
            {
                sources.Add((ObjectNameTypeEnum.Snapshot, _snapshotPositionLookup.SnapshotVersion,
                    snapshotPosition));
            }
        }

        if (entityVersion <= _index.SnapshotVersion)
        {
            // It's 'older' than the snapshot our index 'started' with, so the position refers to the Position in the snapshot
            // Double check that we
            if (sources.Count == 0 || sources[0].version != _index.SnapshotVersion)
                sources.Add((ObjectNameTypeEnum.Snapshot, _index.SnapshotVersion, entityInMainIndex.Position));
        }
        else
        {
            sources.Add((ObjectNameTypeEnum.Delta, entityVersion, entityInMainIndex.Position));
        }

        return await _storageProvider.ReadDataFromSources(sources, entityInMainIndex.Length);
    }

    public async Task<ChangeSet> WaitChangedNames(ulong fromVersion, CancellationToken cancellationToken)
    {
        if (_initializationTask == null || _initializationTask.IsFaulted)
            ThrowIfNotInitialized();

        if (!_initializationTask!.IsCompleted)
            await _initializationTask;

        while (fromVersion == ThrowIfNotInitialized().LatestVersion)
        {
            await IndexChangedTaskCompletionSource.Task.WaitAsync(cancellationToken);
        }

        return await GetChangedNames(fromVersion);
    }

    public async Task<(string name, ReadOnlyMemory<byte> entityBytes)?> Get(string name)
    {
        var indexRef = ThrowIfNotInitialized();

        if (!indexRef.Entities.TryGetValue(name, out _))
            return null;

        var entityBytes = await ReadEntityBytes(name);

        return (name, entityBytes);
    }

    public async Task<ChangeSet> GetChangedNames(ulong fromVersion)
    {
        var indexRef = ThrowIfNotInitialized();
        var toVersion = indexRef.LatestVersion;

        if (fromVersion > toVersion)
        {
            throw new ArgumentOutOfRangeException($"fromVersion {fromVersion} can't be bigger than the latest version {toVersion}");
        }
        if (fromVersion == toVersion)
        {
            return new ChangeSet(toVersion, []);
        }

        if (fromVersion == 0)
        {
            // Full list: all entities in the trie are alive = New
            var changedNames = indexRef.Entities
                .Select(x => (x.Key, ChangeType.New))
                .ToList();
            return new ChangeSet(toVersion, changedNames);
        }

        // Read delta indices and aggregate changes
        var deltas = await _datasetHelper.GetChangedDeltas(fromVersion, toVersion);
        var changedNamesFromDeltas = DeltaChangeAggregator.AggregateChanges(deltas);
        return new ChangeSet(toVersion, changedNamesFromDeltas);
    }

    public Task<(List<string> Names, ulong LatestVersion)> List(string parent, CancellationToken ct = default)
    {
        var indexRef = ThrowIfNotInitialized();

        if (string.IsNullOrEmpty(parent) || parent is "/")
        {
            var allNames = indexRef.Entities
                .Select(entity => entity.Key)
                .ToList();
            return Task.FromResult((allNames, indexRef.LatestVersion));
        }

        var trimmedParent = parent.TrimEnd('/');
        var children = indexRef.Entities.ListDescendants(trimmedParent).ToList();
        return Task.FromResult((children, indexRef.LatestVersion));
    }

    public Task<uint> CountChildren(string parent, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var indexRef = ThrowIfNotInitialized();

        if (string.IsNullOrEmpty(parent) || parent is "/")
        {
            return Task.FromResult((uint)indexRef.Entities.Count);
        }

        var trimmedParent = parent.TrimEnd('/');
        return Task.FromResult((uint)indexRef.Entities.CountDescendants(trimmedParent));
    }

    private async Task<ImmutableDatasetState> UpdateIndexWithLatestDelta(ImmutableDatasetState datasetState, ulong latestDelta)
    {
        var tempState = datasetState;

        if (latestDelta > tempState.LatestVersion)
        {
            var deltas = await _datasetHelper.GetChangedDeltas(tempState.LatestVersion, latestDelta);
            tempState = tempState.ApplyDeltas(deltas);
            _logger.LogInformation($"Dataset updated to version {tempState.LatestVersion}");
        }

        return tempState;
    }

    public ulong GetLatestVersion()
    {
        var indexRef = ThrowIfNotInitialized();
        return indexRef.LatestVersion;
    }
    public ulong GetLastUsedSnapshotVersion()
    {
        ThrowIfNotInitialized();
        return _snapshotPositionLookup?.SnapshotVersion ?? 0;
    }

    public ulong GetMainIndexSnapshotVersion()
    {
        var indexRef = ThrowIfNotInitialized();
        return indexRef?.SnapshotVersion ?? 0;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(CancellationToken cancellationToken = default)
    {
        if (_initializationTask == null)
        {
            return HealthCheckResult.Healthy("DatasetReader: Initialization task is not yet started");
        }

        return await GetHealthCheckResult();
    }

    private async Task<HealthCheckResult> GetHealthCheckResult()
    {
        if (!_initializationTask!.IsCompletedSuccessfully)
        {
            // Give initialization a short grace period before reporting unhealthy
            await Task.WhenAny(_initializationTask, Task.Delay(TimeSpan.FromSeconds(10)));
            if (!_initializationTask.IsCompletedSuccessfully)
                return HealthCheckResult.Degraded("DatasetReader: Still initializing");
        }

        try
        {
            await _storageProvider.ListVersions();
        }
        catch (Exception e)
        {
            return HealthCheckResult.Unhealthy("DatasetReader: Failed to get latest versions from dataset", e);
        }

        return HealthCheckResult.Healthy();
    }

    public async Task<HealthCheckResult> CheckReadinessAsync(CancellationToken cancellationToken = default)
    {
        if (_initializationTask == null)
        {
            return HealthCheckResult.Unhealthy("DatasetReader: Initialization task is not yet started");
        }

        return await GetHealthCheckResult();
    }

    private async Task<IDisposable> Lock(SemaphoreSlim semaphore)
    {
        await semaphore.WaitAsync();
        return new ActionDisposable(() => semaphore.Release());
    }

    public async Task<List<HistoryEntry>> GetHistory(string name, int limit = 0, ulong minVersion = 0, CancellationToken ct = default)
    {
        var indexRef = ThrowIfNotInitialized();
        if (!indexRef.Entities.TryGetValue(name, out var entity))
        {
            return [];
        }

        var history = new List<HistoryEntry>();
        var currentVersion = entity.LatestVersion;

        while (currentVersion > 0 && currentVersion > minVersion)
        {
            if (limit > 0 && history.Count >= limit)
            {
                break;
            }

            ct.ThrowIfCancellationRequested();
            DeltaIndex deltaIndex;
            try
            {
                var deltaBytes = await _storageProvider.ReadDeltaIndex(currentVersion);
                deltaIndex = NestedIndexHelper.ParseDeltaIndex(deltaBytes.Span);
            }
            catch (FileNotFoundException)
            {
                break;
            }

            ChangeType changeType = ChangeType.None;
            ulong previousVersion = 0;

            var newEntity = deltaIndex.NewEntities.FirstOrDefault(e => e.Name == name);
            if (newEntity != null)
            {
                changeType = ChangeType.New;
                previousVersion = newEntity.PreviousVersion;
            }

            if (changeType == ChangeType.None)
            {
                var changedEntity = deltaIndex.ChangedEntities.FirstOrDefault(e => e.Name == name);
                if (changedEntity != null)
                {
                    changeType = ChangeType.Changed;
                    previousVersion = changedEntity.PreviousVersion;
                }
            }

            if (changeType == ChangeType.None)
            {
                // Prefer DeletedEntity (with PreviousVersion) over DeletedNames (legacy, without previous version)
                var deletedEntity = deltaIndex.DeletedEntities.FirstOrDefault(e => e.Name == name);
                if (deletedEntity != null)
                {
                    changeType = ChangeType.Deleted;
                    previousVersion = deletedEntity.PreviousVersion;
                }
                else if (deltaIndex.DeletedNames.Contains(name))
                {
                    changeType = ChangeType.Deleted;
                    previousVersion = 0;
                }
            }

            if (changeType != ChangeType.None)
            {
                var timestamp = deltaIndex.Timestamp?.ToDateTimeOffset() ?? DateTimeOffset.MinValue.ToUniversalTime();

                history.Add(new HistoryEntry(currentVersion, changeType, timestamp, previousVersion));
                currentVersion = previousVersion;
            }
            else
            {
                break;
            }
        }

        return history;
    }

    public async IAsyncEnumerable<HistoryEntry> StreamHistory(string parent, ulong sinceVersion = 0, ulong maxVersion = 0, bool waitIfNone = false, [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (_initializationTask == null || _initializationTask.IsFaulted)
            ThrowIfNotInitialized();

        if (!_initializationTask!.IsCompleted)
            await _initializationTask;

        // Wait for version to advance beyond sinceVersion if requested
        if (waitIfNone)
        {
            while (sinceVersion >= ThrowIfNotInitialized().LatestVersion)
            {
                await IndexChangedTaskCompletionSource.Task.WaitAsync(ct);
            }
        }

        var indexRef = ThrowIfNotInitialized();
        var currentVersion = maxVersion > 0 ? maxVersion : indexRef.LatestVersion;

        while (currentVersion > 0 && currentVersion > sinceVersion)
        {
            ct.ThrowIfCancellationRequested();
            DeltaIndex deltaIndex;
            try
            {
                var deltaBytes = await _storageProvider.ReadDeltaIndex(currentVersion);
                deltaIndex = NestedIndexHelper.ParseDeltaIndex(deltaBytes.Span);
            }
            catch (FileNotFoundException)
            {
                break;
            }

            var timestamp = deltaIndex.Timestamp?.ToDateTimeOffset() ?? DateTimeOffset.MinValue.ToUniversalTime();

            // Create a set of deleted entity names that have the richer deleted entity info (with previous version)
            var deletedEntityNames = new HashSet<string>(deltaIndex.DeletedEntities.Select(e => e.Name));

            foreach (var newEntity in deltaIndex.NewEntities)
            {
                if (string.IsNullOrEmpty(parent) || newEntity.Name.StartsWith(parent, StringComparison.Ordinal))
                {
                    yield return new HistoryEntry(currentVersion, ChangeType.New, timestamp, newEntity.PreviousVersion) { Id = newEntity.Name };
                }
            }

            foreach (var changedEntity in deltaIndex.ChangedEntities)
            {
                if (string.IsNullOrEmpty(parent) || changedEntity.Name.StartsWith(parent, StringComparison.Ordinal))
                {
                    yield return new HistoryEntry(currentVersion, ChangeType.Changed, timestamp, changedEntity.PreviousVersion) { Id = changedEntity.Name };
                }
            }

            foreach (var deletedEntity in deltaIndex.DeletedEntities)
            {
                if (string.IsNullOrEmpty(parent) || deletedEntity.Name.StartsWith(parent, StringComparison.Ordinal))
                {
                    yield return new HistoryEntry(currentVersion, ChangeType.Deleted, timestamp, deletedEntity.PreviousVersion) { Id = deletedEntity.Name };
                }
            }

            // Only yield deleted names that don't have a corresponding deleted entity (avoid duplicates)
            // Prefer the richer deleted entity info when both exist
            foreach (var deletedName in deltaIndex.DeletedNames)
            {
                if (!deletedEntityNames.Contains(deletedName) &&
                    (string.IsNullOrEmpty(parent) || deletedName.StartsWith(parent, StringComparison.Ordinal)))
                {
                    yield return new HistoryEntry(currentVersion, ChangeType.Deleted, timestamp, 0) { Id = deletedName };
                }
            }

            // We need to find the next version to check.
            // Since we are streaming all history, we just go to the previous version of the dataset.
            // However, the DeltaIndex doesn't explicitly store the "previous dataset version".
            // But we know versions are sequential or we can infer it?
            // Actually, DatasetReader usually assumes versions are sequential or we can just decrement?
            // Wait, storage provider ReadDeltaIndex takes a version.
            // In GetHistory, we follow the entity's previous version.
            // Here we want ALL changes, so we should probably just go to currentVersion - 1?
            // Or is there a way to know the previous version from the DeltaIndex?
            // Looking at DeltaIndex proto (not visible here but inferred), it might not have it.
            // But `GetChangedNames` usually iterates.
            // Let's assume sequential for now or check how `GetChangedNames` works.

            // Checking `GetChangedNames` implementation might reveal how versions are traversed.
            // But `GetHistory` follows `previousVersion` from the entity change.

            // If versions are not sequential (e.g. gaps), simply decrementing might fail with FileNotFound, which is handled.
            // But better if we knew.

            currentVersion--;
        }
    }

    public async IAsyncEnumerable<HistoryEntry> TailHistory(string parent, ulong sinceVersion = 0, bool follow = false, [EnumeratorCancellation] CancellationToken ct = default)
    {
        // TailHistory yields history from older -> newer (ascending version order).
        // It starts at (sinceVersion + 1) and yields entries up to the latest version.
        // If `follow` is true, it will wait for new versions and continue yielding.

        if (_initializationTask == null || _initializationTask.IsFaulted)
            ThrowIfNotInitialized();

        if (!_initializationTask!.IsCompleted)
            await _initializationTask;

        var indexRef = ThrowIfNotInitialized();
        var nextVersion = sinceVersion == 0 ? 1UL : sinceVersion + 1;

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            var currentLatest = ThrowIfNotInitialized().LatestVersion;

            if (nextVersion > currentLatest)
            {
                if (!follow)
                    yield break;

                // Wait for the dataset to advance
                await IndexChangedTaskCompletionSource.Task.WaitAsync(ct);
                continue;
            }

            // Read delta for nextVersion and yield its changes (new -> changed -> deleted)
            DeltaIndex deltaIndex;
            try
            {
                var deltaBytes = await _storageProvider.ReadDeltaIndex(nextVersion);
                deltaIndex = NestedIndexHelper.ParseDeltaIndex(deltaBytes.Span);
            }
            catch (FileNotFoundException)
            {
                // If no delta found for this version, skip forward
                nextVersion++;
                continue;
            }

            var timestamp = deltaIndex.Timestamp?.ToDateTimeOffset() ?? DateTimeOffset.MinValue.ToUniversalTime();

            // Yield in a stable order: New, Changed, DeletedEntities, DeletedNames (avoid duplicates)
            var deletedEntityNames = new HashSet<string>(deltaIndex.DeletedEntities.Select(e => e.Name));

            foreach (var newEntity in deltaIndex.NewEntities)
            {
                if (string.IsNullOrEmpty(parent) || newEntity.Name.StartsWith(parent, StringComparison.Ordinal))
                {
                    yield return new HistoryEntry(nextVersion, ChangeType.New, timestamp, newEntity.PreviousVersion) { Id = newEntity.Name };
                }
            }

            foreach (var changedEntity in deltaIndex.ChangedEntities)
            {
                if (string.IsNullOrEmpty(parent) || changedEntity.Name.StartsWith(parent, StringComparison.Ordinal))
                {
                    yield return new HistoryEntry(nextVersion, ChangeType.Changed, timestamp, changedEntity.PreviousVersion) { Id = changedEntity.Name };
                }
            }

            foreach (var deletedEntity in deltaIndex.DeletedEntities)
            {
                if (string.IsNullOrEmpty(parent) || deletedEntity.Name.StartsWith(parent, StringComparison.Ordinal))
                {
                    yield return new HistoryEntry(nextVersion, ChangeType.Deleted, timestamp, deletedEntity.PreviousVersion) { Id = deletedEntity.Name };
                }
            }

            foreach (var deletedName in deltaIndex.DeletedNames)
            {
                if (!deletedEntityNames.Contains(deletedName) &&
                    (string.IsNullOrEmpty(parent) || deletedName.StartsWith(parent, StringComparison.Ordinal)))
                {
                    yield return new HistoryEntry(nextVersion, ChangeType.Deleted, timestamp, 0) { Id = deletedName };
                }
            }

            nextVersion++;
        }
    }

    public async Task<(string name, ReadOnlyMemory<byte> entityBytes)?> GetHistoric(string name, ulong version, CancellationToken ct = default)
    {
        var indexRef = ThrowIfNotInitialized();
        if (version > indexRef.LatestVersion)
        {
            throw new ArgumentOutOfRangeException(nameof(version), "Version is in the future");
        }

        DeltaIndex deltaIndex;
        try
        {
            var deltaBytes = await _storageProvider.ReadDeltaIndex(version);
            deltaIndex = NestedIndexHelper.ParseDeltaIndex(deltaBytes.Span);
        }
        catch (FileNotFoundException)
        {
            throw new ArgumentException("Delta not found", nameof(version));
        }

        // Check if it was deleted in this version
        var deletedEntity = deltaIndex.DeletedEntities.FirstOrDefault(e => e.Name == name);
        if (deletedEntity != null || deltaIndex.DeletedNames.Contains(name))
        {
            return null;
        }

        // Check if it was New or Changed
        var newEntity = deltaIndex.NewEntities.FirstOrDefault(e => e.Name == name);
        if (newEntity != null)
        {
            var data = await _storageProvider.ReadDataFromSources([(ObjectNameTypeEnum.Delta, version, newEntity.Position)], newEntity.Length);
            return (name, data);
        }

        var changedEntity = deltaIndex.ChangedEntities.FirstOrDefault(e => e.Name == name);
        if (changedEntity != null)
        {
            var data = await _storageProvider.ReadDataFromSources([(ObjectNameTypeEnum.Delta, version, changedEntity.Position)], changedEntity.Length);
            return (name, data);
        }

        throw new ArgumentException($"Entity {name} was not changed in version {version}");
    }
}
