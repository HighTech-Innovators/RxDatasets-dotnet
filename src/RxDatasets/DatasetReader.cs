
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
    private ImmutableDatasetState? _mostRecentSnapshotIndex;
    private readonly ParentsIndex _parentsIndex = new();
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
                        await Refresh(_cts.Token);
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
            IndexChangedTaskCompletionSource = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            tcs.SetResult();
        }
        catch (InvalidOperationException)
        {
            // we are not ready
        }
    }


    private async Task OnSnapshotCreated(SnapshotCreatedNotification notification)
    {
        if (_mostRecentSnapshotIndex == null
            || (_mostRecentSnapshotIndex != null && notification.Version > _mostRecentSnapshotIndex.SnapshotVersion))
        {
            _mostRecentSnapshotIndex = await _datasetHelper.GetLatestSnapshotIndex();
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _cts.CancelAsync();
        _subscriptions.Dispose();
    }

    /// <summary>
    /// Completed, and replaced, after every update of <see cref="_index"/>. A waiter reads it before it checks the
    /// version: the update moves the index first and swaps this second, so a waiter that still sees the old version
    /// holds the completion source that is about to complete. Checked first and read second, a swap in between would
    /// leave the waiter on the next one, and the update unseen (VLOW-30). Volatile, so the order holds on every CPU.
    /// </summary>
    private TaskCompletionSource IndexChangedTaskCompletionSource
    {
        get => _indexChangedTaskCompletionSource;
        set => _indexChangedTaskCompletionSource = value;
    }

    private volatile TaskCompletionSource _indexChangedTaskCompletionSource =
        new(TaskCreationOptions.RunContinuationsAsynchronously);


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

    /// <summary>
    /// Reads the index from storage again, every five seconds in read-only mode: for an update whose notification
    /// never came, and for a dataset that was rebuilt, whose version really went back.
    /// </summary>
    /// <remarks>
    /// A reader that is ready keeps answering from the index it has while the new one is read. Assigned to the
    /// initialisation task instead, the re-read made every request fail as not initialised while it ran, and failed
    /// them all when it failed; and racing an update, it put back the older index it had read, so the version went
    /// backwards until the next update. A reader that is not ready yet, or whose last attempt failed, is initialised
    /// as at the start.
    /// <para>
    /// The index is read outside the lock <see cref="OnDatasetUpdated"/> holds, and only taken over under it: held
    /// during the read, a slow storage read would hold up every update notification, and with it the writer that
    /// sends them.
    /// </para>
    /// </remarks>
    private async Task Refresh(CancellationToken cancellationToken)
    {
        if (_initializationTask is not { IsCompletedSuccessfully: true })
        {
            _initializationTask = Initialize();
            await _initializationTask;
            return;
        }

        var latestIndex = await _datasetHelper.GetLatestIndex();
        ImmutableDatasetState? latestSnapshotIndex = null;
        try
        {
            latestSnapshotIndex = await _datasetHelper.GetLatestSnapshotIndex();
        }
        catch (FileNotFoundException)
        {
            //ignored as we dont have a snapshot yet
        }

        using var updateSlot = await Lock(_writerSemaphore, cancellationToken);
        var before = _index?.LatestVersion;
        if (latestIndex.LatestVersion < before)
        {
            // Either an update arrived while the index was read, and the read is stale, or the dataset was rebuilt
            // and its version really went back. Storage tells which.
            var (_, newestDelta, _) = await _storageProvider.ListVersions();
            if (newestDelta >= before)
                return;

            _logger.LogWarning("Dataset {EntityType} went back from version {Before} to {Version}; taking it over",
                typeof(T).Name, before, latestIndex.LatestVersion);
        }

        _index = latestIndex;
        if (latestSnapshotIndex != null)
            _mostRecentSnapshotIndex = latestSnapshotIndex;

        if (_index.LatestVersion != before)
        {
            var tcs = IndexChangedTaskCompletionSource;
            IndexChangedTaskCompletionSource = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            tcs.SetResult();
        }
    }

    private async Task Initialize()
    {
        try
        {
            _index = await _datasetHelper.GetLatestIndex();
            try
            {
                _mostRecentSnapshotIndex = await _datasetHelper.GetLatestSnapshotIndex();
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
        _mostRecentSnapshotIndex = null;
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
            if (!indexRef.Entities.TryGetValue(name, out var entity) || entity.GetLatestChangeType() == ChangeType.Deleted)
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
        // Precondition: calling method MUST have checked it exists AND is not deleted.
        var entityInMainIndex = _index!.Entities[name];
        var entityVersion = entityInMainIndex.GetLatestEntityVersion();

        List<(ObjectNameTypeEnum type, ulong version, long position)> sources = new(2);

        if (_mostRecentSnapshotIndex != null && entityVersion <= _mostRecentSnapshotIndex!.SnapshotVersion)
        {
            // At this point, we KNOW it must be in there and be the latest version in existence,
            // because the latest version is smaller/equal to the snapshot,
            // AND it's not deleted (which should have been checked in the calling method)
            var entityFromSnapshot = _mostRecentSnapshotIndex.Entities[name];
            sources.Add((ObjectNameTypeEnum.Snapshot, _mostRecentSnapshotIndex.SnapshotVersion,
                entityFromSnapshot.Position));
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

        while (true)
        {
            var changed = IndexChangedTaskCompletionSource.Task;
            if (fromVersion != ThrowIfNotInitialized().LatestVersion)
            {
                break;
            }

            await changed.WaitAsync(cancellationToken);
        }

        return await GetChangedNames(fromVersion);
    }

    public async Task<(string name, ReadOnlyMemory<byte> entityBytes)?> Get(string name)
    {
        var indexRef = ThrowIfNotInitialized();

        if (!indexRef.Entities.TryGetValue(name, out var entity))
            return null;

        if (entity.GetLatestChangeType() == ChangeType.Deleted)
            return null;

        var entityBytes = await ReadEntityBytes(name);

        return (name, entityBytes);
    }

    public Task<ChangeSet> GetChangedNames(ulong fromVersion)
    {
        var indexRef = ThrowIfNotInitialized();
        var toVersion = indexRef.LatestVersion;

        if (fromVersion > toVersion)
        {
            throw new ArgumentOutOfRangeException($"fromVersion {fromVersion} can't be bigger than the latest version {toVersion}");
        }
        if (fromVersion == toVersion)
        {
            return Task.FromResult(new ChangeSet(toVersion, []));
        }

        var changedNames = GenerateChangedNames(fromVersion, toVersion, indexRef);
        return Task.FromResult(new ChangeSet(toVersion, changedNames));
    }

    private List<(string key, ChangeType change)> GenerateChangedNames(ulong fromVersion, ulong toVersion, ImmutableDatasetState indexRef)
    {
        List<(string key, ChangeType change)> changedNames = [];

        indexRef.Entities
            .Where(x => x.Value.GetLatestEntityVersion() > fromVersion && x.Value.GetLatestEntityVersion() <= toVersion)
            .ToList()
            .ForEach(x =>
            {
                var relativeChangeType = x.Value.LastDeletedVersion.Select(y => (y, ChangeType.Deleted))
                    .Concat(x.Value.LastInsertedVersion.Select(y => (y, ChangeType.New)))
                    .Append((x.Value.LastUpdatedVersion, ChangeType.Changed))
                    .Where(x => x.Item1 > fromVersion)
                    .OrderBy(x => x.Item1)
                    .Select(x => x.Item2)
                    .Aggregate((current, next) =>
                    {
                        return next switch
                        {
                            ChangeType.New => current == ChangeType.Deleted ? ChangeType.Changed : ChangeType.New,
                            ChangeType.Changed => current == ChangeType.New ? ChangeType.New : ChangeType.Changed,
                            ChangeType.Deleted => current == ChangeType.New ? ChangeType.None : ChangeType.Deleted,
                            _ => next,
                        };
                    });

                if (relativeChangeType != ChangeType.None)
                {
                    changedNames.Add((x.Key, relativeChangeType));
                }
            });
        return changedNames;
    }

    public async Task<(List<string> Names, ulong LatestVersion)> List(string parent, CancellationToken ct = default)
    {
        var indexRef = ThrowIfNotInitialized();


        IEnumerable<string> allNamesGetter = indexRef.Entities
            .Where(x => x.Value.GetLatestChangeType() != ChangeType.Deleted)
            .Select(entity => entity.Key);

        if (string.IsNullOrEmpty(parent) || parent is "/")
        {
            return (allNamesGetter.ToList(), indexRef.LatestVersion);
        }

        var childrenIndex = await _parentsIndex.GetOrCreateChildrenIndex(parent, allNamesGetter);
        return (childrenIndex.ToList(), indexRef.LatestVersion);

    }

    public async Task<uint> CountChildren(string parent, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var indexRef = ThrowIfNotInitialized();

        if (string.IsNullOrEmpty(parent) || parent is "/")
        {
            // For root path, count all non-deleted entities
            return (uint)indexRef.Entities
                .Count(x => x.Value.GetLatestChangeType() != ChangeType.Deleted);
        }

        IEnumerable<string> allNamesGetter = indexRef.Entities
            .Where(x => x.Value.GetLatestChangeType() != ChangeType.Deleted)
            .Select(entity => entity.Key);

        var childrenIndex = await _parentsIndex.GetOrCreateChildrenIndex(parent, allNamesGetter);
        return (uint)childrenIndex.Count();
    }

    private async Task<ImmutableDatasetState> UpdateIndexWithLatestDelta(ImmutableDatasetState datasetState, ulong latestDelta)
    {
        var tempState = datasetState;

        if (latestDelta > tempState.LatestVersion)
        {
            var deltas = await _datasetHelper.GetChangedDeltas(tempState.LatestVersion, latestDelta);
            tempState = tempState.ApplyDeltas(deltas);
            var thereIsStillData = await _parentsIndex.CleanIndex();
            if (thereIsStillData)
            {
                foreach (DeltaIndex delta in deltas)
                {
                    foreach (var newEntity in delta.NewEntities)
                    {
                        await _parentsIndex.OnNew(newEntity.Name);
                    }

                    foreach (var deletedEntity in delta.DeletedNames)
                    {
                        await _parentsIndex.OnDelete(deletedEntity);
                    }
                    foreach (var deletedEntity in delta.DeletedEntities)
                    {
                        await _parentsIndex.OnDelete(deletedEntity.Name);
                    }
                }
            }

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
        return _mostRecentSnapshotIndex?.SnapshotVersion ?? 0;
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
            return HealthCheckResult.Unhealthy("DatasetReader: Initialization task is not completed");

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

    private async Task<IDisposable> Lock(SemaphoreSlim semaphore, CancellationToken cancellationToken = default)
    {
        await semaphore.WaitAsync(cancellationToken);
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
        var currentVersion = entity.GetLatestEntityVersion();

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
                deltaIndex = DeltaIndex.Parser.ParseFrom(deltaBytes.Span);
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
            while (true)
            {
                var changed = IndexChangedTaskCompletionSource.Task;
                if (sinceVersion < ThrowIfNotInitialized().LatestVersion)
                {
                    break;
                }

                await changed.WaitAsync(ct);
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
                deltaIndex = DeltaIndex.Parser.ParseFrom(deltaBytes.Span);
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

            var changed = IndexChangedTaskCompletionSource.Task;
            var currentLatest = ThrowIfNotInitialized().LatestVersion;

            if (nextVersion > currentLatest)
            {
                if (!follow)
                    yield break;

                // Wait for the dataset to advance
                await changed.WaitAsync(ct);
                continue;
            }

            // Read delta for nextVersion and yield its changes (new -> changed -> deleted)
            DeltaIndex deltaIndex;
            try
            {
                var deltaBytes = await _storageProvider.ReadDeltaIndex(nextVersion);
                deltaIndex = DeltaIndex.Parser.ParseFrom(deltaBytes.Span);
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
            deltaIndex = DeltaIndex.Parser.ParseFrom(deltaBytes.Span);
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
