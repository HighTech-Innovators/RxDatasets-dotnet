
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
            var entity = indexRef.GetEntity(name);
            if (entity == null || entity.GetLatestChangeType() == ChangeType.Deleted)
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
        var entityInMainIndex = _index!.GetEntity(name)!;
        var entityVersion = entityInMainIndex.GetLatestEntityVersion();

        List<(ObjectNameTypeEnum type, ulong version, long position)> sources = new(2);

        if (_mostRecentSnapshotIndex != null && entityVersion <= _mostRecentSnapshotIndex!.SnapshotVersion)
        {
            // At this point, we KNOW it must be in there and be the latest version in existence,
            // because the latest version is smaller/equal to the snapshot,
            // AND it's not deleted (which should have been checked in the calling method)
            var entityFromSnapshot = _mostRecentSnapshotIndex.GetEntity(name)!;
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

        while (fromVersion == ThrowIfNotInitialized().LatestVersion)
        {
            await IndexChangedTaskCompletionSource.Task.WaitAsync(cancellationToken);
        }

        return await GetChangedNames(fromVersion);
    }

    public async Task<(string name, ReadOnlyMemory<byte> entityBytes)?> Get(string name)
    {
        var indexRef = ThrowIfNotInitialized();

        var entity = indexRef.GetEntity(name);
        if (entity == null)
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
        var changedNames = new List<(string key, ChangeType change)>();
        TraverseAndFindChanges(indexRef.IndexNode, "", fromVersion, toVersion, changedNames);
        return changedNames;
    }

    private void TraverseAndFindChanges(ImmutableIndexNode node, string pathPrefix, ulong fromVersion, ulong toVersion, List<(string key, ChangeType change)> results)
    {
        foreach (var entityKvp in node.Entities)
        {
            var entity = entityKvp.Value;
            if (entity.GetLatestEntityVersion() > fromVersion && entity.GetLatestEntityVersion() <= toVersion)
            {
                var fullPath = string.IsNullOrEmpty(pathPrefix) ? entityKvp.Key : $"{pathPrefix}/{entityKvp.Key}";
                var changeType = CalculateChangeType(entity, fromVersion);
                if (changeType != ChangeType.None)
                {
                    results.Add((fullPath, changeType));
                }
            }
        }

        foreach (var childKvp in node.Children)
        {
            var childPrefix = string.IsNullOrEmpty(pathPrefix) ? childKvp.Key : $"{pathPrefix}/{childKvp.Key}";
            TraverseAndFindChanges(childKvp.Value, childPrefix, fromVersion, toVersion, results);
        }
    }

    private ChangeType CalculateChangeType(ImmutableSnapshotEntity entity, ulong fromVersion)
    {
        return entity.LastDeletedVersion.Select(y => (y, ChangeType.Deleted))
            .Concat(entity.LastInsertedVersion.Select(y => (y, ChangeType.New)))
            .Append((entity.LastUpdatedVersion, ChangeType.Changed))
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
    }

    public async Task<(List<string> Names, ulong LatestVersion)> List(string parent, CancellationToken ct = default)
    {
        var indexRef = ThrowIfNotInitialized();

        var pathParts = string.IsNullOrEmpty(parent) || parent == "/" 
            ? Array.Empty<string>() 
            : parent.Split('/', StringSplitOptions.RemoveEmptyEntries);

        var currentNode = indexRef.IndexNode;
        foreach (var part in pathParts)
        {
            currentNode = currentNode.GetChild(part);
            if (currentNode == null)
            {
                return (new List<string>(), indexRef.LatestVersion);
            }
        }

        // Build the prefix from the navigated segments, so a leading/trailing slash in parent doesn't leak into names
        var prefix = string.Join('/', pathParts);
        var names = new List<string>();
        CollectEntitiesRecursive(currentNode, prefix, names);

        return (names, indexRef.LatestVersion);
    }

    private void CollectEntitiesRecursive(ImmutableIndexNode node, string basePath, List<string> results)
    {
        foreach (var entity in node.Entities)
        {
            if (entity.Value.GetLatestChangeType() != ChangeType.Deleted)
            {
                var fullPath = string.IsNullOrEmpty(basePath) ? entity.Key : $"{basePath}/{entity.Key}";
                results.Add(fullPath);
            }
        }

        foreach (var child in node.Children)
        {
            var childPath = string.IsNullOrEmpty(basePath) ? child.Key : $"{basePath}/{child.Key}";
            CollectEntitiesRecursive(child.Value, childPath, results);
        }
    }

    public async Task<uint> CountChildren(string parent, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var indexRef = ThrowIfNotInitialized();

        var pathParts = string.IsNullOrEmpty(parent) || parent == "/" 
            ? Array.Empty<string>() 
            : parent.Split('/', StringSplitOptions.RemoveEmptyEntries);

        var currentNode = indexRef.IndexNode;
        foreach (var part in pathParts)
        {
            currentNode = currentNode.GetChild(part);
            if (currentNode == null)
            {
                return 0;
            }
        }

        return (uint)CountEntitiesRecursive(currentNode);
    }

    private int CountEntitiesRecursive(ImmutableIndexNode node)
    {
        var count = node.Entities.Count(x => x.Value.GetLatestChangeType() != ChangeType.Deleted);
        foreach (var child in node.Children.Values)
        {
            count += CountEntitiesRecursive(child);
        }
        return count;
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

    private async Task<IDisposable> Lock(SemaphoreSlim semaphore)
    {
        await semaphore.WaitAsync();
        return new ActionDisposable(() => semaphore.Release());
    }

    public async Task<List<HistoryEntry>> GetHistory(string name, int limit = 0, ulong minVersion = 0, CancellationToken ct = default)
    {
        var indexRef = ThrowIfNotInitialized();
        var entity = indexRef.GetEntity(name);
        if (entity == null)
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

            // Try hierarchical format first (new format with DeltaNode root)
            if (deltaIndex.HasHierarchicalEntries())
            {
                var entry = FindEntityInHierarchicalDelta(deltaIndex.Root, name);
                if (entry != null)
                {
                    changeType = entry.Value.changeType;
                    previousVersion = entry.Value.previousVersion;
                }
            }
            else
            {
                // Fall back to legacy flat format
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

    /// <summary>
    /// Finds an entity in the hierarchical delta structure by walking the tree.
    /// Returns the change type and previous version if found, null otherwise.
    /// </summary>
    private (ChangeType changeType, ulong previousVersion)? FindEntityInHierarchicalDelta(DeltaNode root, string entityName)
    {
        // Split the entity name by path separator
        var pathParts = entityName.Split('/', StringSplitOptions.RemoveEmptyEntries);

        // Navigate to the correct node in the hierarchy
        var currentNode = root;
        for (int i = 0; i < pathParts.Length - 1; i++)
        {
            if (!currentNode.Children.TryGetValue(pathParts[i], out var childNode))
            {
                return null; // Path doesn't exist
            }
            currentNode = childNode;
        }

        // Now look for the entity in the final node
        var finalSegment = pathParts[^1];

        // Check new entities
        if (currentNode.NewEntities.TryGetValue(finalSegment, out var newEntity))
        {
            return (ChangeType.New, newEntity.PreviousVersion);
        }

        // Check changed entities
        if (currentNode.ChangedEntities.TryGetValue(finalSegment, out var changedEntity))
        {
            return (ChangeType.Changed, changedEntity.PreviousVersion);
        }

        // Check deleted entities
        if (currentNode.DeletedEntities.TryGetValue(finalSegment, out var deletedEntity))
        {
            return (ChangeType.Deleted, deletedEntity.PreviousVersion);
        }

        return null;
    }

    private (DeltaEntity? entity, bool isDeleted) FindEntityDataInHierarchicalDelta(DeltaNode root, string entityName)
    {
        var pathParts = entityName.Split('/', StringSplitOptions.RemoveEmptyEntries);
        var currentNode = root;
        for (int i = 0; i < pathParts.Length - 1; i++)
        {
            if (!currentNode.Children.TryGetValue(pathParts[i], out var childNode))
            {
                return (null, false);
            }
            currentNode = childNode;
        }

        var finalSegment = pathParts[^1];

        if (currentNode.NewEntities.TryGetValue(finalSegment, out var newEntity))
        {
            return (newEntity, false);
        }

        if (currentNode.ChangedEntities.TryGetValue(finalSegment, out var changedEntity))
        {
            return (changedEntity, false);
        }

        if (currentNode.DeletedEntities.ContainsKey(finalSegment))
        {
            return (null, true);
        }

        return (null, false);
    }

    /// <summary>
    /// Enumerates changes from a hierarchical DeltaNode using tree traversal (alloc-free).
    /// Splits path once and walks the tree structure for O(depth) instead of O(n) performance.
    /// </summary>
    private IEnumerable<HistoryEntry> EnumerateHierarchicalDeltaEntries(DeltaNode root, string[] parentPathParts, int depth, string currentPath, ChangeType changeType, ulong version, DateTimeOffset timestamp, HashSet<string>? deletedEntityNames = null)
    {
        deletedEntityNames ??= [];

        if (depth >= parentPathParts.Length)
        {
            // Reached target depth - enumerate this subtree
            switch (changeType)
            {
                case ChangeType.New:
                    foreach (var (key, entity) in root.NewEntities)
                    {
                        var fullPath = string.IsNullOrEmpty(currentPath) ? key : currentPath + "/" + key;
                        yield return new HistoryEntry(version, ChangeType.New, timestamp, entity.PreviousVersion) { Id = fullPath };
                    }
                    break;

                case ChangeType.Changed:
                    foreach (var (key, entity) in root.ChangedEntities)
                    {
                        var fullPath = string.IsNullOrEmpty(currentPath) ? key : currentPath + "/" + key;
                        yield return new HistoryEntry(version, ChangeType.Changed, timestamp, entity.PreviousVersion) { Id = fullPath };
                    }
                    break;

                case ChangeType.Deleted:
                    foreach (var (key, deletedEntity) in root.DeletedEntities)
                    {
                        var fullPath = string.IsNullOrEmpty(currentPath) ? key : currentPath + "/" + key;
                        if (!deletedEntityNames.Contains(fullPath))
                        {
                            yield return new HistoryEntry(version, ChangeType.Deleted, timestamp, deletedEntity.PreviousVersion) { Id = fullPath };
                        }
                    }
                    break;
            }

            // Recursively enumerate from all children
            foreach (var (key, child) in root.Children)
            {
                var childPath = string.IsNullOrEmpty(currentPath) ? key : currentPath + "/" + key;
                foreach (var entry in EnumerateHierarchicalDeltaEntries(child, parentPathParts, depth + 1, childPath, changeType, version, timestamp, deletedEntityNames))
                {
                    yield return entry;
                }
            }
        }
        else
        {
            // Navigate deeper using array slicing (alloc-free) - no new array allocation
            var nextPart = parentPathParts[depth];
            if (root.Children.TryGetValue(nextPart, out var nextNode))
            {
                // Recursively navigate with incremented depth (no allocation of new path array)
                var childPath = string.IsNullOrEmpty(currentPath) ? nextPart : currentPath + "/" + nextPart;
                foreach (var entry in EnumerateHierarchicalDeltaEntries(nextNode, parentPathParts, depth + 1, childPath, changeType, version, timestamp, deletedEntityNames))
                {
                    yield return entry;
                }
            }
        }
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

        // Parse parent path once for hierarchical traversal (alloc-free with index parameters)
        var parentParts = string.IsNullOrEmpty(parent) ? [] : parent.Split('/', StringSplitOptions.RemoveEmptyEntries);

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

            // Prefer hierarchical root if available (O(depth) queries), else fallback to flat with StartsWith
            if (deltaIndex.HasHierarchicalEntries())
            {
                // Hierarchical root available - use tree traversal
                foreach (var entry in EnumerateHierarchicalDeltaEntries(deltaIndex.Root, parentParts, 0, "", ChangeType.New, currentVersion, timestamp))
                {
                    yield return entry;
                }
                foreach (var entry in EnumerateHierarchicalDeltaEntries(deltaIndex.Root, parentParts, 0, "", ChangeType.Changed, currentVersion, timestamp))
                {
                    yield return entry;
                }
                foreach (var entry in EnumerateHierarchicalDeltaEntries(deltaIndex.Root, parentParts, 0, "", ChangeType.Deleted, currentVersion, timestamp, deletedEntityNames))
                {
                    yield return entry;
                }
            }
            else
            {
                // Fallback to flat structure with StartsWith
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
            }

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

        // Parse parent path once for hierarchical traversal (alloc-free with index parameters)
        var parentParts = string.IsNullOrEmpty(parent) ? [] : parent.Split('/', StringSplitOptions.RemoveEmptyEntries);

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

            // Prefer hierarchical root if available (O(depth) queries), else fallback to flat with StartsWith
            if (deltaIndex.HasHierarchicalEntries())
            {
                // Hierarchical root available - use tree traversal
                foreach (var entry in EnumerateHierarchicalDeltaEntries(deltaIndex.Root, parentParts, 0, "", ChangeType.New, nextVersion, timestamp))
                {
                    yield return entry;
                }
                foreach (var entry in EnumerateHierarchicalDeltaEntries(deltaIndex.Root, parentParts, 0, "", ChangeType.Changed, nextVersion, timestamp))
                {
                    yield return entry;
                }
                foreach (var entry in EnumerateHierarchicalDeltaEntries(deltaIndex.Root, parentParts, 0, "", ChangeType.Deleted, nextVersion, timestamp, deletedEntityNames))
                {
                    yield return entry;
                }
            }
            else
            {
                // Fallback to flat structure with StartsWith
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

        if (deltaIndex.HasHierarchicalEntries())
        {
            var (entity, isDeleted) = FindEntityDataInHierarchicalDelta(deltaIndex.Root, name);
            if (isDeleted)
            {
                return null;
            }
            if (entity != null)
            {
                var data = await _storageProvider.ReadDataFromSources([(ObjectNameTypeEnum.Delta, version, entity.Position)], entity.Length);
                return (name, data);
            }
        }
        else
        {
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
        }

        throw new ArgumentException($"Entity {name} was not changed in version {version}");
    }
}
