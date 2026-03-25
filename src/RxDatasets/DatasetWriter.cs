
using System.Collections.Concurrent;
using System.Reactive.Disposables;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Models;
using RxDatasets.Notifications;

namespace RxDatasets;

public class DatasetWriter<T> : IDatasetWriter<T>, IDisposable
{
    private readonly ILogger<DatasetWriter<T>> _logger;
    private readonly DatasetHelper _datasetHelper;
    private readonly IStorageProvider<T> _storageProvider;
    private readonly IOptions<WriterConfig> _writerConfig;
    private ImmutableDatasetState? _index;
    private ImmutableDatasetState? _indexBasedOnMostRecentSnapshot;

    protected ILogger Logger => _logger;
    protected IReadOnlyStorageProvider<T> ReadOnlyStorageProvider => _storageProvider;
    protected ImmutableDatasetState? CurrentIndex => _index;
    protected ImmutableDatasetState? MostRecentSnapshotIndex => _indexBasedOnMostRecentSnapshot;

    /// <summary>
    /// Called after a delta has been computed and persisted, but before <c>_index</c> is
    /// swapped to the new state. Override to react to changes (e.g. update caches)
    /// while the old index is still visible to concurrent readers.
    /// </summary>
    protected virtual Task OnDeltaApplied(DeltaIndex deltaIndex) => Task.CompletedTask;
    public Task? InitializationTask;
    private readonly SemaphoreSlim _semaphore = new(1);
    private readonly CompositeDisposable _subscriptions = new();
    private readonly ConcurrentQueue<WriteIntent> _pendingWrites = new();
    private int _batchInProgress;

    public DatasetWriter(INotificationsHub<T> notificationsHub, ILogger<DatasetWriter<T>> logger, IStorageProvider<T> storageProvider, IOptions<WriterConfig> writerConfig, bool retryInitialization = true)
    {
        _storageProvider = storageProvider;
        _writerConfig = writerConfig;
        _logger = logger;
        _datasetHelper = new DatasetHelper(logger, _storageProvider);
        _subscriptions.Add(notificationsHub.Subscribe<SnapshotCreatedNotification>(OnSnapshotCreated));
        _subscriptions.Add(notificationsHub.Subscribe<StorageProviderReadinessNotifcation>(async (notification) =>
        {
            if (notification.ServiceStatus == ServiceStatus.NotReady)
            {
                Uninitialize();
                return;
            }

            InitializationTask = InitializationLoop();
            await InitializationTask;
        }));


    }

    private void Uninitialize()
    {
        InitializationTask = null;
        _index = null;
        _indexBasedOnMostRecentSnapshot = null;
    }

    public virtual void Dispose()
    {
        _subscriptions.Dispose();
    }

    private async Task OnSnapshotCreated(SnapshotCreatedNotification snapshotCreatedNotification)
    {
        if (_indexBasedOnMostRecentSnapshot == null
            || snapshotCreatedNotification.Version > _indexBasedOnMostRecentSnapshot.SnapshotVersion)
        {
            _indexBasedOnMostRecentSnapshot = await _datasetHelper.GetLatestSnapshotIndex();
        }
    }

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
                _logger.LogError(ex, "Failed to initialize Writer for {EntityType} Dataset", typeof(T).Name);
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
                _indexBasedOnMostRecentSnapshot = await _datasetHelper.GetLatestSnapshotIndex();
            }
            catch (FileNotFoundException)
            {
                //ignored as we dont have a snapshot yet
            }

            _logger.LogInformation($"Initialized DatasetWriter {typeof(T).Name} Dataset with latest version {_index.LatestVersion}");
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Failed to initialize Writer for {EntityType} Dataset", typeof(T).Name);
            throw;
        }
    }

    private void ThrowIfNotInitialized()
    {
        if (InitializationTask == null)
        {
            _logger.LogError("Received a request, however we are not ready yet");
            throw new InvalidOperationException("Initialization is not yet completed");
        }

        if (!InitializationTask.IsCompleted)
        {
            _logger.LogError("Received a request before dataset it was initialized");
            throw new InvalidOperationException("Initialization is not yet completed");
        }

        if (InitializationTask.IsFaulted)
        {
            _logger.LogError(InitializationTask.Exception, "Received a request, but the dataset initialization failed");
            throw new InvalidOperationException("Initialization failed", InitializationTask.Exception);
        }
    }

    public async Task<IDatasetWriter<T>.WriteResponse> Delete(string name, CancellationToken ct = default)
    {
        ThrowIfNotInitialized();
        if (_writerConfig.Value.EnableWriteCoalescing)
            return await EnqueueAndAwait(new WriteIntent { Operations = [(name, null)], Completion = new(), CancellationToken = ct });

        using var slot = await Lock(ct);
        if (!_index!.Entities.TryGetValue(name, out var entity) || entity.GetLatestChangeType() == ChangeType.Deleted)
        {
            return new IDatasetWriter<T>.WriteResponse()
            {
                NewDatasetVersion = _index.LatestVersion
            };
        }

        var delta = new Delta([(name, null, ChangeType.Deleted)], _index.LatestVersion + 1);
        await ApplyDelta(delta);

        _logger.LogInformation("Deleted 1 entity; version {NewVersion}", _index.LatestVersion);
        return WriteResponseFor(delta);
    }

    public async Task<IDatasetWriter<T>.WriteResponse> DeleteMany(IAsyncEnumerable<string> names, CancellationToken ct = default)
    {
        ThrowIfNotInitialized();
        if (_writerConfig.Value.EnableWriteCoalescing)
        {
            var materializedNames = new List<(string Name, byte[]? EntityBytes)>();
            await foreach (var name in names.WithCancellation(ct))
                materializedNames.Add((name, null));
            return await EnqueueAndAwait(new WriteIntent { Operations = materializedNames.ToArray(), Completion = new(), CancellationToken = ct });
        }

        using var slot = await Lock(ct);

        var deletedNames = new List<string>(_index!.Entities.Count);

        await foreach (var name in names.WithCancellation(ct))
        {
            if (!_index.Entities.TryGetValue(name, out var existingEntity) ||
                existingEntity.GetLatestChangeType() == ChangeType.Deleted)
            {
                continue;
            }

            deletedNames.Add(name);
        }

        if (!deletedNames.Any())
        {
            return new IDatasetWriter<T>.WriteResponse()
            {
                NewDatasetVersion = _index.LatestVersion
            };
        }

        var deleteChanges = deletedNames.Select(nm => (nm, (byte[]?)null, ChangeType.Deleted)).ToArray();

        var delta = new Delta(deleteChanges, _index.LatestVersion + 1);
        await ApplyDelta(delta);

        _logger.LogInformation("Deleted {Count} entities; version {NewVersion}", deleteChanges.Length, _index.LatestVersion);
        return WriteResponseFor(delta);
    }

    public async Task<IDatasetWriter<T>.WriteResponse> DeleteChildren(string ancestor, CancellationToken ct = default)
    {
        var prefix = ancestor.EndsWith("/", StringComparison.Ordinal) ? ancestor : ancestor + "/";
        var splitAncestor = prefix.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (splitAncestor.Length == 0 || splitAncestor.Length % 2 != 0)
            throw new ArgumentException("Invalid ancestor, an ancestor must have an even number of elements");

        ThrowIfNotInitialized();
        if (_writerConfig.Value.EnableWriteCoalescing)
            return await EnqueueAndAwait(new WriteIntent { DeleteChildrenAncestor = prefix, Completion = new(), CancellationToken = ct });

        using var slot = await Lock(ct);

        var deletedNames = new List<string>(_index!.Entities.Count);

        foreach (var (name, entity) in _index!.Entities)
        {
            if (!name.StartsWith(prefix, StringComparison.Ordinal))
                continue;

            if (entity.GetLatestChangeType() == ChangeType.Deleted)
                continue;

            deletedNames.Add(name);
        }

        if (!deletedNames.Any())
        {
            return new IDatasetWriter<T>.WriteResponse()
            {
                NewDatasetVersion = _index.LatestVersion
            };
        }

        var deleteChanges = deletedNames.Select(nm => (nm, (byte[]?)null, ChangeType.Deleted)).ToArray();

        var delta = new Delta(deleteChanges, _index.LatestVersion + 1);
        await ApplyDelta(delta);

        _logger.LogInformation("Deleted {Count} descendants of '{Prefix}'; version {NewVersion}", deleteChanges.Length, prefix, _index.LatestVersion);
        return WriteResponseFor(delta);
    }

    private void EnsureNameIsValid(string entityName)
    {
        if (string.IsNullOrEmpty(entityName))
            throw new ArgumentException("Entity name cannot be null or empty", nameof(entityName));

        if (_writerConfig.Value.AllowedNamesRegex is null)
            return;

        var result = _writerConfig.Value.AllowedNamesRegex.Match(entityName);
        if (result.Success)
            return;

        throw new ArgumentOutOfRangeException(nameof(entityName), entityName, $"Validation failed for entity name validation regex {_writerConfig.Value.AllowedNamesRegex}");
    }

    public async Task<IDatasetWriter<T>.WriteResponse> Put(string name, byte[] entityBytes, CancellationToken ct = default)
    {
        ThrowIfNotInitialized();
        EnsureNameIsValid(name);
        if (_writerConfig.Value.EnableWriteCoalescing)
            return await EnqueueAndAwait(new WriteIntent { Operations = [(name, entityBytes)], Completion = new(), CancellationToken = ct });

        using var slot = await Lock(ct);
        Delta delta;

        if (!_index!.Entities.TryGetValue(name, out var existingEntity) || existingEntity.GetLatestChangeType() == ChangeType.Deleted)
            delta = new Delta([(name, entityBytes, ChangeType.New)], _index.LatestVersion + 1);

        else if (await IsByteArrayUpdated(name, entityBytes))
            delta = new Delta([(name, entityBytes, ChangeType.Changed)], _index.LatestVersion + 1);

        else
            return new IDatasetWriter<T>.WriteResponse()
            {
                NewDatasetVersion = _index.LatestVersion
            };

        await ApplyDelta(delta);

        _logger.LogInformation("Put 1 entity; version {NewVersion}", _index.LatestVersion);
        return WriteResponseFor(delta);
    }

    public async Task<IDatasetWriter<T>.WriteResponse> PutAll(IAsyncEnumerable<(string name, byte[] entityBytes)> entities, CancellationToken ct = default)
        => await PutEntitiesAsync(entities.Select(e => (e.name, (byte[]?)e.entityBytes)), includeParents: false, performImplicitDeletes: true, ct);

    public async Task<IDatasetWriter<T>.WriteResponse> PutAllChildren(IAsyncEnumerable<(string name, byte[] entityBytes)> entities, CancellationToken ct = default)
        => await PutEntitiesAsync(entities.Select(e => (e.name, (byte[]?)e.entityBytes)), includeParents: true, performImplicitDeletes: true, ct);

    public async Task<IDatasetWriter<T>.WriteResponse> PutMany(IAsyncEnumerable<(string name, byte[] entityBytes)> entities, CancellationToken ct = default)
    {
        ThrowIfNotInitialized();
        if (_writerConfig.Value.EnableWriteCoalescing)
        {
            var materialized = new List<(string Name, byte[]? EntityBytes)>();
            await foreach (var (name, entityBytes) in entities.WithCancellation(ct))
            {
                EnsureNameIsValid(name);
                materialized.Add((name, entityBytes));
            }
            return await EnqueueAndAwait(new WriteIntent { Operations = materialized.ToArray(), Completion = new(), CancellationToken = ct });
        }
        return await PutEntitiesAsync(entities.Select(e => (e.name, (byte[]?)e.entityBytes)), includeParents: false, performImplicitDeletes: false, ct);
    }

    public async Task<IDatasetWriter<T>.WriteResponse> PutOrDeleteMany(IAsyncEnumerable<(string name, byte[]? entityBytes)> changes, CancellationToken ct = default)
    {
        ThrowIfNotInitialized();
        if (_writerConfig.Value.EnableWriteCoalescing)
        {
            var materialized = new List<(string Name, byte[]? EntityBytes)>();
            await foreach (var (name, entityBytes) in changes.WithCancellation(ct))
            {
                if (entityBytes != null)
                    EnsureNameIsValid(name);
                materialized.Add((name, entityBytes));
            }
            return await EnqueueAndAwait(new WriteIntent { Operations = materialized.ToArray(), Completion = new(), CancellationToken = ct });
        }
        return await PutEntitiesAsync(changes, includeParents: false, performImplicitDeletes: false, ct);
    }

    private async Task<IDatasetWriter<T>.WriteResponse> PutEntitiesAsync(
        IAsyncEnumerable<(string name, byte[]? entityBytes)> entities,
        bool includeParents,
        bool performImplicitDeletes,
        CancellationToken ct)
    {
        using var slot = await Lock(ct);
        ThrowIfNotInitialized();

        var tempDataFilePath = DatasetHelper.GetTempFilePath();
        await using var fileStream = new FileStream(tempDataFilePath, FileMode.Create, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous);
        var (deltaIndex, receivedNames, allParents) = await CreateDeltaIndexFromAsyncEnumerable(_index!, entities, includeParents, fileStream, ct);

        // Handle deletes based on parent inclusion and only if deletes are allowed
        if (performImplicitDeletes)
        {
            IEnumerable<string> implicitDeletes;
            if (includeParents)
            {
                implicitDeletes = DatasetHelper.GetImplicitDeletesForParentNames(_index!, receivedNames, allParents);
            }
            else
            {
                implicitDeletes = DatasetHelper.GetImplicitDeletes(_index!, receivedNames);
            }

            foreach (var name in implicitDeletes)
            {
                if (_index!.Entities.TryGetValue(name, out var existingEntity))
                {
                    deltaIndex.DeletedEntities.Add(new DeletedEntity { Name = name, PreviousVersion = existingEntity.GetLatestEntityVersion() });
                }
            }
        }
        var hasEntityData = deltaIndex.NewEntities.Count > 0 || deltaIndex.ChangedEntities.Count > 0;
        if (hasEntityData)
        {
            await fileStream.WriteAsync(new byte[1], ct); // trailing byte sentinel only when data exists
            await fileStream.FlushAsync(ct);
        }
        fileStream.Close();

        if (deltaIndex.NewEntities.Count == 0 && deltaIndex.ChangedEntities.Count == 0 && deltaIndex.DeletedNames.Count == 0 && deltaIndex.DeletedEntities.Count == 0)
        {
            File.Delete(tempDataFilePath);
            return new IDatasetWriter<T>.WriteResponse
            {
                NewDatasetVersion = _index!.LatestVersion
            };
        }

        // Apply the delta and store it
        var newIndex = _index!.ApplyDelta(deltaIndex);
        using (var deltaDataStream = hasEntityData ? File.OpenRead(tempDataFilePath) : Stream.Null)
        {
            await _storageProvider.StoreDelta(newIndex.LatestVersion, NestedIndexHelper.SerializeDeltaIndex(deltaIndex), deltaDataStream);
        }
        File.Delete(tempDataFilePath); // Single deletion point (exists even if empty)

        await OnDeltaApplied(deltaIndex);
        _index = newIndex;

        _logger.LogInformation("Put entities: version {Version}; added {Added} changed {Changed} deleted {Deleted}", _index.LatestVersion, deltaIndex.NewEntities.Count, deltaIndex.ChangedEntities.Count, deltaIndex.DeletedEntities.Count);

        return new IDatasetWriter<T>.WriteResponse
        {
            NewDatasetVersion = _index.LatestVersion,
            NumAddedEntities = (uint)deltaIndex.NewEntities.Count,
            NumUpdatedEntities = (uint)deltaIndex.ChangedEntities.Count,
            NumDeletedEntities = (uint)deltaIndex.DeletedEntities.Count
        };
    }

    private async Task<(DeltaIndex deltaIndex, HashSet<string> receivedNames, HashSet<string> allParents)> CreateDeltaIndexFromAsyncEnumerable(
        ImmutableDatasetState index,
        IAsyncEnumerable<(string name, byte[]? entityBytes)> entities,
        bool includeParents,
        Stream stream,
        CancellationToken ct)
    {
        var deltaIndex = new DeltaIndex { DeltaVersion = index.LatestVersion + 1, DeltaVersionLegacy = 0 };
        var receivedNames = new HashSet<string>();
        var allParents = new HashSet<string>();

        // Track final state per entity - last operation wins
        var entityFinalStates = new Dictionary<string, (uint crc32, long position, int length)?>();
        long byteArrayPosition = 0;

        await foreach (var (name, entityBytes) in entities.WithCancellation(ct))
        {
            try
            {
                EnsureNameIsValid(name);
            }
            catch (ArgumentOutOfRangeException ex)
            {
                _logger.LogWarning(ex, "Skipping entity");
                continue;
            }

            if (includeParents)
            {
                var parent = DatasetHelper.FindParentOfEntity(name);
                if (string.IsNullOrEmpty(parent))
                    continue;
                allParents.Add(parent);
            }

            receivedNames.Add(name);

            if (entityBytes == null)
            {
                // Delete operation - store null state
                entityFinalStates[name] = null;
            }
            else
            {
                // Put operation - compute and store CRC32, write to stream
                var crc32 = DatasetHelper.ComputeCrc32(entityBytes);

                var lengthOfTheVarPrefix = DatasetHelper.WriteProtobufLengthPrefix(stream, entityBytes.Length);
                byteArrayPosition += lengthOfTheVarPrefix;

                // Store final state for this entity
                entityFinalStates[name] = (crc32, byteArrayPosition, entityBytes.Length);

                await stream.WriteAsync(entityBytes, ct);
                byteArrayPosition += entityBytes.Length;
            }
        }

        deltaIndex.Timestamp = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow);

        // Now determine net effect for each entity using CRC32 comparison with index
        foreach (var (name, finalState) in entityFinalStates)
        {
            // Get original state from index
            var hasExistingEntity = index.Entities.TryGetValue(name, out var existingEntity);
            var existingIsDeleted = hasExistingEntity && existingEntity!.GetLatestChangeType() == ChangeType.Deleted;
            ulong previousVersion = hasExistingEntity ? existingEntity!.GetLatestEntityVersion() : 0;

            if (finalState == null)
            {
                // Final state is delete
                if (hasExistingEntity && !existingIsDeleted)
                {
                    deltaIndex.DeletedEntities.Add(new DeletedEntity { Name = name, PreviousVersion = previousVersion });
                }
                continue;
            }

            // Final state is put - compare with original CRC32 from index
            var (finalCrc, position, length) = finalState.Value;
            ChangeType changeType;
            if (!hasExistingEntity || existingIsDeleted)
            {
                changeType = ChangeType.New;
            }
            else
            {
                // Compare final CRC32 with original CRC32 from index
                var originalCrc = existingEntity!.Crc32;
                changeType = (originalCrc != 0 && originalCrc != finalCrc)
                    ? ChangeType.Changed
                    : ChangeType.None;

                // For entities without CRC32 (legacy), assume it's changed
                if (originalCrc == 0)
                    changeType = ChangeType.Changed;
            }

            if (changeType == ChangeType.None)
                continue;

            var deltaEntity = new DeltaEntity
            {
                Position = position,
                Length = length,
                Name = name,
                Crc32 = finalCrc,
                PreviousVersion = previousVersion
            };

            if (changeType == ChangeType.New)
                deltaIndex.NewEntities.Add(deltaEntity);
            else
                deltaIndex.ChangedEntities.Add(deltaEntity);
        }

        return (deltaIndex, receivedNames, allParents);
    }

    private async Task<ChangeType> DetermineChangeType(string name, byte[] entityBytes)
    {
        if (!_index!.Entities.TryGetValue(name, out var existingEntity) ||
            existingEntity.GetLatestChangeType() == ChangeType.Deleted)
        {
            return ChangeType.New;
        }

        return await IsByteArrayUpdated(name, entityBytes) ? ChangeType.Changed : ChangeType.None;
    }

    private async Task<bool> IsByteArrayUpdated(string name, byte[] entityBytes)
    {
        var existingEntity = _index!.Entities[name];
        if (existingEntity.Length != entityBytes.Length)
            return true;

        var newHashCode = DatasetHelper.ComputeCrc32(entityBytes);

        // If we have a stored CRC (non-zero), trust it completely
        if (existingEntity.Crc32 != 0)
            return existingEntity.Crc32 != newHashCode;

        // If stored CRC is 0 (legacy), always compare bytes
        var existingBytes = await ReadEntityBytes(name);
        return !existingBytes.Span.SequenceEqual(entityBytes);
    }

    private async Task<ReadOnlyMemory<byte>> ReadEntityBytes(string name)
    {
        // Precondition: calling method MUST have checked it exists AND is not deleted.
        var entityInMainIndex = _index!.Entities[name];
        var entityVersion = entityInMainIndex.GetLatestEntityVersion();

        List<(ObjectNameTypeEnum type, ulong version, long position)> sources = new(2);

        if (_indexBasedOnMostRecentSnapshot != null && entityVersion <= _indexBasedOnMostRecentSnapshot.SnapshotVersion)
        {
            // At this point, we KNOW it must be in there and be the latest version in existence,
            // because the latest version is smaller/equal to the snapshot,
            // AND it's not deleted (which should have been checked in the calling method)
            var entityFromSnapshot = _indexBasedOnMostRecentSnapshot.Entities[name];
            sources.Add((ObjectNameTypeEnum.Snapshot, _indexBasedOnMostRecentSnapshot.SnapshotVersion,
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

    private async Task<ImmutableDatasetState> ApplyDelta(Delta delta)
    {
        var deltaIndex = new DeltaIndex()
        {
            DeltaVersionLegacy = 0,
            DeltaVersion = delta.DatasetVersion,
            Timestamp = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow)
        };

        await using var concatenatedByteStream = new MemoryStream();
        long targetPosition = 0;

        foreach (var (name, entityBytes, change, crc32) in delta.Changes)
        {
            ulong previousVersion = 0;
            if (_index!.Entities.TryGetValue(name, out var existingEntity))
            {
                previousVersion = existingEntity.GetLatestEntityVersion();
            }

            switch (change)
            {
                case ChangeType.New:
                    var lengthOfTheVarPrefix = DatasetHelper.WriteProtobufLengthPrefix(concatenatedByteStream, entityBytes!.Length);
                    targetPosition += lengthOfTheVarPrefix;
                    deltaIndex.NewEntities.Add(new DeltaEntity()
                    {
                        Position = targetPosition,
                        Length = entityBytes.Length,
                        Name = name,
                        Crc32 = crc32,
                        PreviousVersion = previousVersion
                    });
                    concatenatedByteStream.Write(entityBytes);
                    targetPosition += entityBytes.Length;
                    break;
                case ChangeType.Changed:
                    lengthOfTheVarPrefix = DatasetHelper.WriteProtobufLengthPrefix(concatenatedByteStream, entityBytes!.Length);
                    targetPosition += lengthOfTheVarPrefix;
                    deltaIndex.ChangedEntities.Add(new DeltaEntity()
                    {
                        Position = targetPosition,
                        Length = entityBytes.Length,
                        Name = name,
                        Crc32 = crc32,
                        PreviousVersion = previousVersion
                    });
                    concatenatedByteStream.Write(entityBytes);
                    targetPosition += entityBytes.Length;
                    break;
                case ChangeType.Deleted:
                    deltaIndex.DeletedEntities.Add(new DeletedEntity
                    {
                        Name = name,
                        PreviousVersion = previousVersion
                    });
                    break;
            }
        }

        concatenatedByteStream.Position = 0;
        await _storageProvider.StoreDelta(deltaIndex.DeltaVersion, NestedIndexHelper.SerializeDeltaIndex(deltaIndex), concatenatedByteStream);

        var newIndex = _index!.ApplyDelta(deltaIndex);
        await OnDeltaApplied(deltaIndex);
        _index = newIndex;

        return _index;
    }


    public async Task<HealthCheckResult> CheckHealthAsync(CancellationToken cancellationToken = default)
    {
        if (InitializationTask == null)
        {
            return HealthCheckResult.Healthy("DatasetWriter: Initialization task is not yet started");
        }

        return await GetHealthCheckResult();
    }

    private async Task<HealthCheckResult> GetHealthCheckResult()
    {
        if (!InitializationTask!.IsCompletedSuccessfully)
            return HealthCheckResult.Unhealthy("DatasetWriter: Initialization task is not completed");

        try
        {
            await _storageProvider.ListVersions();
        }
        catch (Exception e)
        {
            return HealthCheckResult.Unhealthy("DatasetWriter: Failed to get latest versions from dataset", e);
        }

        return HealthCheckResult.Healthy();
    }

    public async Task<HealthCheckResult> CheckReadinessAsync(CancellationToken cancellationToken = default)
    {
        if (InitializationTask == null)
        {
            return HealthCheckResult.Unhealthy("DatasetWriter: Initialization task is not yet started");
        }

        return await GetHealthCheckResult();
    }

    #region Write Coalescing

    private async Task<IDatasetWriter<T>.WriteResponse> EnqueueAndAwait(WriteIntent intent)
    {
        // Fast path: no write in progress — execute directly, skip queue/TCS overhead
        if (await _semaphore.WaitAsync(0).ConfigureAwait(false))
        {
            try
            {
                var (mergedChanges, perIntentResponses) = MergeIntents([intent], _index!);
                if (mergedChanges.Length == 0)
                    return perIntentResponses[0] with { NewDatasetVersion = _index!.LatestVersion };

                var delta = new Delta(mergedChanges, _index!.LatestVersion + 1);
                await ApplyDelta(delta);
                return perIntentResponses[0] with { NewDatasetVersion = _index!.LatestVersion };
            }
            finally
            {
                _semaphore.Release();
            }
        }

        // Slow path: another write holds the semaphore — enqueue for coalescing
        _pendingWrites.Enqueue(intent);

        if (Interlocked.CompareExchange(ref _batchInProgress, 1, 0) == 0)
        {
            await DrainAndFlush();
        }

        return await intent.Completion.Task;
    }

    private async Task DrainAndFlush()
    {
        while (true)
        {
            // Quick exit check before acquiring the semaphore
            if (_pendingWrites.IsEmpty)
            {
                Interlocked.Exchange(ref _batchInProgress, 0);

                // Double-check: items may have been enqueued after our check but before we released leader
                if (!_pendingWrites.IsEmpty && Interlocked.CompareExchange(ref _batchInProgress, 1, 0) == 0)
                    continue;

                return;
            }

            // Acquire the semaphore first, THEN drain — this captures everything
            // that arrived while we were waiting for the current write to finish.
            await _semaphore.WaitAsync().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
            var batch = new List<WriteIntent>();
            try
            {
                while (_pendingWrites.TryDequeue(out var intent))
                {
                    if (intent.CancellationToken.IsCancellationRequested)
                    {
                        intent.Completion.TrySetCanceled(intent.CancellationToken);
                        continue;
                    }
                    batch.Add(intent);
                }

                if (batch.Count == 0)
                    continue;
                var (mergedChanges, perIntentResponses) = MergeIntents(batch, _index!);

                if (mergedChanges.Length == 0)
                {
                    var currentVersion = _index!.LatestVersion;
                    for (int i = 0; i < batch.Count; i++)
                    {
                        batch[i].Completion.TrySetResult(perIntentResponses[i] with { NewDatasetVersion = currentVersion });
                    }
                    continue;
                }

                var delta = new Delta(mergedChanges, _index!.LatestVersion + 1);
                await ApplyDelta(delta);

                var version = _index!.LatestVersion;
                if (batch.Count > 1)
                    _logger.LogInformation("Coalesced {Count} write intents into single delta; version {Version}", batch.Count, version);

                for (int i = 0; i < batch.Count; i++)
                {
                    batch[i].Completion.TrySetResult(perIntentResponses[i] with { NewDatasetVersion = version });
                }
            }
            catch (Exception ex)
            {
                foreach (var intent in batch)
                    intent.Completion.TrySetException(ex);
            }
            finally
            {
                _semaphore.Release();
            }
        }
    }

    internal static (
        (string Name, byte[]? Entity, ChangeType Change)[] MergedChanges,
        IDatasetWriter<T>.WriteResponse[] PerIntentResponses
    ) MergeIntents(List<WriteIntent> intents, ImmutableDatasetState index)
    {
        var responses = new IDatasetWriter<T>.WriteResponse[intents.Count];

        // Virtual state tracks entities modified by intents processed so far.
        // Value: (Bytes, Crc32, Exists) — null entry means deleted in virtual state
        var virtualState = new Dictionary<string, (byte[]? Bytes, uint Crc32, bool Exists)>();

        for (int i = 0; i < intents.Count; i++)
        {
            uint added = 0, changed = 0, deleted = 0;
            var intent = intents[i];

            if (intent.DeleteChildrenAncestor != null)
            {
                // Resolve DeleteChildren against virtual + real index
                var prefix = intent.DeleteChildrenAncestor;

                // First, check entities in the real index
                foreach (var (name, entity) in index.Entities)
                {
                    if (!name.StartsWith(prefix, StringComparison.Ordinal))
                        continue;

                    // Check virtual state first
                    if (virtualState.TryGetValue(name, out var vs))
                    {
                        if (vs.Exists)
                        {
                            deleted++;
                            virtualState[name] = (null, 0, false);
                        }
                    }
                    else if (entity.GetLatestChangeType() != ChangeType.Deleted)
                    {
                        deleted++;
                        virtualState[name] = (null, 0, false);
                    }
                }

                // Also check entities that were added by prior intents but don't exist in the real index
                foreach (var (name, vs) in virtualState)
                {
                    if (!name.StartsWith(prefix, StringComparison.Ordinal))
                        continue;
                    if (!vs.Exists)
                        continue;
                    if (index.Entities.ContainsKey(name))
                        continue; // Already handled above

                    deleted++;
                    virtualState[name] = (null, 0, false);
                }
            }
            else if (intent.Operations != null)
            {
                // Last-write-wins within a single intent: compute final state per entity first,
                // then compare against the virtual index state from before this intent.
                // This matches the original PutOrDeleteMany behavior where delete+put of same
                // data correctly reports "no change".
                var intentFinalStates = new Dictionary<string, (byte[]? Bytes, uint Crc32)>();
                foreach (var (name, entityBytes) in intent.Operations)
                {
                    if (entityBytes == null)
                    {
                        intentFinalStates[name] = (null, 0);
                    }
                    else
                    {
                        intentFinalStates[name] = (entityBytes, DatasetHelper.ComputeCrc32(entityBytes));
                    }
                }

                // Now compare each entity's final state against the virtual index
                foreach (var (name, (finalBytes, finalCrc)) in intentFinalStates)
                {
                    bool currentlyExists;
                    uint currentCrc32;

                    if (virtualState.TryGetValue(name, out var vs))
                    {
                        currentlyExists = vs.Exists;
                        currentCrc32 = vs.Crc32;
                    }
                    else if (index.Entities.TryGetValue(name, out var existing)
                             && existing.GetLatestChangeType() != ChangeType.Deleted)
                    {
                        currentlyExists = true;
                        currentCrc32 = existing.Crc32;
                    }
                    else
                    {
                        currentlyExists = false;
                        currentCrc32 = 0;
                    }

                    if (finalBytes == null)
                    {
                        // Final state is delete
                        if (currentlyExists)
                        {
                            deleted++;
                            virtualState[name] = (null, 0, false);
                        }
                    }
                    else
                    {
                        // Final state is put
                        if (!currentlyExists)
                        {
                            added++;
                            virtualState[name] = (finalBytes, finalCrc, true);
                        }
                        else if (currentCrc32 == 0 || currentCrc32 != finalCrc)
                        {
                            changed++;
                            virtualState[name] = (finalBytes, finalCrc, true);
                        }
                        else
                        {
                            // CRC match — no change, but still track in virtual state for consistency
                            virtualState[name] = (finalBytes, finalCrc, true);
                        }
                    }
                }
            }

            responses[i] = new IDatasetWriter<T>.WriteResponse
            {
                NumAddedEntities = added,
                NumUpdatedEntities = changed,
                NumDeletedEntities = deleted
            };
        }

        // Diff virtual state against real index to produce merged changes for storage
        var mergedChanges = new List<(string Name, byte[]? Entity, ChangeType Change)>();

        foreach (var (name, (bytes, crc, exists)) in virtualState)
        {
            bool realExists = index.Entities.TryGetValue(name, out var realEntity)
                              && realEntity.GetLatestChangeType() != ChangeType.Deleted;

            if (exists && !realExists)
            {
                mergedChanges.Add((name, bytes, ChangeType.New));
            }
            else if (exists && realExists)
            {
                var realCrc = realEntity!.Crc32;
                if (realCrc == 0 || realCrc != crc)
                    mergedChanges.Add((name, bytes, ChangeType.Changed));
            }
            else if (!exists && realExists)
            {
                mergedChanges.Add((name, null, ChangeType.Deleted));
            }
            // !exists && !realExists: no-op (entity was added then deleted within the batch, net zero)
        }

        return (mergedChanges.ToArray(), responses);
    }

    internal sealed class WriteIntent
    {
        public (string Name, byte[]? EntityBytes)[]? Operations { get; init; }
        public string? DeleteChildrenAncestor { get; init; }
        public required TaskCompletionSource<IDatasetWriter<T>.WriteResponse> Completion { get; init; }
        public CancellationToken CancellationToken { get; init; }
    }

    #endregion

    private async Task<IDisposable> Lock(CancellationToken cancellationToken)
    {
        await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        return new ActionDisposable(() => _semaphore.Release());
    }

    private static IDatasetWriter<T>.WriteResponse WriteResponseFor(Delta delta)
    {
        return new IDatasetWriter<T>.WriteResponse()
        {
            NewDatasetVersion = delta.DatasetVersion,
            NumAddedEntities = (uint)delta.Changes.Count(x => x.Change == ChangeType.New),
            NumDeletedEntities = (uint)delta.Changes.Count(x => x.Change == ChangeType.Deleted),
            NumUpdatedEntities = (uint)delta.Changes.Count(x => x.Change == ChangeType.Changed)
        };
    }


    private record Delta
    {
        public Delta((string Name, byte[]? Entity, ChangeType Change)[] Changes, ulong DatasetVersion)
        {
            this.Changes = Changes.Select(change => (change.Name, change.Entity, change.Change,
                DatasetHelper.ComputeCrc32(change.Entity ?? []))).ToArray();
            this.DatasetVersion = DatasetVersion;
        }

        public (string Name, byte[]? Entity, ChangeType Change, uint HashCode)[] Changes { get; init; }
        public ulong DatasetVersion { get; init; }

        public void Deconstruct(out (string Name, byte[]? Entity, ChangeType Change, uint HashCode)[] Changes, out ulong DatasetVersion)
        {
            Changes = this.Changes;
            DatasetVersion = this.DatasetVersion;
        }
    }
}
