
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Reactive.Disposables;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
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
    private readonly IReadOnlyList<AliasDefinition> _aliasDefinitions;
    private readonly string _aliasDefinitionsHash;
    private ImmutableDatasetState? _index;
    private ImmutableDatasetState? _indexBasedOnMostRecentSnapshot;

    protected ILogger Logger => _logger;
    protected IReadOnlyStorageProvider<T> ReadOnlyStorageProvider => _storageProvider;
    protected ImmutableDatasetState? CurrentIndex => _index;
    protected ImmutableDatasetState? MostRecentSnapshotIndex => _indexBasedOnMostRecentSnapshot;

    /// <summary>
    /// Called after a delta has been persisted and <c>_index</c> swapped to the new state, while the
    /// write lock is still held and before the write returns. Override to react to changes (e.g. update
    /// caches, wake waiters). The delta is on storage first, so readers never see an index entry whose
    /// data is missing.
    /// </summary>
    protected virtual Task OnDeltaApplied(DeltaIndex deltaIndex) => Task.CompletedTask;
    public Task? InitializationTask;
    private readonly SemaphoreSlim _semaphore = new(1);
    private readonly CompositeDisposable _subscriptions = new();
    private readonly ConcurrentQueue<WriteIntent> _pendingWrites = new();
    private int _batchInProgress;

    public DatasetWriter(INotificationsHub<T> notificationsHub, ILogger<DatasetWriter<T>> logger,
        IStorageProvider<T> storageProvider, IOptions<WriterConfig> writerConfig, bool retryInitialization = true,
        IOptions<AliasOptions<T>>? aliasOptions = null)
    {
        _storageProvider = storageProvider;
        _writerConfig = writerConfig;
        _aliasDefinitions = aliasOptions?.Value.Definitions ?? [];
        _aliasDefinitionsHash = AliasOptions<T>.HashOf(_aliasDefinitions);
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

            InitializationTask = retryInitialization ? InitializationLoop() : Initialize();
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

            if (_index.AliasDefinitionsHash != _aliasDefinitionsHash)
            {
                // A rebuild without definitions would drop every alias; keep failing until the host registers them
                if (_aliasDefinitions.Count == 0)
                {
                    throw new InvalidOperationException($"Dataset {typeof(T).Name} stores aliases (hash "
                        + $"{_index.AliasDefinitionsHash}) but this host registers no alias definitions; register them "
                        + "(AddDatasetAliases) before writing");
                }

                await RebuildAliases(_index);
            }

            _logger.LogInformation($"Initialized DatasetWriter {typeof(T).Name} Dataset with latest version {_index.LatestVersion}");
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Failed to initialize Writer for {EntityType} Dataset", typeof(T).Name);
            throw;
        }
    }

    /// <summary>
    /// Recomputes the aliases of every live entity after the alias definitions changed, reading the data
    /// sequentially, and stores the result as one ordinary delta: an alias change for every entity whose set changed,
    /// and the new definitions hash. Runs before the writer is ready; a crash before the delta is stored just means the
    /// next start does it again.
    /// </summary>
    /// <remarks>
    /// An alias that comes out on more than one entity goes to the newest entity version, then the lowest name; the
    /// others keep the rest of their aliases. Logged, and counted in <see cref="AliasMetrics.RebuildConflicts"/>. An
    /// entity the definitions can't evaluate gets no aliases; counted in <see cref="AliasMetrics.RebuildUnparseable"/>.
    /// </remarks>
    private async Task RebuildAliases(ImmutableDatasetState index)
    {
        // A new or empty dataset gets here on every start until its first write stores the hash
        _logger.Log(index.EnumerateAll(where: entity => entity.IsLive).Any() ? LogLevel.Warning : LogLevel.Information,
            "The alias definitions of {EntityType} changed; recomputing the aliases of all entities", typeof(T).Name);

        var entities = new List<(string Name, ulong Version, ImmutableArray<(string Name, string Value)> Aliases)>();
        await ReadAllLiveEntities(index, (name, entity, bytes) =>
        {
            ImmutableArray<(string Name, string Value)> aliases;
            try
            {
                aliases = AliasStaging.Evaluate(_aliasDefinitions, name, bytes);
            }
            catch (ArgumentException e)
            {
                _logger.LogError(e, "Entity {Name} gets no aliases", name);
                AliasMetrics.RebuildUnparseable.Add(1, new KeyValuePair<string, object?>("dataset", typeof(T).Name));
                aliases = [];
            }

            entities.Add((name, entity.GetLatestEntityVersion(), aliases));
        });

        if (entities.Count == 0)
            return; // nothing to store: the next delta carries the new hash

        var owners = new Dictionary<(string Name, string Value), string>();
        foreach (var (name, _, aliases) in entities.OrderByDescending(e => e.Version).ThenBy(e => e.Name, StringComparer.Ordinal))
        {
            foreach (var alias in aliases)
            {
                if (owners.TryAdd(alias, name))
                    continue;
                _logger.LogWarning("Alias {Alias} ({AliasName}) of {Name} stays with {Owner}", alias.Value, alias.Name, name,
                    owners[alias]);
                AliasMetrics.RebuildConflicts.Add(1, new KeyValuePair<string, object?>("dataset", typeof(T).Name));
            }
        }

        var delta = new DeltaIndex
        {
            DeltaVersion = index.LatestVersion + 1,
            Timestamp = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow),
            AliasDefinitionsHash = _aliasDefinitionsHash
        };
        foreach (var (name, _, aliases) in entities)
        {
            var before = index.GetEntity(name).Aliases;
            var after = aliases.Where(alias => owners[alias] == name).ToList();
            var added = after.Except(before).ToProto().ToList();
            var removed = before.Except(after).ToProto().ToList();
            if (added.Count > 0 || removed.Count > 0)
            {
                delta.AliasChanges.Add(new AliasOnlyChange
                {
                    Name = name, AddedAliases = { added }, RemovedAliases = { removed }
                });
            }
        }

        await _storageProvider.StoreDelta(delta.DeltaVersion, delta.ToByteArray(), Stream.Null);
        _index = index.ApplyDelta(delta);
        await OnDeltaApplied(delta);
        _logger.LogInformation("Recomputed the aliases of {Count} entities; {Changed} changed; version {Version}",
            entities.Count, delta.AliasChanges.Count, delta.DeltaVersion);
    }

    /// <summary>
    /// Reads every live entity: those in the snapshot the index starts from in one pass over its data file, as
    /// <see cref="SnapshotCreationPlugin{T}"/> does, the newer ones from their deltas.
    /// </summary>
    private async Task ReadAllLiveEntities(ImmutableDatasetState index,
        Action<string, ImmutableSnapshotEntity, ReadOnlyMemory<byte>> onEntity)
    {
        var live = index.EnumerateAll(where: entity => entity.IsLive).ToList();
        var inSnapshot = live.Where(e => e.Value.GetLatestEntityVersion() <= index.SnapshotVersion)
            .OrderBy(e => e.Value.Position).ToList();
        if (inSnapshot.Count > 0)
        {
            await _storageProvider.ReadSnapshotData(index.SnapshotVersion, stream =>
            {
                long position = 0;
                foreach (var (name, entity) in inSnapshot)
                {
                    position += SnapshotCreationPlugin<T>.SeekForwardBy(stream, entity.Position - position);
                    var bytes = new byte[entity.Length];
                    stream.ReadExactly(bytes);
                    position += entity.Length;
                    onEntity(name, entity, bytes);
                }
            });
        }

        // One range read per entity, as in CopyRelevantItemsFromDeltas; batching them per delta is an option if
        // first-deploy rebuilds turn out slow
        foreach (var (name, entity) in live.Where(e => e.Value.GetLatestEntityVersion() > index.SnapshotVersion))
        {
            var bytes = await _storageProvider.ReadDataFromSources(
                [(ObjectNameTypeEnum.Delta, entity.GetLatestEntityVersion(), entity.Position)], entity.Length);
            onEntity(name, entity, bytes);
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

    public async Task<IDatasetWriter<T>.WriteResponse> Delete(string name, string? trackingId, DateTimeOffset? slaTimestamp, CancellationToken ct = default)
    {
        ThrowIfNotInitialized();
        if (_writerConfig.Value.EnableWriteCoalescing)
            return await EnqueueAndAwait(new WriteIntent { Operations = [(name, null, trackingId, slaTimestamp)], Completion = new(), CancellationToken = ct });

        using var slot = await _semaphore.LockAsync(ct);
        if (!_index!.TryGetEntity(name, out var entity) || entity.GetLatestChangeType() == ChangeType.Deleted)
        {
            return new IDatasetWriter<T>.WriteResponse()
            {
                NewDatasetVersion = _index.LatestVersion
            };
        }

        var staging = NewAliasStaging();
        staging.Release(name);
        var delta = new Delta([(name, null, ChangeType.Deleted, trackingId, slaTimestamp)], _index.LatestVersion + 1);
        await ApplyDelta(delta, staging);

        _logger.LogInformation("Deleted 1 entity; version {NewVersion}", _index.LatestVersion);
        return WriteResponseFor(delta);
    }

    public async Task<IDatasetWriter<T>.WriteResponse> DeleteMany(IAsyncEnumerable<(string name, string? trackingId, DateTimeOffset? slaTimestamp)> names, CancellationToken ct = default)
    {
        ThrowIfNotInitialized();
        if (_writerConfig.Value.EnableWriteCoalescing)
        {
            var materializedNames = new List<(string Name, byte[]? EntityBytes, string? TrackingId, DateTimeOffset? SlaTimestamp)>();
            await foreach (var (name, trackingId, slaTimestamp) in names.WithCancellation(ct))
                materializedNames.Add((name, null, trackingId, slaTimestamp));
            return await EnqueueAndAwait(new WriteIntent { Operations = materializedNames.ToArray(), Completion = new(), CancellationToken = ct });
        }

        using var slot = await _semaphore.LockAsync(ct);

        var deletedNames = new List<(string Name, string? TrackingId, DateTimeOffset? SlaTimestamp)>();

        await foreach (var (name, trackingId, slaTimestamp) in names.WithCancellation(ct))
        {
            if (!_index!.TryGetEntity(name, out var existingEntity) ||
                existingEntity.GetLatestChangeType() == ChangeType.Deleted)
            {
                continue;
            }

            deletedNames.Add((name, trackingId, slaTimestamp));
        }

        if (!deletedNames.Any())
        {
            return new IDatasetWriter<T>.WriteResponse()
            {
                NewDatasetVersion = _index!.LatestVersion
            };
        }

        var deleteChanges = deletedNames.Select(nm => (nm.Name, (byte[]?)null, ChangeType.Deleted, nm.TrackingId, nm.SlaTimestamp)).ToArray();
        var staging = NewAliasStaging();
        foreach (var (name, _, _) in deletedNames)
            staging.Release(name);

        var delta = new Delta(deleteChanges, _index!.LatestVersion + 1);
        await ApplyDelta(delta, staging);

        _logger.LogInformation("Deleted {Count} entities; version {NewVersion}", deleteChanges.Length, _index.LatestVersion);
        return WriteResponseFor(delta);
    }

    public async Task<IDatasetWriter<T>.WriteResponse> DeleteChildren(string ancestor, string? trackingId, DateTimeOffset? slaTimestamp, CancellationToken ct = default)
    {
        var prefix = ancestor.EndsWith("/", StringComparison.Ordinal) ? ancestor : ancestor + "/";
        var splitAncestor = prefix.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (splitAncestor.Length == 0 || splitAncestor.Length % 2 != 0)
            throw new ArgumentException("Invalid ancestor, an ancestor must have an even number of elements");

        ThrowIfNotInitialized();
        if (_writerConfig.Value.EnableWriteCoalescing)
            return await EnqueueAndAwait(new WriteIntent { DeleteChildrenAncestor = prefix, TrackingId = trackingId, SlaTimestamp = slaTimestamp, Completion = new(), CancellationToken = ct });

        using var slot = await _semaphore.LockAsync(ct);

        var deletedNames = _index!.EnumerateAll(prefix, entity => entity.IsLive)
            .Select(entity => entity.Key)
            .Where(name => name.StartsWith(prefix, StringComparison.Ordinal)) // not the ancestor itself
            .ToList();

        if (!deletedNames.Any())
        {
            return new IDatasetWriter<T>.WriteResponse()
            {
                NewDatasetVersion = _index.LatestVersion
            };
        }

        var deleteChanges = deletedNames.Select(nm => (nm, (byte[]?)null, ChangeType.Deleted, trackingId, slaTimestamp)).ToArray();
        var staging = NewAliasStaging();
        foreach (var name in deletedNames)
            staging.Release(name);

        var delta = new Delta(deleteChanges, _index.LatestVersion + 1);
        await ApplyDelta(delta, staging);

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

    public async Task<IDatasetWriter<T>.WriteResponse> Put(string name, byte[] entityBytes, string? trackingId, DateTimeOffset? slaTimestamp, CancellationToken ct = default)
    {
        ThrowIfNotInitialized();
        EnsureNameIsValid(name);
        if (_writerConfig.Value.EnableWriteCoalescing)
            return await EnqueueAndAwait(new WriteIntent { Operations = [(name, entityBytes, trackingId, slaTimestamp)], Completion = new(), CancellationToken = ct });

        using var slot = await _semaphore.LockAsync(ct);
        Delta delta;
        uint crc = DatasetHelper.ComputeCrc32(entityBytes);

        if (!_index!.TryGetEntity(name, out var existingEntity) || existingEntity.GetLatestChangeType() == ChangeType.Deleted)
            delta = new Delta([(name, entityBytes, ChangeType.New, trackingId, slaTimestamp)], _index.LatestVersion + 1);

        else if (await IsByteArrayUpdated(name, entityBytes, crc))
            delta = new Delta([(name, entityBytes, ChangeType.Changed, trackingId, slaTimestamp)], _index.LatestVersion + 1);

        else
            return new IDatasetWriter<T>.WriteResponse()
            {
                NewDatasetVersion = _index.LatestVersion
            };

        var staging = NewAliasStaging();
        staging.Put(name, entityBytes, crc);
        await ApplyDelta(delta, staging);

        _logger.LogInformation("Put 1 entity; version {NewVersion}", _index.LatestVersion);
        return WriteResponseFor(delta);
    }

    public async Task<IDatasetWriter<T>.WriteResponse> PutAll(IAsyncEnumerable<(string name, byte[] entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)> entities, CancellationToken ct = default)
        => await PutEntitiesAsync(entities.Select(e => (e.name, (byte[]?)e.entityBytes, e.trackingId, e.slaTimestamp)), includeParents: false, performImplicitDeletes: true, ct);

    public async Task<IDatasetWriter<T>.WriteResponse> PutAllChildren(IAsyncEnumerable<(string name, byte[] entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)> entities, CancellationToken ct = default)
        => await PutEntitiesAsync(entities.Select(e => (e.name, (byte[]?)e.entityBytes, e.trackingId, e.slaTimestamp)), includeParents: true, performImplicitDeletes: true, ct);

    public async Task<IDatasetWriter<T>.WriteResponse> PutMany(IAsyncEnumerable<(string name, byte[] entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)> entities, CancellationToken ct = default)
    {
        ThrowIfNotInitialized();
        if (_writerConfig.Value.EnableWriteCoalescing)
        {
            var materialized = new List<(string Name, byte[]? EntityBytes, string? TrackingId, DateTimeOffset? SlaTimestamp)>();
            await foreach (var (name, entityBytes, trackingId, slaTimestamp) in entities.WithCancellation(ct))
            {
                EnsureNameIsValid(name);
                materialized.Add((name, entityBytes, trackingId, slaTimestamp));
            }
            return await EnqueueAndAwait(new WriteIntent { Operations = materialized.ToArray(), Completion = new(), CancellationToken = ct });
        }
        return await PutEntitiesAsync(entities.Select(e => (e.name, (byte[]?)e.entityBytes, e.trackingId, e.slaTimestamp)), includeParents: false, performImplicitDeletes: false, ct);
    }

    public async Task<IDatasetWriter<T>.WriteResponse> PutOrDeleteMany(IAsyncEnumerable<(string name, byte[]? entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)> changes, CancellationToken ct = default)
    {
        ThrowIfNotInitialized();
        if (_writerConfig.Value.EnableWriteCoalescing)
        {
            var materialized = new List<(string Name, byte[]? EntityBytes, string? TrackingId, DateTimeOffset? SlaTimestamp)>();
            await foreach (var (name, entityBytes, trackingId, slaTimestamp) in changes.WithCancellation(ct))
            {
                if (entityBytes != null)
                    EnsureNameIsValid(name);
                materialized.Add((name, entityBytes, trackingId, slaTimestamp));
            }
            return await EnqueueAndAwait(new WriteIntent { Operations = materialized.ToArray(), Completion = new(), CancellationToken = ct });
        }
        return await PutEntitiesAsync(changes, includeParents: false, performImplicitDeletes: false, ct);
    }

    private async Task<IDatasetWriter<T>.WriteResponse> PutEntitiesAsync(
        IAsyncEnumerable<(string name, byte[]? entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)> entities,
        bool includeParents,
        bool performImplicitDeletes,
        CancellationToken ct)
    {
        using var slot = await _semaphore.LockAsync(ct);
        ThrowIfNotInitialized();

        var tempDataFilePath = DatasetHelper.GetTempFilePath();
        try
        {
            return await PutEntitiesAsync(entities, includeParents, performImplicitDeletes, tempDataFilePath, ct);
        }
        finally
        {
            File.Delete(tempDataFilePath); // Single deletion point (exists even if empty, or when the write failed)
        }
    }

    private async Task<IDatasetWriter<T>.WriteResponse> PutEntitiesAsync(
        IAsyncEnumerable<(string name, byte[]? entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)> entities,
        bool includeParents,
        bool performImplicitDeletes,
        string tempDataFilePath,
        CancellationToken ct)
    {
        await using var fileStream = new FileStream(tempDataFilePath, FileMode.Create, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous);
        var staging = NewAliasStaging();
        if (performImplicitDeletes && !includeParents)
            staging.ReleaseAll(); // every existing entity is either sent again or implicitly deleted
        var (deltaIndex, receivedNames, allParents) =
            await CreateDeltaIndexFromAsyncEnumerable(_index!, entities, includeParents, fileStream, staging, ct);

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
                if (_index!.TryGetEntity(name, out var existingEntity))
                {
                    deltaIndex.DeletedEntities.Add(new DeletedEntity { Name = name, PreviousVersion = existingEntity.GetLatestEntityVersion() });
                }
            }
        }

        staging.AttachTo(deltaIndex);
        int added = deltaIndex.NewEntities.Count, changed = deltaIndex.ChangedEntities.Count,
            deleted = deltaIndex.DeletedEntities.Count;
        var hasEntityData = added > 0 || changed > 0;
        if (hasEntityData)
        {
            await fileStream.WriteAsync(new byte[1], ct); // trailing byte sentinel only when data exists
            await fileStream.FlushAsync(ct);
        }
        fileStream.Close();

        if (added == 0 && changed == 0 && deleted == 0 && deltaIndex.AliasChanges.Count == 0)
        {
            return new IDatasetWriter<T>.WriteResponse
            {
                NewDatasetVersion = _index!.LatestVersion
            };
        }

        // Apply the delta and store it
        deltaIndex.ConvertToTree();
        var newIndex = _index!.ApplyDelta(deltaIndex);
        using (var deltaDataStream = hasEntityData ? File.OpenRead(tempDataFilePath) : Stream.Null)
        {
            await _storageProvider.StoreDelta(newIndex.LatestVersion, deltaIndex.ToByteArray(), deltaDataStream);
        }

        _index = newIndex;
        await OnDeltaApplied(deltaIndex);

        _logger.LogInformation("Put entities: version {Version}; added {Added} changed {Changed} deleted {Deleted}",
            _index.LatestVersion, added, changed, deleted);

        return new IDatasetWriter<T>.WriteResponse
        {
            NewDatasetVersion = _index.LatestVersion,
            NumAddedEntities = (uint)added,
            NumUpdatedEntities = (uint)changed,
            NumDeletedEntities = (uint)deleted
        };
    }

    private async Task<(DeltaIndex deltaIndex, HashSet<string> receivedNames, HashSet<string> allParents)> CreateDeltaIndexFromAsyncEnumerable(
        ImmutableDatasetState index,
        IAsyncEnumerable<(string name, byte[]? entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)> entities,
        bool includeParents,
        Stream stream,
        AliasStaging staging,
        CancellationToken ct)
    {
        var deltaIndex = new DeltaIndex { DeltaVersion = index.LatestVersion + 1, DeltaVersionLegacy = 0 };
        var receivedNames = new HashSet<string>();
        var allParents = new HashSet<string>();

        // Track final state per entity - last operation wins
        var entityFinalStates = new Dictionary<string, (uint crc32, long position, int length)?>();
        var entityMetadata = new Dictionary<string, (string? trackingId, DateTimeOffset? slaTimestamp)>();
        long byteArrayPosition = 0;

        await foreach (var (name, entityBytes, trackingId, slaTimestamp) in entities.WithCancellation(ct))
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

            if (!entityMetadata.TryGetValue(name, out var currentMeta)) {
                entityMetadata[name] = (trackingId, slaTimestamp);
            } else {
                var currentSla = currentMeta.slaTimestamp;
                if (slaTimestamp.HasValue) {
                    if (!currentSla.HasValue || slaTimestamp.Value < currentSla.Value) {
                        entityMetadata[name] = (trackingId, slaTimestamp);
                    }
                } else if (!currentSla.HasValue && currentMeta.trackingId == null) {
                     entityMetadata[name] = (trackingId, null);
                }
            }

            if (includeParents)
            {
                var parent = DatasetHelper.FindParentOfEntity(name);
                if (string.IsNullOrEmpty(parent))
                    continue;
                // The first time a parent appears, its existing children free their aliases: each is either sent
                // again or implicitly deleted
                if (allParents.Add(parent))
                    staging.ReleaseChildren(parent);
            }

            uint crc32 = entityBytes == null ? 0 : DatasetHelper.ComputeCrc32(entityBytes);

            // In stream order, before entries with the same name are collapsed
            if (entityBytes == null)
                staging.Release(name);
            else
                staging.Put(name, entityBytes, crc32);

            receivedNames.Add(name);

            if (entityBytes == null)
            {
                // Delete operation - store null state
                entityFinalStates[name] = null;
            }
            else
            {
                // Put operation - store CRC32, write to stream
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
            var hasExistingEntity = index.TryGetEntity(name, out var existingEntity);
            var existingIsDeleted = hasExistingEntity && existingEntity!.GetLatestChangeType() == ChangeType.Deleted;
            ulong previousVersion = hasExistingEntity ? existingEntity!.GetLatestEntityVersion() : 0;

            if (finalState == null)
            {
                // Final state is delete
                if (hasExistingEntity && !existingIsDeleted)
                {
                    deltaIndex.DeletedEntities.Add(new DeletedEntity { Name = name, PreviousVersion = previousVersion, TrackingId = entityMetadata[name].trackingId ?? "", SlaTimestamp = entityMetadata[name].slaTimestamp.HasValue ? Timestamp.FromDateTimeOffset(entityMetadata[name].slaTimestamp!.Value) : null });
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
                PreviousVersion = previousVersion,
                TrackingId = entityMetadata[name].trackingId ?? "",
                SlaTimestamp = entityMetadata[name].slaTimestamp.HasValue ? Timestamp.FromDateTimeOffset(entityMetadata[name].slaTimestamp!.Value) : null
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
        if (!_index!.TryGetEntity(name, out var existingEntity) ||
            existingEntity.GetLatestChangeType() == ChangeType.Deleted)
        {
            return ChangeType.New;
        }

        return await IsByteArrayUpdated(name, entityBytes, DatasetHelper.ComputeCrc32(entityBytes))
            ? ChangeType.Changed
            : ChangeType.None;
    }

    private async Task<bool> IsByteArrayUpdated(string name, byte[] entityBytes, uint crc)
    {
        var existingEntity = _index!.GetEntity(name);
        if (existingEntity.Length != entityBytes.Length)
            return true;

        // If we have a stored CRC (non-zero), trust it completely
        if (existingEntity.Crc32 != 0)
            return existingEntity.Crc32 != crc;

        // If stored CRC is 0 (legacy), always compare bytes
        var existingBytes = await ReadEntityBytes(name);
        return !existingBytes.Span.SequenceEqual(entityBytes);
    }

    private async Task<ReadOnlyMemory<byte>> ReadEntityBytes(string name)
    {
        // Precondition: calling method MUST have checked it exists AND is not deleted.
        var entityInMainIndex = _index!.GetEntity(name);
        var entityVersion = entityInMainIndex.GetLatestEntityVersion();

        List<(ObjectNameTypeEnum type, ulong version, long position)> sources = new(2);

        if (_indexBasedOnMostRecentSnapshot != null && entityVersion <= _indexBasedOnMostRecentSnapshot.SnapshotVersion)
        {
            // At this point, we KNOW it must be in there and be the latest version in existence,
            // because the latest version is smaller/equal to the snapshot,
            // AND it's not deleted (which should have been checked in the calling method)
            var entityFromSnapshot = _indexBasedOnMostRecentSnapshot.GetEntity(name);
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

    private AliasStaging NewAliasStaging() => new(_index!, _aliasDefinitions, _aliasDefinitionsHash);

    private async Task<ImmutableDatasetState> ApplyDelta(Delta delta, AliasStaging staging)
    {
        var deltaIndex = new DeltaIndex()
        {
            DeltaVersionLegacy = 0,
            DeltaVersion = delta.DatasetVersion,
            Timestamp = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow)
        };

        await using var concatenatedByteStream = new MemoryStream();
        long targetPosition = 0;

        foreach (var (name, entityBytes, change, crc32, trackingId, slaTimestamp) in delta.Changes)
        {
            ulong previousVersion = 0;
            if (_index!.TryGetEntity(name, out var existingEntity))
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
                        PreviousVersion = previousVersion,
                        TrackingId = trackingId ?? "",
                        SlaTimestamp = slaTimestamp.HasValue ? Timestamp.FromDateTimeOffset(slaTimestamp.Value) : null
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
                        PreviousVersion = previousVersion,
                        TrackingId = trackingId ?? "",
                        SlaTimestamp = slaTimestamp.HasValue ? Timestamp.FromDateTimeOffset(slaTimestamp.Value) : null
                    });
                    concatenatedByteStream.Write(entityBytes);
                    targetPosition += entityBytes.Length;
                    break;
                case ChangeType.Deleted:
                    deltaIndex.DeletedEntities.Add(new DeletedEntity
                    {
                        Name = name,
                        PreviousVersion = previousVersion,
                        TrackingId = trackingId ?? "",
                        SlaTimestamp = slaTimestamp.HasValue ? Timestamp.FromDateTimeOffset(slaTimestamp.Value) : null
                    });
                    break;
            }
        }

        staging.AttachTo(deltaIndex);
        deltaIndex.ConvertToTree();
        concatenatedByteStream.Position = 0;
        await _storageProvider.StoreDelta(deltaIndex.DeltaVersion, deltaIndex.ToByteArray(), concatenatedByteStream);

        _index = _index!.ApplyDelta(deltaIndex);
        await OnDeltaApplied(deltaIndex);

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
            var batch = new List<WriteIntent>();
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
            {
                Interlocked.Exchange(ref _batchInProgress, 0);

                // Double-check: items may have been enqueued after drain but before we released leader
                if (!_pendingWrites.IsEmpty && Interlocked.CompareExchange(ref _batchInProgress, 1, 0) == 0)
                    continue;

                return;
            }

            using var slot = await _semaphore.LockAsync();
            try
            {
                var staging = NewAliasStaging();
                var (mergedChanges, perIntentResponses, errors) = MergeIntents(batch, _index!, staging);

                // An intent whose aliases conflict fails on its own; its changes are not in mergedChanges
                for (int i = 0; i < batch.Count; i++)
                {
                    if (errors[i] != null)
                        batch[i].Completion.TrySetException(errors[i]!);
                }

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
                await ApplyDelta(delta, staging);

                var version = _index!.LatestVersion;
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
        }
    }

    /// <summary>
    /// Merges the intents into one set of changes. Each intent's aliases are staged on their own, in order: an intent
    /// that fails them gets its exception in <c>Errors</c>, and its changes are left out.
    /// </summary>
    internal static (
        (string Name, byte[]? Entity, ChangeType Change, string? TrackingId, DateTimeOffset? SlaTimestamp)[] MergedChanges,
        IDatasetWriter<T>.WriteResponse[] PerIntentResponses,
        Exception?[] Errors
    ) MergeIntents(List<WriteIntent> intents, ImmutableDatasetState index, AliasStaging staging)
    {
        var responses = new IDatasetWriter<T>.WriteResponse[intents.Count];
        var errors = new Exception?[intents.Count];

        // Virtual state tracks entities modified by intents processed so far.
        // Value: (Bytes, Crc32, Exists, TrackingId, SlaTimestamp) — null entry means deleted in virtual state
        var virtualState = new Dictionary<string, (byte[]? Bytes, uint Crc32, bool Exists, string? TrackingId, DateTimeOffset? SlaTimestamp)>();

        for (int i = 0; i < intents.Count; i++)
        {
            uint added = 0, changed = 0, deleted = 0;
            var intent = intents[i];

            var crcs = intent.Operations?.Select(op => op.EntityBytes == null ? 0 : DatasetHelper.ComputeCrc32(op.EntityBytes))
                .ToArray();
            int mark = staging.Mark();
            try
            {
                StageAliases(intent, crcs, index, virtualState.Keys, staging);
            }
            catch (Exception e) when (e is AliasConflictException or ArgumentException)
            {
                staging.Rollback(mark);
                errors[i] = e;
                responses[i] = new IDatasetWriter<T>.WriteResponse();
                continue;
            }

            if (intent.DeleteChildrenAncestor != null)
            {
                // Resolve DeleteChildren against virtual + real index
                var prefix = intent.DeleteChildrenAncestor;

                // First, check entities in the real index
                foreach (var (name, entity) in index.EnumerateAll(prefix))
                {
                    if (!name.StartsWith(prefix, StringComparison.Ordinal))
                        continue;

                    // Check virtual state first
                    if (virtualState.TryGetValue(name, out var vs))
                    {
                        if (vs.Exists)
                        {
                            deleted++;
                            virtualState[name] = (null, 0, false, UpdateTrackingId(vs.TrackingId, vs.SlaTimestamp, intent.TrackingId, intent.SlaTimestamp), UpdateSlaTimestamp(vs.SlaTimestamp, intent.SlaTimestamp));
                        }
                    }
                    else if (entity.GetLatestChangeType() != ChangeType.Deleted)
                    {
                        deleted++;
                        virtualState[name] = (null, 0, false, intent.TrackingId, intent.SlaTimestamp);
                    }
                }

                // Also check entities that were added by prior intents but don't exist in the real index
                foreach (var (name, vs) in virtualState.ToList())
                {
                    if (!name.StartsWith(prefix, StringComparison.Ordinal))
                        continue;
                    if (!vs.Exists)
                        continue;
                    if (index.TryGetEntity(name, out _))
                        continue; // Already handled above

                    deleted++;
                    virtualState[name] = (null, 0, false, UpdateTrackingId(vs.TrackingId, vs.SlaTimestamp, intent.TrackingId, intent.SlaTimestamp), UpdateSlaTimestamp(vs.SlaTimestamp, intent.SlaTimestamp));
                }
            }
            else if (intent.Operations != null)
            {
                var intentFinalStates = new Dictionary<string, (byte[]? Bytes, uint Crc32, string? TrackingId, DateTimeOffset? SlaTimestamp)>();
                for (int op = 0; op < intent.Operations.Length; op++)
                {
                    var (name, entityBytes, trackingId, slaTimestamp) = intent.Operations[op];
                    if (entityBytes == null)
                    {
                        if (!intentFinalStates.TryGetValue(name, out var currentState))
                        {
                            intentFinalStates[name] = (null, 0, trackingId, slaTimestamp);
                        }
                        else
                        {
                            intentFinalStates[name] = (null, 0, UpdateTrackingId(currentState.TrackingId, currentState.SlaTimestamp, trackingId, slaTimestamp), UpdateSlaTimestamp(currentState.SlaTimestamp, slaTimestamp));
                        }
                    }
                    else
                    {
                        var crc = crcs![op];
                        if (!intentFinalStates.TryGetValue(name, out var currentState))
                        {
                            intentFinalStates[name] = (entityBytes, crc, trackingId, slaTimestamp);
                        }
                        else
                        {
                            intentFinalStates[name] = (entityBytes, crc, UpdateTrackingId(currentState.TrackingId, currentState.SlaTimestamp, trackingId, slaTimestamp), UpdateSlaTimestamp(currentState.SlaTimestamp, slaTimestamp));
                        }
                    }
                }

                // Now compare each entity's final state against the virtual index
                foreach (var (name, (finalBytes, finalCrc, finalTrackingId, finalSlaTimestamp)) in intentFinalStates)
                {
                    bool currentlyExists;
                    uint currentCrc32;

                    if (virtualState.TryGetValue(name, out var vs))
                    {
                        currentlyExists = vs.Exists;
                        currentCrc32 = vs.Crc32;
                    }
                    else if (index.TryGetEntity(name, out var existing)
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
                            var newTrackingId = vs.Exists ? UpdateTrackingId(vs.TrackingId, vs.SlaTimestamp, finalTrackingId, finalSlaTimestamp) : finalTrackingId;
                            var newSla = vs.Exists ? UpdateSlaTimestamp(vs.SlaTimestamp, finalSlaTimestamp) : finalSlaTimestamp;
                            virtualState[name] = (null, 0, false, newTrackingId, newSla);
                        }
                    }
                    else
                    {
                        // Final state is put
                        var newTrackingId = vs.Exists ? UpdateTrackingId(vs.TrackingId, vs.SlaTimestamp, finalTrackingId, finalSlaTimestamp) : finalTrackingId;
                        var newSla = vs.Exists ? UpdateSlaTimestamp(vs.SlaTimestamp, finalSlaTimestamp) : finalSlaTimestamp;
                        
                        if (!currentlyExists)
                        {
                            added++;
                            virtualState[name] = (finalBytes, finalCrc, true, newTrackingId, newSla);
                        }
                        else if (currentCrc32 == 0 || currentCrc32 != finalCrc)
                        {
                            changed++;
                            virtualState[name] = (finalBytes, finalCrc, true, newTrackingId, newSla);
                        }
                        else
                        {
                            // CRC match — no change, but still track in virtual state for consistency
                            virtualState[name] = (finalBytes, finalCrc, true, newTrackingId, newSla);
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
        var mergedChanges = new List<(string Name, byte[]? Entity, ChangeType Change, string? TrackingId, DateTimeOffset? SlaTimestamp)>();

        foreach (var (name, (bytes, crc, exists, trackingId, slaTimestamp)) in virtualState)
        {
            bool realExists = index.TryGetEntity(name, out var realEntity)
                              && realEntity.GetLatestChangeType() != ChangeType.Deleted;

            if (exists && !realExists)
            {
                mergedChanges.Add((name, bytes, ChangeType.New, trackingId, slaTimestamp));
            }
            else if (exists && realExists)
            {
                var realCrc = realEntity!.Crc32;
                if (realCrc == 0 || realCrc != crc)
                    mergedChanges.Add((name, bytes, ChangeType.Changed, trackingId, slaTimestamp));
            }
            else if (!exists && realExists)
            {
                mergedChanges.Add((name, null, ChangeType.Deleted, trackingId, slaTimestamp));
            }
            // !exists && !realExists: no-op (entity was added then deleted within the batch, net zero)
        }

        return (mergedChanges.ToArray(), responses, errors);
    }

    private static void StageAliases(WriteIntent intent, uint[]? crcs, ImmutableDatasetState index,
        IEnumerable<string> namesInBatch, AliasStaging staging)
    {
        if (intent.DeleteChildrenAncestor is { } prefix)
        {
            // Only entities with aliases have any to release
            foreach (var name in index.EnumerateAll(prefix, e => !e.Aliases.IsEmpty).Select(e => e.Key).Concat(namesInBatch))
            {
                if (name.StartsWith(prefix, StringComparison.Ordinal))
                    staging.Release(name);
            }

            return;
        }

        for (int op = 0; op < intent.Operations!.Length; op++)
        {
            var (name, entityBytes, _, _) = intent.Operations[op];
            if (entityBytes == null)
                staging.Release(name);
            else
                staging.Put(name, entityBytes, crcs![op]);
        }
    }

    private static string? UpdateTrackingId(string? currentTrackingId, DateTimeOffset? currentSla, string? newTrackingId, DateTimeOffset? newSla)
    {
        if (!newSla.HasValue) return currentSla.HasValue ? currentTrackingId : (currentTrackingId ?? newTrackingId);
        if (!currentSla.HasValue || newSla.Value < currentSla.Value) return newTrackingId;
        return currentTrackingId;
    }

    private static DateTimeOffset? UpdateSlaTimestamp(DateTimeOffset? currentSla, DateTimeOffset? newSla)
    {
        if (!newSla.HasValue) return currentSla;
        if (!currentSla.HasValue || newSla.Value < currentSla.Value) return newSla;
        return currentSla;
    }

    internal sealed class WriteIntent
    {
        public (string Name, byte[]? EntityBytes, string? TrackingId, DateTimeOffset? SlaTimestamp)[]? Operations { get; init; }
        public string? DeleteChildrenAncestor { get; init; }
        public string? TrackingId { get; init; }
        public DateTimeOffset? SlaTimestamp { get; init; }
        public required TaskCompletionSource<IDatasetWriter<T>.WriteResponse> Completion { get; init; }
        public CancellationToken CancellationToken { get; init; }
    }

    #endregion


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
        public Delta((string Name, byte[]? Entity, ChangeType Change, string? TrackingId, DateTimeOffset? SlaTimestamp)[] Changes, ulong DatasetVersion)
        {
            this.Changes = Changes.Select(change => (change.Name, change.Entity, change.Change,
                DatasetHelper.ComputeCrc32(change.Entity ?? []), change.TrackingId, change.SlaTimestamp)).ToArray();
            this.DatasetVersion = DatasetVersion;
        }

        public (string Name, byte[]? Entity, ChangeType Change, uint HashCode, string? TrackingId, DateTimeOffset? SlaTimestamp)[] Changes { get; init; }
        public ulong DatasetVersion { get; init; }

        public void Deconstruct(out (string Name, byte[]? Entity, ChangeType Change, uint HashCode, string? TrackingId, DateTimeOffset? SlaTimestamp)[] Changes, out ulong DatasetVersion)
        {
            Changes = this.Changes;
            DatasetVersion = this.DatasetVersion;
        }
    }
}
