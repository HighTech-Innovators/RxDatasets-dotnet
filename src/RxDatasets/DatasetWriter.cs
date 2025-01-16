
using Google.Protobuf;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Models;
using RxDatasets.Notifications;
using System.Reactive.Disposables;
using System.Text.RegularExpressions;

namespace RxDatasets;

public class DatasetWriter<T> : IDatasetWriter<T>, IDisposable
{
    private readonly ILogger<DatasetWriter<T>> _logger;
    private readonly DatasetHelper _datasetHelper;
    private readonly IStorageProvider<T> _storageProvider;
    private readonly IOptions<WriterConfig> _writerConfig;
    private ImmutableDatasetState? _index;
    private ImmutableDatasetState? _indexBasedOnMostRecentSnapshot;
    public Task? InitializationTask;
    private readonly SemaphoreSlim _semaphore = new(1);
    private readonly CompositeDisposable _subscriptions = new();

    public DatasetWriter(INotificationsHub<T> notificationsHub, ILogger<DatasetWriter<T>> logger, IStorageProvider<T> storageProvider, IOptions<WriterConfig> writerConfig, bool retryInitialization = true)
    {
        _storageProvider = storageProvider;
        _writerConfig = writerConfig;
        _logger = logger;
        _datasetHelper = new DatasetHelper(logger, _storageProvider);
        _retryInitialization = retryInitialization;
        _subscriptions.Add(notificationsHub.Subscribe<SnapshotCreatedNotification>(OnSnapshotCreated));
        _subscriptions.Add(notificationsHub.Subscribe<StorageProviderReadinessNotifcation>(async (notification) =>
        {
            if (notification.ServiceStatus == ServiceStatus.NotReady)
            {
                Uninitialize();
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

    public void Dispose()
    {
        _subscriptions?.Dispose();
    }

    private async Task OnSnapshotCreated(SnapshotCreatedNotification snapshotCreatedNotification)
    {
        if (_indexBasedOnMostRecentSnapshot == null 
            || (snapshotCreatedNotification.Version > _indexBasedOnMostRecentSnapshot.SnapshotVersion))
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
                _logger.LogError(ex, $"Failed to initialize {typeof(T).Name} Dataset");
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

            _logger.LogInformation($"Initialized {typeof(T).Name} Dataset with latest version {_index.LatestVersion}");
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Failed to initialize Dataset");
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
        using var slot = await Lock(ct);
        ThrowIfNotInitialized();
        EnsureNameIsValid(name);
        if (!_index!.Entities.TryGetValue(name, out var entity) || entity.GetLatestChangeType() == ChangeType.Deleted)
        {
            return new IDatasetWriter<T>.WriteResponse()
            {
                NewDatasetVersion = _index.LatestVersion
            };
        }

        var delta = new Delta([(name, null, ChangeType.Deleted)], _index.LatestVersion + 1);
        await ApplyDelta(delta);

        _logger.LogInformation($"Deleted entity version {_index.LatestVersion}");
        return WriteResponseFor(delta);
    }

    public async Task<IDatasetWriter<T>.WriteResponse> DeleteMany(string ancestor, CancellationToken ct = default)
    {
        using var slot = await Lock(ct);
        ThrowIfNotInitialized();

        var prefix = ancestor.EndsWith("/") ? ancestor : ancestor + "/";
        var splitAncestor = prefix.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (splitAncestor.Length  == 0 || splitAncestor.Length % 2 != 0)
            throw new ArgumentException("Invalid ancestor, an ancestor must have an even number of elements");
        
        var deletedNames = new List<string>(_index!.Entities.Count);

        foreach (var (name,entity) in _index!.Entities)
        {
            if (!name.StartsWith(prefix))
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

        _logger.LogInformation($"Deleted {deleteChanges.Length} descendants of {prefix} entity version");
        return WriteResponseFor(delta);
    }

    private void EnsureNameIsValid(string entityName)
    {
        if (_writerConfig.Value.AllowedNamesRegex is null)
           return;
        
        var result = _writerConfig.Value.AllowedNamesRegex.Match(entityName);
        if (result.Success)
            return;

        throw new ArgumentOutOfRangeException(nameof(entityName), entityName, $"valiation failed for entity name validation regex {_writerConfig.Value.AllowedNamesRegex}");
    }

    public async Task<IDatasetWriter<T>.WriteResponse> Put(string name, byte[] entityBytes, CancellationToken ct = default)
    {
        using var slot = await Lock(ct);
        ThrowIfNotInitialized();
        EnsureNameIsValid(name);
        Delta delta;

        if (!_index!.Entities.TryGetValue(name, out var existingEntity) || existingEntity.GetLatestChangeType() == ChangeType.Deleted)
            delta = new Delta([(name, entityBytes, ChangeType.New)], _index.LatestVersion + 1);

        else if (await IsByteArrayUpdated(name, entityBytes))
            delta = new Delta([(name, entityBytes, ChangeType.Changed)], _index.LatestVersion + 1);

        else return new IDatasetWriter<T>.WriteResponse()
        {
            NewDatasetVersion = _index.LatestVersion
        };

        await ApplyDelta(delta);

        _logger.LogInformation($"Put entity version {_index.LatestVersion}");
        return WriteResponseFor(delta);
    }

    public async Task<IDatasetWriter<T>.WriteResponse> PutAll(IAsyncEnumerable<(string name, byte[] entityBytes)> entities, CancellationToken ct = default)
    {
        return await PutEntitiesAsync(entities, includeParents: false, performDeletes: true, ct);
    }

    public async Task<IDatasetWriter<T>.WriteResponse> PutAllChildren(IAsyncEnumerable<(string name, byte[] entityBytes)> entities, CancellationToken ct = default)
    {
        return await PutEntitiesAsync(entities, includeParents: true, performDeletes: true, ct);
    }

    public async Task<IDatasetWriter<T>.WriteResponse> PutMany(IAsyncEnumerable<(string name, byte[] entityBytes)> entities, CancellationToken ct = default)
    {
        return await PutEntitiesAsync(entities, includeParents: false, performDeletes: false, ct);
    }

    private async Task<IDatasetWriter<T>.WriteResponse> PutEntitiesAsync(
        IAsyncEnumerable<(string name, byte[] entityBytes)> entities,
        bool includeParents,
        bool performDeletes,
        CancellationToken ct)
    {
        using var slot = await Lock(ct);
        ThrowIfNotInitialized();

        var tempDataFilePath = DatasetHelper.GetTempFilePath();
        await using var fileStream = new FileStream(tempDataFilePath, FileMode.Create, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous);
        var (deltaIndex, receivedNames, allParents) = await CreateDeltaIndexFromAsyncEnumerable(_index!, entities, includeParents, fileStream, ct);

        // Handle deletes based on parent inclusion and only if deletes are allowed
        if (performDeletes)
        {
            if (includeParents)
            {
                deltaIndex.DeletedNames.AddRange(DatasetHelper.GetImplicitDeletesForParentNames(_index, receivedNames, allParents));
            }
            else
            {
                deltaIndex.DeletedNames.AddRange(DatasetHelper.GetImplicitDeletes(_index, receivedNames));
            }
        }

        // Finalize write (directly in main method)
        await fileStream.WriteAsync(new byte[1], ct); // Write a trailing byte for the snapshot
        await fileStream.FlushAsync(ct);
        fileStream.Close();

        if (deltaIndex.NewEntities.Count == 0 && deltaIndex.ChangedEntities.Count == 0 && deltaIndex.DeletedNames.Count == 0)
        {
            File.Delete(tempDataFilePath);
            return new IDatasetWriter<T>.WriteResponse
            {
                NewDatasetVersion = _index.LatestVersion
            };
        }

        // Apply the delta and store it
        _index = _index.ApplyDelta(deltaIndex);
        using var newTempFileStream = File.OpenRead(tempDataFilePath);
        await _storageProvider.StoreDelta(_index.LatestVersion, deltaIndex.ToByteArray(), newTempFileStream);
        newTempFileStream.Close();
        File.Delete(tempDataFilePath);

        _logger.LogInformation($"Put many entities with version {_index.LatestVersion}, changed {deltaIndex.ChangedEntities.Count} entities of which {deltaIndex.DeletedNames.Count} were implicit deletes");

        return new IDatasetWriter<T>.WriteResponse
        {
            NewDatasetVersion = _index.LatestVersion,
            NumAddedEntities = (uint)deltaIndex.NewEntities.Count,
            NumUpdatedEntities = (uint)deltaIndex.ChangedEntities.Count,
            NumDeletedEntities = (uint)deltaIndex.DeletedNames.Count
        };
    }

    private async Task<(DeltaIndex deltaIndex, HashSet<string> receivedNames, HashSet<string> allParents)> CreateDeltaIndexFromAsyncEnumerable(ImmutableDatasetState index, IAsyncEnumerable<(string name, byte[] entityBytes)> entities, bool includeParents, Stream stream, CancellationToken ct)
    {
        var deltaIndex = new DeltaIndex { DeltaVersion = index!.LatestVersion + 1, DeltaVersionLegacy = 0};

        var receivedNames = new HashSet<string>();
        var allParents = new HashSet<string>();

        long byteArrayPosition = 0;

        await foreach (var (name, entityBytes) in entities.WithCancellation(ct))
        {
            try { EnsureNameIsValid(name); }
            catch (ArgumentOutOfRangeException ex)
            {
                _logger.LogWarning(ex, "Skipping entity");
                continue;
            }

            if (includeParents)
            {
                var parent = DatasetHelper.FindParentOfEntity(name);
                if (string.IsNullOrEmpty(parent)) continue;
                if (!allParents.Contains(parent)) allParents.Add(parent);
            }

            receivedNames.Add(name);
            var changeType = await DetermineChangeType(name, entityBytes);

            if (changeType == ChangeType.None) continue;

            var deltaEntity = new DeltaEntity
            {
                Position = byteArrayPosition,
                Length = entityBytes.Length,
                Name = name
            };

            if (changeType == ChangeType.New)
            {
                deltaIndex.NewEntities.Add(deltaEntity);
            }
            else
            {
                deltaIndex.ChangedEntities.Add(deltaEntity);
            }

            await stream.WriteAsync(entityBytes, ct);
            byteArrayPosition += entityBytes.Length;
        }

        return (deltaIndex, receivedNames, allParents);
    }

    private async Task<ChangeType> DetermineChangeType(string name, byte[] entityBytes)
    {
        if (!_index.Entities.TryGetValue(name, out var existingEntity) ||
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
        // If the crc32 is 0 (which is the case for the entities inserted before we added crc32 hashes), we can't compare the crc32, so we have to compare the bytes
        if (existingEntity.Crc32 != 0 && existingEntity.Crc32 != newHashCode)
            return true;
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
            sources.Add((ObjectNameTypeEnum.SNAPSHOT, _indexBasedOnMostRecentSnapshot.SnapshotVersion,
                entityFromSnapshot.Position));
        }

        if (entityVersion <= _index.SnapshotVersion)
        {
            // It's 'older' than the snapshot our index 'started' with, so the position refers to the Position in the snapshot
            // Double check that we 
            if (sources.Count == 0 || sources[0].version != _index.SnapshotVersion)
                sources.Add((ObjectNameTypeEnum.SNAPSHOT, _index.SnapshotVersion, entityInMainIndex.Position));
        }
        else
        {
            sources.Add((ObjectNameTypeEnum.DELTA, entityVersion, entityInMainIndex.Position));
        }

        return await _storageProvider.ReadDataFromSources(sources, entityInMainIndex.Length);
    }

    private async Task<ImmutableDatasetState> ApplyDelta(Delta delta)
    {
        var deltaIndex = new DeltaIndex()
        {
            DeltaVersionLegacy = 0,
            DeltaVersion = delta.DatasetVersion
        };

        await using var concatenatedByteStream = new MemoryStream(); 
        long targetPosition = 0;

        foreach (var (name, entityBytes, change, crc32) in delta.Changes)
        {
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
                        Crc32 = crc32
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
                        Length = entityBytes!.Length,
                        Name = name,
                        Crc32 = crc32
                    }); 
                    concatenatedByteStream.Write(entityBytes);
                    targetPosition += entityBytes.Length;
                    break;
                case ChangeType.Deleted:
                    deltaIndex.DeletedNames.Add(name);
                    break;
            }
        }

        concatenatedByteStream.Position = 0;
        await _storageProvider.StoreDelta(deltaIndex.DeltaVersion, deltaIndex.ToByteArray(), concatenatedByteStream);

        _index = _index!.ApplyDelta(deltaIndex);

        return _index;
    }


    public async Task<HealthCheckResult> CheckHealthAsync(CancellationToken cancellationToken = default)
    {
        if (InitializationTask == null)
        {
            return HealthCheckResult.Unhealthy("DatasetWriter: Initialization task is not yet started");
        }

        if (!InitializationTask.IsCompletedSuccessfully)
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

    private async Task<IDisposable> Lock()
    {
        await _semaphore.WaitAsync().ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext);
        return new ActionDisposable(() => _semaphore.Release());
    }

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

    private readonly bool _retryInitialization;
}
