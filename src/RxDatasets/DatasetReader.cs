using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.DatasetUpdateNotifications;
using RxDatasets.Models;

namespace RxDatasets;

public class DatasetReader<T> : IDatasetReader<T>, IHostedService
{
    private readonly ILogger<DatasetReader<T>> _logger;
    private readonly IStorageProvider<T> _storageProvider;
    private readonly DatasetHelper _datasetHelper;
    private ImmutableDatasetState? _index;
    private ImmutableDatasetState? _indexBasedOnMostRecentSnapshot;
    public Task? InitializationTask;
    private readonly SemaphoreSlim _writerSemaphore = new(1);
    private readonly bool _retryInitialization;
    private TaskCompletionSource _indexChangedTaskCompletionSource = new();


    public DatasetReader(IDatasetUpdateNotifier<T> datasetUpdateNotifier,
        IOptions<StorageConfig> dtoConfig,
        ILogger<DatasetReader<T>> logger,
        IStorageProvider<T> storageProvider,
        bool retryInitialization = true)
    {
        _storageProvider = storageProvider;
        _datasetHelper = new DatasetHelper(logger, _storageProvider);
        _logger = logger;
        _retryInitialization = retryInitialization;
        datasetUpdateNotifier.Subscribe(OnDatasetUpdated);
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        InitializationTask = _retryInitialization ? InitializationLoop() : Initialize();
        return InitializationTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
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
            catch (FileNotFoundException e)
            {
                //ignored as we dont have a snapshot yet
            }
            _logger.LogInformation($"Initialized {typeof(T).Name} Dataset with latest version {_index.LatestVersion}");
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, $"Failed to initialize Dataset {typeof(T).Name}");
            throw;
        }
    }

    public async Task OnDatasetUpdated(DatasetUpdateNotification datasetUpdate)
    {
        if (datasetUpdate.DtoName != typeof(T).Name) return;
        using var updateSlot = await Lock(_writerSemaphore);

        if (datasetUpdate.SnapshotVersion != datasetUpdate.Version)
        {
            _index = await UpdateIndexWithLatestDelta(_index!, datasetUpdate.Version);
            var tcs = _indexChangedTaskCompletionSource;
            _indexChangedTaskCompletionSource = new TaskCompletionSource();
            tcs.SetResult();
        }
        else if ((_indexBasedOnMostRecentSnapshot == null && datasetUpdate.SnapshotVersion != 0) || (_indexBasedOnMostRecentSnapshot != null && datasetUpdate.SnapshotVersion > _indexBasedOnMostRecentSnapshot.SnapshotVersion))
        {
            _indexBasedOnMostRecentSnapshot = await _datasetHelper.GetLatestSnapshotIndex(); ;
        }


    }

    private async Task<ImmutableDatasetState> UpdateIndexWithLatestDelta(ImmutableDatasetState datasetState, ulong latestDelta)
    {
        var tempState = datasetState;

         if(latestDelta > tempState.LatestVersion)
        {
            var deltas = await _datasetHelper.GetChangedDeltas(tempState.LatestVersion, latestDelta);
            tempState = tempState.ApplyDeltas(deltas);
            _logger.LogInformation($"Dataset updated to version {tempState.LatestVersion}");
        }

        return tempState;
    }

    private ImmutableDatasetState EnsureInitialized()
    {
        if (!InitializationTask!.IsCompleted)
        {
            _logger.LogError("Dataset received a request before it was initialized");
            throw new InvalidOperationException("Initialization is not yet completed");
        }

        if (InitializationTask.IsFaulted)
        {
            _logger.LogError(InitializationTask.Exception, "Dataset received a request, but initialization failed");
            throw new InvalidOperationException("Initialization failed", InitializationTask.Exception);
        }
        return _index!;
    }

    public async Task<ICollection<(string name, ReadOnlyMemory<byte>? entityBytes)>> BatchGet(IEnumerable<string> names, string? parent)
    {
        var indexRef = EnsureInitialized();
        var tasks = names.Select(async name => {
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

        if (_indexBasedOnMostRecentSnapshot != null && entityVersion <= _indexBasedOnMostRecentSnapshot!.SnapshotVersion)
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

    public async Task<ChangeSet> WaitChangedNames(ulong fromVersion, CancellationToken cancellationToken)
    {
        if (fromVersion == _index!.LatestVersion)
        {
            await _indexChangedTaskCompletionSource.Task.WaitAsync(cancellationToken);
        }

        return await GetChangedNames(fromVersion);
    }

    public async Task<(string name, ReadOnlyMemory<byte> entityBytes)?> Get(string name)
    {
        var indexRef = EnsureInitialized();

        if (!indexRef.Entities.TryGetValue(name, out var entity))
            return null;

        if (entity.GetLatestChangeType() == ChangeType.Deleted)
            return null;

        var entityBytes = await ReadEntityBytes(name);

        return (name, entityBytes);
    }

    public Task<ChangeSet> GetChangedNames(ulong fromVersion)
    {
        var indexRef = EnsureInitialized();
        var toVersion = indexRef.LatestVersion;

        if (fromVersion > toVersion)
        {
            throw new ArgumentOutOfRangeException($"fromVersion {fromVersion} can't be bigger than the latest version {toVersion}");
        }

        var changedNames = GenerateChangedNames(fromVersion, toVersion, indexRef);
        return Task.FromResult(new ChangeSet(toVersion, changedNames));
    }

    private ICollection<(string key, ChangeType change)> GenerateChangedNames(ulong fromVersion, ulong toVersion, ImmutableDatasetState indexRef)
    {
        ICollection<(string key, ChangeType change)> changedNames = [];

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

    public async Task<HealthCheckResult> CheckHealthAsync(CancellationToken cancellationToken = default)
    {
        if (InitializationTask == null)
        {
            return HealthCheckResult.Unhealthy("DatasetReader: Initialization task is not yet started");
        }

        if (!InitializationTask.IsCompletedSuccessfully)
            return HealthCheckResult.Unhealthy("DatasetReader: Initialization task is not completed");

        try
        {
            await _storageProvider.ListVersions();
        }
        catch (Exception e)
        {
            return HealthCheckResult.Unhealthy("DatasetReader: Failed to get latest versions from storageProvider", e);
        }

        return HealthCheckResult.Healthy();
    }

    public Task<ulong> GetLatestVersion()
    {
        var indexRef = EnsureInitialized();
        return Task.FromResult(indexRef.LatestVersion);
    }

    public async IAsyncEnumerable<(string name, ReadOnlyMemory<byte> entityBytes)> List(string parent)
    {
        var indexRef = EnsureInitialized();
        List<(string name, ReadOnlyMemory<byte> bytes)> entities = [];
        var prefix = parent + "/";

        foreach (var entity in indexRef.Entities
                     .Where(x => x.Key.StartsWith(prefix))
                     .Where(x => x.Value.GetLatestChangeType() != ChangeType.Deleted))
        {
            var entityBytes = await ReadEntityBytes(entity.Key);
            entities.Add((entity.Key, entityBytes));
        }
        foreach (var entity in entities)
        {
            yield return entity;
        }
    }

    private async Task<IDisposable> Lock(SemaphoreSlim semaphore)
    {
        await semaphore.WaitAsync();
        return new ActionDisposable(() => semaphore.Release());
    }

    private async Task<IDisposable> Lock(SemaphoreSlim semaphore, CancellationToken cancellationToken)
    {
        await semaphore.WaitAsync(cancellationToken);
        return new ActionDisposable(() => semaphore.Release());
    }

}
