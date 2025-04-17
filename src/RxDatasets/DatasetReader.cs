
using System.Reactive.Disposables;
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
            _logger.LogCritical(ex, $"Failed to initialize DataReader Dataset {typeof(T).Name}");
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
        if (fromVersion == _index!.LatestVersion)
        {
            await IndexChangedTaskCompletionSource.Task.WaitAsync(cancellationToken);
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

    private async Task<IDisposable> Lock(SemaphoreSlim semaphore)
    {
        await semaphore.WaitAsync();
        return new ActionDisposable(() => semaphore.Release());
    }
}
