using System.Reactive.Linq;
using System.Reactive.Subjects;
using Google.Protobuf;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.DatasetUpdateNotifications;
using RxDatasets.Models;

namespace RxDatasets;

public class DatasetWriter<T> : IDatasetWriter<T>, IHostedService
{
    private readonly IOptions<StorageConfig> _dtoConfig;
    private readonly ILogger<DatasetWriter<T>> _logger;
    private readonly DatasetHelper _datasetHelper;
    private readonly IStorageProvider<T> _storageProvider;
    private ImmutableDatasetState? _index;
    private ImmutableDatasetState? _backupIndex;
    public Task? InitializationTask;
    private readonly SemaphoreSlim _semaphore = new(1);

    public DatasetWriter(IDatasetUpdateNotifier<T> datasetUpdateNotifier, IOptions<StorageConfig> dtoConfig, ILogger<DatasetWriter<T>> logger, IStorageProvider<T> storageProvider, bool retryInitialization = true)
    {
        _storageProvider = storageProvider;
        datasetUpdateNotifier.Subscribe(OnDatasetUpdate);
        _logger = logger;
        _datasetHelper = new DatasetHelper(logger, _storageProvider);
        _dtoConfig = dtoConfig;
        _retryInitialization = retryInitialization;
    }

    private async Task OnDatasetUpdate(DatasetUpdateNotification updateNotification)
    {
        if (updateNotification.SnapshotVersion == updateNotification.Version)
        {
            _backupIndex = await _datasetHelper.GetSnapshotIndex(updateNotification.Version, updateNotification.SnapshotVersion);
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
            _backupIndex = await _datasetHelper.GetLatestIndex();

            _logger.LogInformation($"Initialized {typeof(T).Name} Dataset with latest version {_index.LatestVersion}");
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Failed to initialize Dataset");
            throw;
        }
    }

    private void EnsureInitialized()
    {
        if (!InitializationTask!.IsCompleted)
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
        EnsureInitialized();

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

    public async Task<IDatasetWriter<T>.WriteResponse> Put(string name, byte[] entityBytes, CancellationToken ct = default)
    {
        using var slot = await Lock(ct);
        EnsureInitialized();
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
        using var slot = await Lock(ct);
        EnsureInitialized();
        
        var receivedNames = new List<string>();
        var tempDataFilePath = Path.GetTempFileName();
        await using var fileStream = new FileStream(tempDataFilePath, FileMode.Create, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous);
        var deltaIndex = new DeltaIndex() { DeltaVersion = _index!.LatestVersion + 1 };

        long byteArrayPosition = 0;

        await foreach (var (name, entityBytes) in entities.WithCancellation(ct))
        {
             receivedNames.Add(name);
                var changeType = ChangeType.None;

                if (deltaIndex.NewEntities.All(entity => entity.Name != name) && (!_index.Entities.TryGetValue(name, out var existingEntity) ||
                                                                                  existingEntity.GetLatestChangeType() == ChangeType.Deleted))
                {
                    changeType = ChangeType.New;
                    
                }
                else if (_index.Entities.TryGetValue(name, out _) && await IsByteArrayUpdated(name, entityBytes))
                {
                    changeType = ChangeType.Changed;
                }

                if (changeType == ChangeType.None) continue;

                var deltaEntity = new DeltaEntity()
                {
                    Position = byteArrayPosition,
                    Length = entityBytes.Length,
                    Name = name
                }; 
                
                if (changeType == ChangeType.New)
                {
                    deltaIndex.NewEntities.Add(deltaEntity);
                }
                else //It will never be None or Deleted here
                {
                    deltaIndex.ChangedEntities.Add(deltaEntity);
                }
                await fileStream.WriteAsync(entityBytes, ct);
                byteArrayPosition += entityBytes.Length;
        }
        // Handle deletes
        var implicitDeletes = _index.Entities
            .Where(x => x.Value.GetLatestChangeType() != ChangeType.Deleted)
            .Select(x => x.Key)
            .Except(receivedNames);

        foreach (var name in implicitDeletes)
        {
            deltaIndex.DeletedNames.Add(name);
        }

        var snapshotTrailingBytes = new byte[1];
        await fileStream.WriteAsync(snapshotTrailingBytes, ct);
        await fileStream.FlushAsync(ct);
        fileStream.Close();

        if (deltaIndex.NewEntities.Count == 0 && deltaIndex.ChangedEntities.Count == 0 && deltaIndex.DeletedNames.Count == 0)
        {
            File.Delete(tempDataFilePath);

            return new IDatasetWriter<T>.WriteResponse()
            {
                NewDatasetVersion = _index.LatestVersion
            };
        }

        //upload 
        _index = _index.ApplyDelta(deltaIndex);
        _backupIndex = _backupIndex!.ApplyDelta(deltaIndex);


        var newTempFileStream = File.OpenRead(tempDataFilePath);
        await _storageProvider.StoreDelta(_index.LatestVersion, deltaIndex.ToByteArray(), newTempFileStream);
        newTempFileStream.Close();
        
        File.Delete(tempDataFilePath);
        _logger.LogInformation($"PutAll entities with version {_index.LatestVersion}, changed {deltaIndex.ChangedEntities.Count} entities of which {deltaIndex.DeletedNames.Count} were implicit deletes");
        return new IDatasetWriter<T>.WriteResponse()
        {
            NewDatasetVersion = _index.LatestVersion,
            NumAddedEntities = (uint)deltaIndex.NewEntities.Count,
            NumUpdatedEntities = (uint)deltaIndex.ChangedEntities.Count,
            NumDeletedEntities = (uint)deltaIndex.DeletedNames.Count,
        };
    }

    private async Task<bool> IsByteArrayUpdated(string name, byte[] entityBytes)
    {
        var existingEntity = _index!.Entities[name];
        var newHashCode = DatasetHelper.ComputeGeneralHash(entityBytes);
        if (existingEntity.Length != entityBytes.Length)
            return true;

        // If the hash code is 0 (which is the case for the entities inserted before we added hashing), we can't compare the hash code, so we have to compare the bytes
        if (existingEntity.HashCode != 0 && existingEntity.HashCode != newHashCode)
            return true;
        var existingBytes = await ReadEntityData(existingEntity);
        return !existingBytes.Span.SequenceEqual(entityBytes);
    }

    private async Task<ReadOnlyMemory<byte>> ReadEntityData(ImmutableEntity existingEntity)
    {
        ReadOnlyMemory<byte> existingBytes;
        var lastUsableVersionFromMain = await _storageProvider.GetLastPresentVersion();
        var entityVersion = existingEntity.GetLatestEntityVersion();

        if (entityVersion < lastUsableVersionFromMain)
        {
            existingBytes = await _storageProvider.ReadSnapshotData(_backupIndex!.LatestSnapshotVersion, existingEntity.Position, existingEntity.Length);
        }
        else if (entityVersion <= _index!.LatestSnapshotVersion)
        {
            existingBytes = await _storageProvider.ReadSnapshotData(_index.LatestSnapshotVersion, existingEntity.Position, existingEntity.Length);
        }
        else
        {
            existingBytes = await _storageProvider.ReadDeltaData(entityVersion, existingEntity.Position, existingEntity.Length);
        }

        return existingBytes;
    }

    private async Task<ImmutableDatasetState> ApplyDelta(Delta delta)
    {
        var deltaIndex = new DeltaIndex()
        {
            DeltaVersion = delta.DatasetVersion
        };

        byte[] data = new byte[delta.Changes.Sum(x => x.Entity?.Length ?? 0)];
        long byteArrayPosition = 0;
        await using var concatenatedByteStream = new MemoryStream();
        foreach (var (name, entityBytes, change, hashCode) in delta.Changes)
        {
            switch (change)
            {
                case ChangeType.New:
                    deltaIndex.NewEntities.Add(new DeltaEntity()
                    {
                        Position = byteArrayPosition,
                        Length = entityBytes!.Length,
                        Name = name,
                        Hashcode = hashCode
                    });
                    entityBytes.CopyTo(data, byteArrayPosition);
                    byteArrayPosition += entityBytes.Length;
                    break;
                case ChangeType.Changed:
                    deltaIndex.ChangedEntities.Add(new DeltaEntity()
                    {
                        Position = byteArrayPosition,
                        Length = entityBytes!.Length,
                        Name = name,
                        Hashcode = hashCode
                    });
                    entityBytes.CopyTo(data, byteArrayPosition);
                    byteArrayPosition += entityBytes.Length;
                    break;
                case ChangeType.Deleted:
                    deltaIndex.DeletedNames.Add(name);
                    break;
            }
        }
        await _storageProvider.StoreDelta(deltaIndex.DeltaVersion, deltaIndex.ToByteArray(), data);

        _index = _index!.ApplyDelta(deltaIndex);
        _backupIndex = _backupIndex!.ApplyDelta(deltaIndex);

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

    public Task StartAsync(CancellationToken cancellationToken)
    {
        InitializationTask = _retryInitialization ? InitializationLoop() : Initialize();

        return InitializationTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    private record Delta
    {
        public Delta((string Name, byte[]? Entity, ChangeType Change)[] Changes, ulong DatasetVersion)
        {
            this.Changes = Changes.Select(change => (change.Name, change.Entity, change.Change,
                DatasetHelper.ComputeGeneralHash(change.Entity ?? []))).ToArray();
            this.DatasetVersion = DatasetVersion;
        }

        public (string Name, byte[]? Entity, ChangeType Change, int HashCode)[] Changes { get; init; }
        public ulong DatasetVersion { get; init; }

        public void Deconstruct(out (string Name, byte[]? Entity, ChangeType Change, int HashCode)[] Changes, out ulong DatasetVersion)
        {
            Changes = this.Changes;
            DatasetVersion = this.DatasetVersion;
        }
    }

    private readonly Subject<(string Name, byte[] NewProtoBytes)> _onNewDtoSubject = new();
    private readonly Subject<(string Name, byte[] NewProtoBytes)> _onChangedDtoSubject = new();
    private readonly Subject<string> _onDeletedDtoSubject = new();
    private readonly bool _retryInitialization;
}
