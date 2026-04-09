
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Models;
using RxDatasets.Notifications;

namespace RxDatasets;

/// <summary>
/// Combined reader+writer that shares a single <see cref="ImmutableDatasetState"/> index
/// and a single initialization path, eliminating duplicate storage reads at startup.
/// </summary>
public class DatasetReaderWriter<T> : DatasetWriter<T>, IDatasetReader<T>
{
    private TaskCompletionSource _indexChangedTcs = new();
    private readonly IDisposable _updateSubscription;

    public DatasetReaderWriter(
        INotificationsHub<T> notificationsHub,
        ILogger<DatasetWriter<T>> logger,
        IStorageProvider<T> storageProvider,
        IOptions<WriterConfig> writerConfig)
        : base(notificationsHub, logger, storageProvider, writerConfig)
    {
        // The base writer already updates _index after each write.
        // We subscribe to the notification only to wake up reactive waiters
        // (WaitChangedNames, StreamHistory, TailHistory).
        _updateSubscription = notificationsHub.Subscribe<DatasetUpdatedNotification>(_ =>
        {
            var tcs = _indexChangedTcs;
            _indexChangedTcs = new TaskCompletionSource();
            tcs.TrySetResult();
            return Task.CompletedTask;
        });
    }

    public override void Dispose()
    {
        _updateSubscription.Dispose();
        base.Dispose();
    }

    protected override Task OnDeltaApplied(DeltaIndex deltaIndex) => Task.CompletedTask;

    private ImmutableDatasetState GetInitializedIndex()
    {
        if (InitializationTask == null)
        {
            Logger.LogError("Received a request, however we are not ready yet");
            throw new InvalidOperationException("Not initialized - possibly because storage isn't ready or doesn't have a lock");
        }

        if (!InitializationTask.IsCompleted)
        {
            Logger.LogError("Dataset received a request before it was initialized");
            throw new InvalidOperationException("Initialization is not yet completed");
        }

        if (InitializationTask.IsFaulted)
        {
            Logger.LogError(InitializationTask.Exception, "Dataset received a request, but initialization failed");
            throw new InvalidOperationException("Initialization failed", InitializationTask.Exception);
        }

        return CurrentIndex!;
    }

    private async Task<ReadOnlyMemory<byte>> ReadEntityBytes(string name)
    {
        var index = CurrentIndex!;
        var snapshotIndex = MostRecentSnapshotIndex;
        var entityInMainIndex = index.Entities[name];
        var entityVersion = entityInMainIndex.LatestVersion;

        List<(ObjectNameTypeEnum type, ulong version, long position)> sources = new(2);

        if (snapshotIndex != null && entityVersion <= snapshotIndex.SnapshotVersion)
        {
            var entityFromSnapshot = snapshotIndex.Entities[name];
            sources.Add((ObjectNameTypeEnum.Snapshot, snapshotIndex.SnapshotVersion,
                entityFromSnapshot.Position));
        }

        if (entityVersion <= index.SnapshotVersion)
        {
            if (sources.Count == 0 || sources[0].version != index.SnapshotVersion)
                sources.Add((ObjectNameTypeEnum.Snapshot, index.SnapshotVersion, entityInMainIndex.Position));
        }
        else
        {
            sources.Add((ObjectNameTypeEnum.Delta, entityVersion, entityInMainIndex.Position));
        }

        return await ReadOnlyStorageProvider.ReadDataFromSources(sources, entityInMainIndex.Length);
    }

    public async Task<(string name, ReadOnlyMemory<byte> entityBytes)?> Get(string name)
    {
        var indexRef = GetInitializedIndex();

        if (!indexRef.Entities.TryGetValue(name, out _))
            return null;

        var entityBytes = await ReadEntityBytes(name);
        return (name, entityBytes);
    }

    public async Task<ICollection<(string name, ReadOnlyMemory<byte>? entityBytes)>> BatchGet(IEnumerable<string> names, string? parent)
    {
        var indexRef = GetInitializedIndex();
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

    public Task<(List<string> Names, ulong LatestVersion)> List(string parent, CancellationToken ct = default)
    {
        var indexRef = GetInitializedIndex();

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
        var indexRef = GetInitializedIndex();

        if (string.IsNullOrEmpty(parent) || parent is "/")
        {
            return Task.FromResult((uint)indexRef.Entities.Count);
        }

        var trimmedParent = parent.TrimEnd('/');
        return Task.FromResult((uint)indexRef.Entities.CountDescendants(trimmedParent));
    }

    public async Task<ChangeSet> GetChangedNames(ulong fromVersion)
    {
        var indexRef = GetInitializedIndex();
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
        var deltas = await DatasetHelper.GetChangedDeltas(fromVersion, toVersion);
        var changedNamesFromDeltas = DeltaChangeAggregator.AggregateChanges(deltas);
        return new ChangeSet(toVersion, changedNamesFromDeltas);
    }

    public async Task<ChangeSet> WaitChangedNames(ulong fromVersion, CancellationToken cancellationToken)
    {
        if (InitializationTask == null || InitializationTask.IsFaulted)
            GetInitializedIndex();

        if (!InitializationTask!.IsCompleted)
            await InitializationTask;

        while (fromVersion == GetInitializedIndex().LatestVersion)
        {
            await _indexChangedTcs.Task.WaitAsync(cancellationToken);
        }

        return await GetChangedNames(fromVersion);
    }

    public async Task<List<HistoryEntry>> GetHistory(string name, int limit = 0, ulong minVersion = 0, CancellationToken ct = default)
    {
        var indexRef = GetInitializedIndex();
        if (!indexRef.Entities.TryGetValue(name, out var entity))
        {
            return [];
        }

        var history = new List<HistoryEntry>();
        var currentVersion = entity.LatestVersion;

        while (currentVersion > 0 && currentVersion > minVersion)
        {
            if (limit > 0 && history.Count >= limit)
                break;

            ct.ThrowIfCancellationRequested();
            DeltaIndex deltaIndex;
            try
            {
                var deltaBytes = await ReadOnlyStorageProvider.ReadDeltaIndex(currentVersion);
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
        if (InitializationTask == null || InitializationTask.IsFaulted)
            GetInitializedIndex();

        if (!InitializationTask!.IsCompleted)
            await InitializationTask;

        if (waitIfNone)
        {
            while (sinceVersion >= GetInitializedIndex().LatestVersion)
            {
                await _indexChangedTcs.Task.WaitAsync(ct);
            }
        }

        var indexRef = GetInitializedIndex();
        var currentVersion = maxVersion > 0 ? maxVersion : indexRef.LatestVersion;

        while (currentVersion > 0 && currentVersion > sinceVersion)
        {
            ct.ThrowIfCancellationRequested();
            DeltaIndex deltaIndex;
            try
            {
                var deltaBytes = await ReadOnlyStorageProvider.ReadDeltaIndex(currentVersion);
                deltaIndex = NestedIndexHelper.ParseDeltaIndex(deltaBytes.Span);
            }
            catch (FileNotFoundException)
            {
                break;
            }

            var timestamp = deltaIndex.Timestamp?.ToDateTimeOffset() ?? DateTimeOffset.MinValue.ToUniversalTime();
            var deletedEntityNames = new HashSet<string>(deltaIndex.DeletedEntities.Select(e => e.Name));

            foreach (var newEntity in deltaIndex.NewEntities)
            {
                if (string.IsNullOrEmpty(parent) || newEntity.Name.StartsWith(parent, StringComparison.Ordinal))
                    yield return new HistoryEntry(currentVersion, ChangeType.New, timestamp, newEntity.PreviousVersion) { Id = newEntity.Name };
            }

            foreach (var changedEntity in deltaIndex.ChangedEntities)
            {
                if (string.IsNullOrEmpty(parent) || changedEntity.Name.StartsWith(parent, StringComparison.Ordinal))
                    yield return new HistoryEntry(currentVersion, ChangeType.Changed, timestamp, changedEntity.PreviousVersion) { Id = changedEntity.Name };
            }

            foreach (var deletedEntity in deltaIndex.DeletedEntities)
            {
                if (string.IsNullOrEmpty(parent) || deletedEntity.Name.StartsWith(parent, StringComparison.Ordinal))
                    yield return new HistoryEntry(currentVersion, ChangeType.Deleted, timestamp, deletedEntity.PreviousVersion) { Id = deletedEntity.Name };
            }

            foreach (var deletedName in deltaIndex.DeletedNames)
            {
                if (!deletedEntityNames.Contains(deletedName) &&
                    (string.IsNullOrEmpty(parent) || deletedName.StartsWith(parent, StringComparison.Ordinal)))
                    yield return new HistoryEntry(currentVersion, ChangeType.Deleted, timestamp, 0) { Id = deletedName };
            }

            currentVersion--;
        }
    }

    public async IAsyncEnumerable<HistoryEntry> TailHistory(string parent, ulong sinceVersion = 0, bool follow = false, [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (InitializationTask == null || InitializationTask.IsFaulted)
            GetInitializedIndex();

        if (!InitializationTask!.IsCompleted)
            await InitializationTask;

        var nextVersion = sinceVersion == 0 ? 1UL : sinceVersion + 1;

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            var currentLatest = GetInitializedIndex().LatestVersion;

            if (nextVersion > currentLatest)
            {
                if (!follow)
                    yield break;

                await _indexChangedTcs.Task.WaitAsync(ct);
                continue;
            }

            DeltaIndex deltaIndex;
            try
            {
                var deltaBytes = await ReadOnlyStorageProvider.ReadDeltaIndex(nextVersion);
                deltaIndex = NestedIndexHelper.ParseDeltaIndex(deltaBytes.Span);
            }
            catch (FileNotFoundException)
            {
                nextVersion++;
                continue;
            }

            var timestamp = deltaIndex.Timestamp?.ToDateTimeOffset() ?? DateTimeOffset.MinValue.ToUniversalTime();
            var deletedEntityNames = new HashSet<string>(deltaIndex.DeletedEntities.Select(e => e.Name));

            foreach (var newEntity in deltaIndex.NewEntities)
            {
                if (string.IsNullOrEmpty(parent) || newEntity.Name.StartsWith(parent, StringComparison.Ordinal))
                    yield return new HistoryEntry(nextVersion, ChangeType.New, timestamp, newEntity.PreviousVersion) { Id = newEntity.Name };
            }

            foreach (var changedEntity in deltaIndex.ChangedEntities)
            {
                if (string.IsNullOrEmpty(parent) || changedEntity.Name.StartsWith(parent, StringComparison.Ordinal))
                    yield return new HistoryEntry(nextVersion, ChangeType.Changed, timestamp, changedEntity.PreviousVersion) { Id = changedEntity.Name };
            }

            foreach (var deletedEntity in deltaIndex.DeletedEntities)
            {
                if (string.IsNullOrEmpty(parent) || deletedEntity.Name.StartsWith(parent, StringComparison.Ordinal))
                    yield return new HistoryEntry(nextVersion, ChangeType.Deleted, timestamp, deletedEntity.PreviousVersion) { Id = deletedEntity.Name };
            }

            foreach (var deletedName in deltaIndex.DeletedNames)
            {
                if (!deletedEntityNames.Contains(deletedName) &&
                    (string.IsNullOrEmpty(parent) || deletedName.StartsWith(parent, StringComparison.Ordinal)))
                    yield return new HistoryEntry(nextVersion, ChangeType.Deleted, timestamp, 0) { Id = deletedName };
            }

            nextVersion++;
        }
    }

    public async Task<(string name, ReadOnlyMemory<byte> entityBytes)?> GetHistoric(string name, ulong version, CancellationToken ct = default)
    {
        var indexRef = GetInitializedIndex();
        if (version > indexRef.LatestVersion)
        {
            throw new ArgumentOutOfRangeException(nameof(version), "Version is in the future");
        }

        DeltaIndex deltaIndex;
        try
        {
            var deltaBytes = await ReadOnlyStorageProvider.ReadDeltaIndex(version);
            deltaIndex = NestedIndexHelper.ParseDeltaIndex(deltaBytes.Span);
        }
        catch (FileNotFoundException)
        {
            throw new ArgumentException("Delta not found", nameof(version));
        }

        var deletedEntity = deltaIndex.DeletedEntities.FirstOrDefault(e => e.Name == name);
        if (deletedEntity != null || deltaIndex.DeletedNames.Contains(name))
        {
            return null;
        }

        var newEntity = deltaIndex.NewEntities.FirstOrDefault(e => e.Name == name);
        if (newEntity != null)
        {
            var data = await ReadOnlyStorageProvider.ReadDataFromSources([(ObjectNameTypeEnum.Delta, version, newEntity.Position)], newEntity.Length);
            return (name, data);
        }

        var changedEntity = deltaIndex.ChangedEntities.FirstOrDefault(e => e.Name == name);
        if (changedEntity != null)
        {
            var data = await ReadOnlyStorageProvider.ReadDataFromSources([(ObjectNameTypeEnum.Delta, version, changedEntity.Position)], changedEntity.Length);
            return (name, data);
        }

        throw new ArgumentException($"Entity {name} was not changed in version {version}");
    }

    public ulong GetLatestVersion()
    {
        var indexRef = GetInitializedIndex();
        return indexRef.LatestVersion;
    }

    public ulong GetLastUsedSnapshotVersion()
    {
        GetInitializedIndex();
        return MostRecentSnapshotIndex?.SnapshotVersion ?? 0;
    }

    public ulong GetMainIndexSnapshotVersion()
    {
        var indexRef = GetInitializedIndex();
        return indexRef.SnapshotVersion;
    }

}
