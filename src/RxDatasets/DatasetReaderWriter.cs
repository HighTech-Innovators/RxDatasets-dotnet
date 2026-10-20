
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Models;

namespace RxDatasets;

/// <summary>
/// Combined reader+writer that shares a single <see cref="ImmutableDatasetState"/> index
/// and a single initialization path, eliminating duplicate storage reads at startup.
/// </summary>
public class DatasetReaderWriter<T> : DatasetWriter<T>, IDatasetReader<T>
{
    private readonly ParentsIndex _parentsIndex = new();
    private TaskCompletionSource _indexChangedTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public DatasetReaderWriter(
        INotificationsHub<T> notificationsHub,
        ILogger<DatasetWriter<T>> logger,
        IStorageProvider<T> storageProvider,
        IOptions<WriterConfig> writerConfig)
        : base(notificationsHub, logger, storageProvider, writerConfig)
    {
    }

    protected override async Task OnDeltaApplied(DeltaIndex deltaIndex)
    {
        foreach (var entity in deltaIndex.NewEntities)
            await _parentsIndex.OnNew(entity.Name);
        foreach (var entity in deltaIndex.DeletedEntities)
            await _parentsIndex.OnDelete(entity.Name);
        foreach (var name in deltaIndex.DeletedNames)
            await _parentsIndex.OnDelete(name);

        // Wake reactive waiters (WaitChangedNames, StreamHistory, TailHistory) here, after the index swap,
        // not on DatasetUpdatedNotification: that one is sent from StoreDelta, before the swap, so a waiter
        // woken by it would still see the old LatestVersion, re-arm, and sleep through this write.
        Interlocked.Exchange(ref _indexChangedTcs, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously))
            .TrySetResult();
    }

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
        var entityVersion = entityInMainIndex.GetLatestEntityVersion();

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

        if (!indexRef.Entities.TryGetValue(name, out var entity))
            return null;

        if (entity.GetLatestChangeType() == ChangeType.Deleted)
            return null;

        var entityBytes = await ReadEntityBytes(name);
        return (name, entityBytes);
    }

    public async Task<ICollection<(string name, ReadOnlyMemory<byte>? entityBytes)>> BatchGet(IEnumerable<string> names, string? parent)
    {
        var indexRef = GetInitializedIndex();
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

    public async Task<(List<string> Names, ulong LatestVersion)> List(string parent, CancellationToken ct = default)
    {
        var indexRef = GetInitializedIndex();

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
        var indexRef = GetInitializedIndex();

        if (string.IsNullOrEmpty(parent) || parent is "/")
        {
            return (uint)indexRef.Entities
                .Count(x => x.Value.GetLatestChangeType() != ChangeType.Deleted);
        }

        IEnumerable<string> allNamesGetter = indexRef.Entities
            .Where(x => x.Value.GetLatestChangeType() != ChangeType.Deleted)
            .Select(entity => entity.Key);

        var childrenIndex = await _parentsIndex.GetOrCreateChildrenIndex(parent, allNamesGetter);
        return (uint)childrenIndex.Count();
    }

    public Task<ChangeSet> GetChangedNames(ulong fromVersion)
    {
        var indexRef = GetInitializedIndex();
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

    public async Task<ChangeSet> WaitChangedNames(ulong fromVersion, CancellationToken cancellationToken)
    {
        if (InitializationTask == null || InitializationTask.IsFaulted)
            GetInitializedIndex();

        if (!InitializationTask!.IsCompleted)
            await InitializationTask;

        while (true)
        {
            // Take the signal before reading the version, so a write landing in between still wakes us
            var changed = _indexChangedTcs.Task;
            if (fromVersion != GetInitializedIndex().LatestVersion)
                break;
            await changed.WaitAsync(cancellationToken);
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
        var currentVersion = entity.GetLatestEntityVersion();

        while (currentVersion > 0 && currentVersion > minVersion)
        {
            if (limit > 0 && history.Count >= limit)
                break;

            ct.ThrowIfCancellationRequested();
            DeltaIndex deltaIndex;
            try
            {
                var deltaBytes = await ReadOnlyStorageProvider.ReadDeltaIndex(currentVersion);
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
            while (true)
            {
                var changed = _indexChangedTcs.Task;
                if (sinceVersion < GetInitializedIndex().LatestVersion)
                    break;
                await changed.WaitAsync(ct);
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
                deltaIndex = DeltaIndex.Parser.ParseFrom(deltaBytes.Span);
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

            var changed = _indexChangedTcs.Task;
            var currentLatest = GetInitializedIndex().LatestVersion;

            if (nextVersion > currentLatest)
            {
                if (!follow)
                    yield break;

                await changed.WaitAsync(ct);
                continue;
            }

            DeltaIndex deltaIndex;
            try
            {
                var deltaBytes = await ReadOnlyStorageProvider.ReadDeltaIndex(nextVersion);
                deltaIndex = DeltaIndex.Parser.ParseFrom(deltaBytes.Span);
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
            deltaIndex = DeltaIndex.Parser.ParseFrom(deltaBytes.Span);
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

    private static List<(string key, ChangeType change)> GenerateChangedNames(ulong fromVersion, ulong toVersion, ImmutableDatasetState indexRef)
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
}
