
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
public class DatasetReaderWriter<T> : DatasetWriter<T>, IDatasetReader<T>, IAliasIndex
{
    private TaskCompletionSource _indexChangedTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public DatasetReaderWriter(
        INotificationsHub<T> notificationsHub,
        ILogger<DatasetWriter<T>> logger,
        IStorageProvider<T> storageProvider,
        IOptions<WriterConfig> writerConfig,
        IOptions<AliasOptions<T>>? aliasOptions = null,
        bool retryInitialization = true)
        : base(notificationsHub, logger, storageProvider, writerConfig, retryInitialization, aliasOptions)
    {
    }

    public string? ResolveAlias(string aliasName, string alias) => GetInitializedIndex().ResolveAlias(aliasName, alias);

    public (IReadOnlyList<string> Aliases, ulong Version) ListAliases(string aliasName, string prefix)
    {
        var indexRef = GetInitializedIndex();
        return (indexRef.ListAliases(aliasName, prefix), indexRef.LatestVersion);
    }

    protected override Task OnDeltaApplied(DeltaIndex deltaIndex)
    {
        // Wake reactive waiters (WaitChangedNames, StreamHistory, TailHistory) here, after the index swap.
        // DatasetUpdatedNotification is also sent after the swap, but only after this, while the write lock is held.
        Interlocked.Exchange(ref _indexChangedTcs, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously))
            .TrySetResult();
        return Task.CompletedTask;
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
        var entityInMainIndex = index.GetEntity(name);
        var entityVersion = entityInMainIndex.GetLatestEntityVersion();

        List<(ObjectNameTypeEnum type, ulong version, long position)> sources = new(2);

        if (snapshotIndex != null && entityVersion <= snapshotIndex.SnapshotVersion)
        {
            var entityFromSnapshot = snapshotIndex.GetEntity(name);
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

        if (!indexRef.TryGetEntity(name, out var entity))
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
            if (!indexRef.TryGetEntity(name, out var entity) || entity.GetLatestChangeType() == ChangeType.Deleted)
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
        var names = indexRef.EnumerateAll(parent, entity => entity.IsLive).Select(entity => entity.Key).ToList();
        return Task.FromResult((names, indexRef.LatestVersion));
    }

    public Task<uint> CountChildren(string parent, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var indexRef = GetInitializedIndex();
        return Task.FromResult((uint)indexRef.Root.CountLive(parent));
    }

    /// <exception cref="ResyncRequiredException">
    /// <paramref name="fromVersion"/> is not 0 and a delta after it is no longer kept.
    /// </exception>
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

        return ReadOnlyStorageProvider.ReadChangedNames(indexRef, fromVersion);
    }

    public async Task<ChangeSet> WaitChangedNames(ulong fromVersion, CancellationToken cancellationToken)
    {
        await WaitForVersionAfter(fromVersion, cancellationToken);
        return await GetChangedNames(fromVersion);
    }

    public async Task<HistoryChangeSet> WaitHistoryChanges(ulong fromVersion, CancellationToken ct = default)
    {
        await WaitForVersionAfter(fromVersion, ct);
        return await GetHistoryChanges(fromVersion, ct);
    }

    public Task<HistoryChangeSet> GetHistoryChanges(ulong fromVersion, CancellationToken ct = default)
    {
        var toVersion = GetInitializedIndex().LatestVersion;
        if (fromVersion > toVersion)
        {
            throw new ArgumentOutOfRangeException($"fromVersion {fromVersion} can't be bigger than the latest version {toVersion}");
        }

        return ReadOnlyStorageProvider.ReadHistoryChanges(fromVersion, toVersion, ct);
    }

    /// <summary>Returns once the latest version is no longer <paramref name="fromVersion"/>.</summary>
    private async Task WaitForVersionAfter(ulong fromVersion, CancellationToken cancellationToken)
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
    }

    public async Task<List<HistoryEntry>> GetHistory(string name, int limit = 0, ulong minVersion = 0, CancellationToken ct = default)
    {
        var indexRef = GetInitializedIndex();
        if (!indexRef.TryGetEntity(name, out var entity))
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

            var entry = deltaIndex.FindHistoryEntry(currentVersion, name);

            if (entry != null)
            {
                history.Add(entry);
                currentVersion = entry.PreviousVersion;
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

            foreach (var entry in deltaIndex.EnumerateHistoryEntries(currentVersion, parent))
                yield return entry;

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

            foreach (var entry in deltaIndex.EnumerateHistoryEntries(nextVersion, parent))
                yield return entry;

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

        var (changeType, _, changedEntity) = deltaIndex.FindChange(name);
        if (changeType == ChangeType.Deleted)
        {
            return null;
        }

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
