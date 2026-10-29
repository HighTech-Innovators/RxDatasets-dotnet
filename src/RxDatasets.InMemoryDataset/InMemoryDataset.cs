using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Runtime.CompilerServices;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.Models;

namespace RxDatasets.InMemoryDataset;

public record DatasetVersionUpdate(ulong NewDatasetVersion, ulong PreviousDatasetVersion);

/// <summary>
/// A dataset in memory, for tests. Checks aliases exactly like <see cref="DatasetWriter{T}"/>, with the same
/// <see cref="AliasStaging"/>, and keeps them in an index of its own.
/// </summary>
public class InMemoryDataset<T>(IReadOnlyList<AliasDefinition>? aliasDefinitions = null)
    : IDatasetReader<T>, IDatasetWriter<T>, IAliasIndex
{
    private readonly IReadOnlyList<AliasDefinition> _aliasDefinitions = aliasDefinitions ?? [];
    private readonly string _aliasDefinitionsHash = AliasOptions<T>.HashOf(aliasDefinitions ?? []);
    private ImmutableDatasetState _aliasIndex = ImmutableDatasetState.Empty(0, 0);
    private readonly SemaphoreSlim _semaphore = new(1);
    private ulong LatestVersion => _deltas.LastOrDefault()?.DatasetVersion ?? 0;
    private readonly Dictionary<string, byte[]> _currentSnapshot = [];
    private readonly List<Delta> _deltas = [];
    private TaskCompletionSource _waitForChangesTaskCompletionSource = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public async Task<IDatasetWriter<T>.WriteResponse> Delete(string name, string? trackingId, DateTimeOffset? slaTimestamp, CancellationToken ct = default)
    {
        using var slot = await _semaphore.LockAsync(ct);
        if (!_currentSnapshot.ContainsKey(name))
            return new IDatasetWriter<T>.WriteResponse { NewDatasetVersion = LatestVersion };
        var staging = NewAliasStaging();
        staging.Release(name);
        var delta = new Delta(new[] { (name, (byte[]?)null, ChangeType.Deleted) }, LatestVersion + 1);
        ApplyDelta(delta, staging);
        return WriteResponseFor(delta);
    }

    public async Task<IDatasetWriter<T>.WriteResponse> DeleteMany(IAsyncEnumerable<(string name, string? trackingId, DateTimeOffset? slaTimestamp)> names, CancellationToken ct = default)
    {
        using var slot = await _semaphore.LockAsync(ct);
        var changes = new Dictionary<string, (string Name, byte[]? Entity, ChangeType Change)>();
        var staging = NewAliasStaging();

        await foreach (var (name, _, _) in names.WithCancellation(ct))
        {
            if (_currentSnapshot.TryGetValue(name, out _))
                changes[name] = (name, null, ChangeType.Deleted);
            staging.Release(name);
        }

        if (changes.Count == 0)
        {
            return new IDatasetWriter<T>.WriteResponse { NewDatasetVersion = LatestVersion };
        }

        // Create delta and apply it
        var theDelta = new Delta(changes.Values.ToArray(), LatestVersion + 1);
        ApplyDelta(theDelta, staging);

        return WriteResponseFor(theDelta);
    }

    public async Task<IDatasetWriter<T>.WriteResponse> DeleteChildren(string ancestor, string? trackingId, DateTimeOffset? slaTimestamp, CancellationToken ct = default)
    {
        using var slot = await _semaphore.LockAsync(ct);
        var prefix = ancestor.EndsWith("/", StringComparison.Ordinal) ? ancestor : ancestor + "/";
        var splitAncestor = prefix.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (splitAncestor.Length == 0 || splitAncestor.Length % 2 != 0)
            throw new ArgumentException("Invalid ancestor, an ancestor must have an even number of elements");

        var deletedNames = new List<string>(_currentSnapshot.Count);

        foreach (var name in _currentSnapshot.Keys)
        {
            if (!name.StartsWith(prefix, StringComparison.Ordinal))
                continue;

            deletedNames.Add(name);
        }

        if (!deletedNames.Any())
        {
            return new IDatasetWriter<T>.WriteResponse()
            {
                NewDatasetVersion = LatestVersion
            };
        }

        var deleteChanges = deletedNames.Select(nm => (nm, (byte[]?)null, ChangeType.Deleted)).ToArray();
        var staging = NewAliasStaging();
        foreach (var name in deletedNames)
            staging.Release(name);
        var delta = new Delta(deleteChanges, LatestVersion + 1);
        ApplyDelta(delta, staging);
        return WriteResponseFor(delta);
    }

    public async Task<IDatasetWriter<T>.WriteResponse> Put(string name, byte[] newEntity, string? trackingId, DateTimeOffset? slaTimestamp, CancellationToken ct = default)
    {
        using var slot = await _semaphore.LockAsync(ct);
        Delta delta;
        if (!_currentSnapshot.TryGetValue(name, out var oldEntity))
        {
            delta = new Delta(new[] { (name, (byte[]?)newEntity, ChangeType.New) }, LatestVersion + 1);
        }
        else
        {
            if (newEntity?.Length == oldEntity.Length && newEntity.SequenceEqual(oldEntity))
                return new IDatasetWriter<T>.WriteResponse { NewDatasetVersion = LatestVersion };

            delta = new Delta(new[] { (name, newEntity, ChangeType.Changed) }, LatestVersion + 1);
        }

        var staging = NewAliasStaging();
        staging.Put(name, newEntity, DatasetHelper.ComputeCrc32(newEntity));
        ApplyDelta(delta, staging);
        return WriteResponseFor(delta);
    }

    public async Task<IDatasetWriter<T>.WriteResponse> PutAll(IAsyncEnumerable<(string name, byte[] entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)> entities, CancellationToken ct = default)
    {
        // implicit deletes across whole dataset
        return await PutEntitiesAsync(WrapNonNull(entities), includeParents: false, performImplicitDeletes: true, ct);
    }

    public async Task<IDatasetWriter<T>.WriteResponse> PutAllChildren(IAsyncEnumerable<(string name, byte[] entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)> entities, CancellationToken ct = default)
    {
        // implicit deletes scoped to provided parents
        return await PutEntitiesAsync(WrapNonNull(entities), includeParents: true, performImplicitDeletes: true, ct);
    }

    public async Task<IDatasetWriter<T>.WriteResponse> PutMany(IAsyncEnumerable<(string name, byte[] entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)> entities, CancellationToken ct = default)
    {
        // no implicit deletes
        return await PutEntitiesAsync(WrapNonNull(entities), includeParents: false, performImplicitDeletes: false, ct);
    }

    public async Task<IDatasetWriter<T>.WriteResponse> PutOrDeleteMany(IAsyncEnumerable<(string name, byte[]? entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)> changes, CancellationToken ct = default)
    {
        // mixed explicit puts and deletes, no implicit deletes
        return await PutEntitiesAsync(changes, includeParents: false, performImplicitDeletes: false, ct);
    }

    private static async IAsyncEnumerable<(string name, byte[]? entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)> WrapNonNull(IAsyncEnumerable<(string name, byte[] entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)> source)
    {
        await foreach (var (name, bytes, trackingId, slaTimestamp) in source)
            yield return (name, (byte[]?)bytes, trackingId, slaTimestamp);
    }

    private async Task<IDatasetWriter<T>.WriteResponse> PutEntitiesAsync(
        IAsyncEnumerable<(string name, byte[]? entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)> entities,
        bool includeParents,
        bool performImplicitDeletes,
        CancellationToken ct)
    {
        using var slot = await _semaphore.LockAsync(ct);
        var changes = new Dictionary<string, (string Name, byte[]? Entity, ChangeType Change)>();
        var receivedNames = new HashSet<string>();
        var allParents = new HashSet<string>();

        // Track final state per entity - last operation wins
        var entityFinalStates = new Dictionary<string, (uint crc32, byte[] bytes)?>();
        var staging = NewAliasStaging();
        if (performImplicitDeletes && !includeParents)
            staging.ReleaseAll();

        await foreach (var (name, entityBytes, trackingId, slaTimestamp) in entities.WithCancellation(ct))
        {
            if (includeParents)
            {
                var parent = DatasetHelper.FindParentOfEntity(name);
                if (string.IsNullOrEmpty(parent))
                    continue;
                if (allParents.Add(parent))
                    staging.ReleaseChildren(parent);
            }

            receivedNames.Add(name);

            // Store final state (last operation wins)
            if (entityBytes == null)
            {
                staging.Release(name);
                entityFinalStates[name] = null;
            }
            else
            {
                var crc32 = DatasetHelper.ComputeCrc32(entityBytes);
                staging.Put(name, entityBytes, crc32);
                entityFinalStates[name] = (crc32, entityBytes);
            }
        }

        // Second pass: determine net effect for each entity using CRC32 comparison
        foreach (var (name, finalState) in entityFinalStates)
        {
            if (finalState == null)
            {
                // Final state is delete
                if (_currentSnapshot.ContainsKey(name))
                    changes[name] = (name, null, ChangeType.Deleted);
                continue;
            }

            // Final state is put - determine net change using CRC32 comparison when possible
            var (finalCrc, finalBytes) = finalState.Value;
            if (!_currentSnapshot.TryGetValue(name, out var oldEntityBytes))
            {
                changes[name] = (name, finalBytes, ChangeType.New);
            }
            else
            {
                // Fast CRC32 comparison - only fall back to byte comparison if needed
                var oldCrc = DatasetHelper.ComputeCrc32(oldEntityBytes);

                if (oldCrc != finalCrc)
                {
                    changes[name] = (name, finalBytes, ChangeType.Changed);
                }
                // If CRC32 matches, no change needed (not added to changes)
            }
        }

        if (performImplicitDeletes)
        {
            if (includeParents)
            {
                foreach (var parent in allParents)
                {
                    var parentDeletes = HandleParentDeletes(receivedNames, parent);
                    foreach (var deleteName in parentDeletes)
                    {
                        if (!changes.ContainsKey(deleteName))
                            changes[deleteName] = (deleteName, null, ChangeType.Deleted);
                    }
                }
            }
            else
            {
                var implicitDeletes = HandleImplicitDeletes(receivedNames);
                foreach (var name in implicitDeletes)
                {
                    if (!changes.ContainsKey(name))
                        changes[name] = (name, null, ChangeType.Deleted);
                }
            }
        }

        if (changes.Count == 0)
        {
            return new IDatasetWriter<T>.WriteResponse { NewDatasetVersion = LatestVersion };
        }

        var theDelta = new Delta(changes.Values.ToArray(), LatestVersion + 1);
        ApplyDelta(theDelta, staging);
        return WriteResponseFor(theDelta);
    }

    private IEnumerable<string> HandleImplicitDeletes(HashSet<string> receivedNames)
    {
        return _currentSnapshot.Keys.Except(receivedNames);
    }

    private IEnumerable<string> HandleParentDeletes(HashSet<string> receivedNames, string parent)
    {
        return _currentSnapshot
            .Where(kvp => DatasetHelper.FindParentOfEntity(kvp.Key) == parent && !kvp.Value.SequenceEqual(Array.Empty<byte>()))
            .Select(kvp => kvp.Key)
            .Except(receivedNames);
    }


    public async Task<ulong> GetLatestVersion()
    {
        using var slot = await _semaphore.LockAsync();
        return LatestVersion;
    }

    public async Task<ChangeSet> GetChangedNames(ulong fromVersion)
    {
        Delta delta;
        var toVersion = await GetLatestVersion();
        using (await _semaphore.LockAsync())
        {
            delta = MergeDeltas(_deltas.Where(d => d.DatasetVersion > fromVersion && d.DatasetVersion <= toVersion));
        }

        return new ChangeSet(toVersion, delta.Changes.Select(d => (d.Name, d.Change)).ToList());

    }

    public async Task<ChangeSet> WaitChangedNames(ulong fromVersion, CancellationToken cancellationToken)
    {
        // The version and the completion source to wait on are read under one lock. Read apart, a write in
        // between completes the completion source that was there and puts a new one in its place, and the
        // wait would miss the write it should have seen (VLOW-30).
        Task changed;
        using (await _semaphore.LockAsync(cancellationToken))
        {
            changed = fromVersion == LatestVersion ? _waitForChangesTaskCompletionSource.Task : Task.CompletedTask;
        }

        await changed.WaitAsync(cancellationToken);
        return await GetChangedNames(fromVersion);
    }

    // This method should be called whenever a new delta is added to the _deltas list.
    private void NotifyDeltaUpdate()
    {
        var tcs = _waitForChangesTaskCompletionSource;
        _waitForChangesTaskCompletionSource = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        tcs.SetResult();
    }

    private Delta MergeDeltas(IEnumerable<Delta> deltasInOrder)
    {
        Dictionary<string, (string, byte[]?, ChangeType)> news = new();
        Dictionary<string, (string, byte[]?, ChangeType)> updates = new();
        Dictionary<string, (string, byte[]?, ChangeType)> deletes = new();
        foreach (var change in deltasInOrder.SelectMany(dio => dio.Changes))
        {
            if (change.Change == ChangeType.New)
            {
                if (!deletes.ContainsKey(change.Name))
                    news[change.Name] = change;
                else
                {
                    deletes.Remove(change.Name);
                    updates[change.Name] = (change.Name, change.Entity, ChangeType.Changed);
                }
            }
            else if (change.Change == ChangeType.Changed)
            {
                if (!news.ContainsKey(change.Name))
                    updates[change.Name] = change;
                else
                    news[change.Name] = (change.Name, change.Entity, ChangeType.New);
            }
            else if (change.Change == ChangeType.Deleted)
            {
                if (news.ContainsKey(change.Name))
                    news.Remove(change.Name);
                else if (updates.ContainsKey(change.Name))
                {
                    updates.Remove(change.Name);
                    deletes[change.Name] = change;
                }
                else
                {
                    deletes[change.Name] = change;
                }
            }
        }

        return new Delta(deletes.Values.Concat(updates.Values).Concat(news.Values).ToArray(), 0U);
    }

    private IEnumerable<(string name, ReadOnlyMemory<byte> entityBytes)> GetAll(ulong version,
        CancellationToken cancellationToken)
    {
        (string name, ReadOnlyMemory<byte> entityBytes)[] snapshot;

        // A synchronous lock: this is an iterator and cannot await.
        using (_semaphore.Lock(cancellationToken))
        {
            // NOTE: doesn't check version
            snapshot = _currentSnapshot.Select(x => (x.Key, new ReadOnlyMemory<byte>(x.Value))).ToArray();
        }

        foreach (var q in snapshot)
            yield return q;
    }

    private readonly Subject<DatasetVersionUpdate> _versionUpdates = new();

    public IObservable<DatasetVersionUpdate> ObserveDatasetChanges(ulong fromVersion)
    {
        using (_semaphore.Lock())
        {
            return LatestVersion > fromVersion
                ? _versionUpdates.Prepend(
                    new DatasetVersionUpdate(LatestVersion, fromVersion))
                : _versionUpdates.AsObservable();
        }
    }

    public async Task<(string name, ReadOnlyMemory<byte> entityBytes)?> Get(string name)
    {
        using (await _semaphore.LockAsync())
        {
            return _currentSnapshot.TryGetValue(name, out var entity) ? (name, new ReadOnlyMemory<byte>(entity)) : null;
        }
    }

    public async Task<(string name, ReadOnlyMemory<byte> entityBytes)?> GetHistoric(string name, ulong version, CancellationToken ct = default)
    {
        using (await _semaphore.LockAsync(ct))
        {
            if (version > LatestVersion)
            {
                throw new ArgumentOutOfRangeException(nameof(version), "Version is in the future");
            }

            var delta = _deltas.FirstOrDefault(d => d.DatasetVersion == version);
            if (delta == null)
            {
                throw new ArgumentException("Delta not found", nameof(version));
            }

            var change = delta.Changes.FirstOrDefault(c => c.Name == name);

            if (change.Name == null)
            {
                throw new ArgumentException($"Entity {name} was not changed in version {version}");
            }

            if (change.Change == ChangeType.Deleted)
            {
                return null;
            }

            if (change.Entity == null)
            {
                return null;
            }

            return (name, new ReadOnlyMemory<byte>(change.Entity));
        }
    }

    public async Task<ICollection<(string, ReadOnlyMemory<byte>?)>> BatchGet(IEnumerable<string> names, string? parent)
    {
        (string name, ReadOnlyMemory<byte>? entityBytes)[] entities;
        using (await _semaphore.LockAsync())
        {
            entities = names.Select(name => ((string name, ReadOnlyMemory<byte>? entityBytes))(_currentSnapshot.TryGetValue(name, out var entity) ? (name, new ReadOnlyMemory<byte>(entity)) : (name, null))).ToArray();
        }

        return entities;
    }

    public Task<(List<string> Names, ulong LatestVersion)> List(string parent, CancellationToken ct = default)
    {
        List<string> names;

        if (string.IsNullOrEmpty(parent) || parent is "/")
        {
            // For root path, return only direct children (names with exactly one "/" segment)
            names = GetAll(ulong.MaxValue, ct)
                .Where(ent => !string.IsNullOrEmpty(ent.name) && ent.name.Count(c => c == '/') == 1)
                .Select(s => s.name)
                .ToList();
        }
        else
        {
            var prefix = parent + "/";
            names = GetAll(ulong.MaxValue, ct)
                .Where(ent => ent.name.StartsWith(prefix, StringComparison.Ordinal))
                .Select(s => s.name).ToList();
        }

        return Task.FromResult<(List<string> Names, ulong LatestVersion)>((names, 0));
    }

    public async Task<uint> CountChildren(string parent, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // Always count all descendants under parent, regardless of nesting level
        // (different from List() which has backwards-compatibility asymmetric behavior)
        using var _ = await _semaphore.LockAsync(cancellationToken);

        if (string.IsNullOrEmpty(parent) || parent is "/")
        {
            // For root path, count all entities
            return (uint)_currentSnapshot.Count;
        }
        else
        {
            var prefix = parent + "/";
            return (uint)_currentSnapshot.Keys.Count(key => key.StartsWith(prefix, StringComparison.Ordinal));
        }
    }




    private IDatasetWriter<T>.WriteResponse WriteResponseFor(Delta delta) =>
        new()
        {
            NewDatasetVersion = delta.DatasetVersion,
            NumAddedEntities = (uint)delta.Changes.Count(ch => ch.Change == ChangeType.New),
            NumUpdatedEntities = (uint)delta.Changes.Count(ch => ch.Change == ChangeType.Changed),
            NumDeletedEntities = (uint)delta.Changes.Count(ch => ch.Change == ChangeType.Deleted),
        };

    private AliasStaging NewAliasStaging() => new(_aliasIndex, _aliasDefinitions, _aliasDefinitionsHash);

    public string? ResolveAlias(string aliasName, string alias) => _aliasIndex.ResolveAlias(aliasName, alias);

    public (IReadOnlyList<string> Aliases, ulong Version) ListAliases(string aliasName, string prefix)
    {
        var index = _aliasIndex;
        return (index.ListAliases(aliasName, prefix), index.LatestVersion);
    }

    private void ApplyDelta(Delta delta, AliasStaging staging)
    {
        var fromVersion = LatestVersion;

        // The same index the real writer keeps, without the data positions: it holds the aliases
        var deltaIndex = new DeltaIndex { DeltaVersion = delta.DatasetVersion };
        foreach (var (name, entity, change) in delta.Changes)
        {
            if (change == ChangeType.Deleted)
                deltaIndex.DeletedEntities.Add(new DeletedEntity { Name = name });
            else
                (change == ChangeType.New ? deltaIndex.NewEntities : deltaIndex.ChangedEntities)
                    .Add(new DeltaEntity { Name = name, Crc32 = DatasetHelper.ComputeCrc32(entity) });
        }

        staging.AttachTo(deltaIndex);
        _aliasIndex = _aliasIndex.ApplyDelta(deltaIndex);

        _deltas.Add(delta);
        NotifyDeltaUpdate();
        foreach (var change in delta.Changes)
        {
            if (change.Change == ChangeType.Deleted)
                _currentSnapshot.Remove(change.Name);
            else
                _currentSnapshot[change.Name] = change.Entity!;
        }

        try
        {
            var datasetVersionUpdate = new DatasetVersionUpdate(
                LatestVersion, fromVersion);
            _versionUpdates.OnNext(datasetVersionUpdate);
        }
        catch
        {
            // ignored
        }
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public Task<List<HistoryEntry>> GetHistory(string name, int limit = 0, ulong minVersion = 0, CancellationToken ct = default)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<HistoryEntry> StreamHistory(string parent, ulong sinceVersion = 0, ulong maxVersion = 0, bool waitIfNone = false, CancellationToken ct = default)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerable<HistoryEntry> TailHistory(string parent, ulong sinceVersion = 0, bool follow = false, CancellationToken ct = default)
    {
        throw new NotImplementedException();
    }

    public Task<HistoryChangeSet> GetHistoryChanges(ulong fromVersion, CancellationToken ct = default)
    {
        throw new NotImplementedException();
    }

    public Task<HistoryChangeSet> WaitHistoryChanges(ulong fromVersion, CancellationToken ct = default)
    {
        throw new NotImplementedException();
    }

    private record Delta((string Name, byte[]? Entity, ChangeType Change)[] Changes, ulong DatasetVersion);
}


