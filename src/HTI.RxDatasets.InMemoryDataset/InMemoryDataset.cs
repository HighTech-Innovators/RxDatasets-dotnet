using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Runtime.CompilerServices;
using HTI.RxDatasets.Abstractions;

namespace HTI.RxDatasets.InMemoryDataset;

public record DatasetVersionUpdate(ulong NewDatasetVersion, ulong PreviousDatasetVersion);

public class InMemoryDataset<T> : IDatasetReader<T>, IDatasetWriter<T>
{
    private readonly SemaphoreSlim _semaphore = new(1);
    private ulong LatestVersion => _deltas.LastOrDefault()?.DatasetVersion ?? 0;
    private readonly Dictionary<string, byte[]> _currentSnapshot = [];
    private readonly List<Delta> _deltas = [];
    private TaskCompletionSource _waitForChangesTaskCompletionSource = new();

    public async Task<IDatasetWriter<T>.WriteResponse> Delete(string name, CancellationToken ct = default)        
    {
        using var slot = await Lock(ct);
        if (!_currentSnapshot.ContainsKey(name))
            return new IDatasetWriter<T>.WriteResponse {NewDatasetVersion = LatestVersion};
        var delta = new Delta(new[] {(name, (byte[]?) null, ChangeType.Deleted)}, LatestVersion + 1);
        ApplyDelta(delta);
        return WriteResponseFor(delta);
    }

    public async Task<IDatasetWriter<T>.WriteResponse> Put(string name, byte[] newEntity, CancellationToken ct = default)
    {
        using var slot = await Lock(ct);
        Delta delta;
        if (!_currentSnapshot.TryGetValue(name, out var oldEntity))
        {
            delta = new Delta(new[] { (name, (byte[]?) newEntity, ChangeType.New) }, LatestVersion + 1);
        }
        else
        {
            if (newEntity?.Length == oldEntity.Length && newEntity.SequenceEqual(oldEntity))
                return new IDatasetWriter<T>.WriteResponse {NewDatasetVersion = LatestVersion};

            delta = new Delta(new[] {(name, newEntity, ChangeType.Changed)}, LatestVersion + 1);
        }

        ApplyDelta(delta);
        return WriteResponseFor(delta);
    }

    public async Task<IDatasetWriter<T>.WriteResponse> PutAll(IAsyncEnumerable<(string name, byte[] entityBytes)> entities, CancellationToken ct = default){
        using var slot = await Lock(ct);
        var changes = new Dictionary<string, (string Name, byte[]? Entity, ChangeType Change)>();
        var receivedNames = new HashSet<string>();
        await foreach (var (name, entityBytes) in entities.WithCancellation(ct))
        {
            receivedNames.Add(name);
            if (!_currentSnapshot.TryGetValue(name, out var oldEntityBytes))
            {
                changes[name] = (name, (byte[]?)entityBytes, ChangeType.New);
            }
            else
            {
                if (entityBytes.Length == oldEntityBytes.Length && entityBytes.SequenceEqual(oldEntityBytes))
                    continue;

                changes[name] = (name, (byte[]?)entityBytes, ChangeType.Changed);
            }
        }

        var implicitDeletes = _currentSnapshot.Keys.Except(receivedNames);
        foreach (var name in implicitDeletes)
            changes[name] = (name, null, ChangeType.Deleted);

        if (changes.Count == 0)
        {
            return new IDatasetWriter<T>.WriteResponse { NewDatasetVersion = LatestVersion };
        }

        var theDelta = new Delta([.. changes.Values], LatestVersion + 1);

        ApplyDelta(theDelta);
        return WriteResponseFor(theDelta);
    }

    public async Task<ulong> GetLatestVersion()
    {
        using var slot = await Lock();
        return LatestVersion;
    }

    public async Task<ChangeSet> GetChangedNames(ulong fromVersion)
    {
        Delta delta;
        var toVersion = await GetLatestVersion();
        using (await Lock())
        {
            delta = MergeDeltas(_deltas.Where(d => d.DatasetVersion > fromVersion && d.DatasetVersion <= toVersion));
        }

        return new ChangeSet(toVersion, delta.Changes.Select(d=>(d.Name, d.Change)).ToList());

    }

    public async Task<ChangeSet> WaitChangedNames(ulong fromVersion, CancellationToken cancellationToken)
    {
        var toVersion = await GetLatestVersion();

        if (fromVersion == toVersion)
        {
            await _waitForChangesTaskCompletionSource.Task.WaitAsync(cancellationToken);
        }

        return await GetChangedNames(fromVersion);
    }

    // This method should be called whenever a new delta is added to the _deltas list.
    private void NotifyDeltaUpdate()
    {
        var tcs = _waitForChangesTaskCompletionSource;
        _waitForChangesTaskCompletionSource = new TaskCompletionSource();
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

    private async IAsyncEnumerable<(string name, ReadOnlyMemory<byte> entityBytes)> GetAll(ulong version,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        (string name, ReadOnlyMemory<byte> entityBytes)[] snapshot;
        using (await Lock(cancellationToken))
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
        using (Lock().Result)
        {
            return LatestVersion > fromVersion
                ? _versionUpdates.Prepend(
                    new DatasetVersionUpdate(LatestVersion, fromVersion))
                : _versionUpdates.AsObservable();
        }
    }

    public async Task<(string name, ReadOnlyMemory<byte> entityBytes)?> Get(string name)
    {
        using (await Lock())
        {
            return _currentSnapshot.TryGetValue(name, out var entity) ? (name, new ReadOnlyMemory<byte>(entity)) : null;
        }
    }

    public async IAsyncEnumerable<(string, ReadOnlyMemory<byte>)?> BatchGet(IEnumerable<string> names, string? parent)
    {
        (string name, ReadOnlyMemory<byte> entityBytes)?[] snapshot;
        using (await Lock())
        {
            snapshot = names.Select(name =>
            {
                return ((string name, ReadOnlyMemory<byte> entityBytes)?) (_currentSnapshot.TryGetValue(name, out var entity) ? (name, new ReadOnlyMemory<byte>(entity)) : null);
            }).ToArray();
        }

        foreach (var q in snapshot)
            yield return q;
    }

    public IAsyncEnumerable<(string name, ReadOnlyMemory<byte> entityBytes)> List(string parent)
    {
        var prefix = parent + "/";
        var a = GetAll(ulong.MaxValue, CancellationToken.None);
        return a.Where(ent => ent.name.StartsWith(prefix));
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

    private class ActionDisposable : IDisposable
    {
        private readonly Action _action;

        public ActionDisposable(Action action)
        {
            _action = action ?? throw new ArgumentNullException(nameof(action));
        }

        public void Dispose()
        {
            _action();
        }
    }

    private IDatasetWriter<T>.WriteResponse WriteResponseFor(Delta delta) =>
        new()
        {
            NewDatasetVersion = delta.DatasetVersion,
            NumAddedEntities = (uint) delta.Changes.Count(ch => ch.Change == ChangeType.New),
            NumUpdatedEntities = (uint) delta.Changes.Count(ch => ch.Change == ChangeType.Changed),
            NumDeletedEntities = (uint) delta.Changes.Count(ch => ch.Change == ChangeType.Deleted),
        };

    private void ApplyDelta(Delta delta)
    {
        var fromVersion = LatestVersion;

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

    private record Delta((string Name, byte[]? Entity, ChangeType Change)[] Changes, ulong DatasetVersion);
}