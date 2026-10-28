namespace RxDatasets.Abstractions;

public record ChangeSet(ulong ToVersion, List<(string Name, ChangeType ChangeType)> Changes);
public record HistoryChangeSet(ulong ToVersion, IReadOnlyList<HistoryChange> Changes);
public interface IDatasetReader<T>
{
    Task<ChangeSet> GetChangedNames(ulong fromVersion);
    Task<ChangeSet> WaitChangedNames(ulong fromVersion, CancellationToken cancellationToken);
    Task<(string name, ReadOnlyMemory<byte> entityBytes)?> Get(string name);
    Task<ICollection<(string name, ReadOnlyMemory<byte>? entityBytes)>> BatchGet(IEnumerable<string> names, string? parent);
    Task<(List<string> Names, ulong LatestVersion)> List(string parent, CancellationToken ct);
    Task<uint> CountChildren(string parent, CancellationToken cancellationToken = default);
    Task<List<HistoryEntry>> GetHistory(string name, int limit = 0, ulong minVersion = 0, CancellationToken ct = default);
    IAsyncEnumerable<HistoryEntry> StreamHistory(string parent, ulong sinceVersion = 0, ulong maxVersion = 0, bool waitIfNone = false, CancellationToken ct = default);
    IAsyncEnumerable<HistoryEntry> TailHistory(string parent, ulong sinceVersion = 0, bool follow = false, CancellationToken ct = default);
    Task<(string name, ReadOnlyMemory<byte> entityBytes)?> GetHistoric(string name, ulong version, CancellationToken ct = default);

    /// <summary>
    /// The names changed after <paramref name="fromVersion"/>, as <see cref="GetChangedNames"/> reports them, each with
    /// the tracking id and SLA timestamp of its last change; read from the deltas, not the index.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="fromVersion"/> is newer than the latest version.</exception>
    /// <exception cref="ResyncRequiredException">A delta after <paramref name="fromVersion"/> is no longer kept.</exception>
    Task<HistoryChangeSet> GetHistoryChanges(ulong fromVersion, CancellationToken ct = default);

    /// <summary><see cref="GetHistoryChanges"/>, once there is a version after <paramref name="fromVersion"/>.</summary>
    Task<HistoryChangeSet> WaitHistoryChanges(ulong fromVersion, CancellationToken ct = default);
}
