namespace RxDatasets.Abstractions;

public record ChangeSet(ulong ToVersion, List<(string Name, ChangeType ChangeType)> Changes)
{
    /// <summary>
    /// The origin of the latest change of each changed name whose write passed a tracking id or an SLA timestamp;
    /// names whose write passed neither are absent.
    /// </summary>
    public IReadOnlyDictionary<string, ChangeOrigin> Origins { get; init; } = new Dictionary<string, ChangeOrigin>();
}
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
}
