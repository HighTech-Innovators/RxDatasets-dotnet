namespace HTI.RxDatasets.Abstractions;

public record ChangeSet(ulong toVersion, ICollection<(string name, ChangeType changeType)> changes);
public interface IDatasetReader<T>
{
    Task<ulong> GetLatestVersion();
    Task<ChangeSet> GetChangedNames(ulong fromVersion);
    Task<ChangeSet> WaitChangedNames(ulong fromVersion, CancellationToken cancellationToken);
    Task<(string name, ReadOnlyMemory<byte> entityBytes)?> Get(string name);
    IAsyncEnumerable<(string name, ReadOnlyMemory<byte> entityBytes)?> BatchGet(IEnumerable<string> names, string? parent);
    IAsyncEnumerable<(string name, ReadOnlyMemory<byte> entityBytes)> List(string parent);
}