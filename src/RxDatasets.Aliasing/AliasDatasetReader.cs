using Google.Protobuf;
using RxDatasets.Abstractions;

namespace RxDatasets.Aliasing;

/// <summary>
/// An <see cref="IDatasetReader{T}"/> view that looks entities up by an alias of one alias name. The aliases live in
/// the dataset's own index (see <see cref="IAliasIndex"/>), so a lookup is at the same version as the entities: in
/// shared reader/writer mode an alias is resolvable as soon as the write that claimed it returns.
/// </summary>
/// <typeparam name="T">The DTO type, must be a protobuf message</typeparam>
public class AliasDatasetReader<T> : IDatasetReader<T>
    where T : class, IMessage<T>, new()
{
    private readonly IDatasetReader<T> _sourceReader;
    private readonly IAliasIndex _aliasIndex;
    private readonly string _aliasName;
    private readonly Func<string, ReadOnlyMemory<byte>, IEnumerable<string>> _aliasFactory;

    /// <param name="sourceReader">The dataset reader; it must also be an <see cref="IAliasIndex"/>.</param>
    /// <param name="aliasName">The alias name this view resolves.</param>
    /// <param name="aliasFactory">The alias factory of the alias name, for <see cref="AliasingExtensions.GetAliasesFor{T}"/>.</param>
    public AliasDatasetReader(IDatasetReader<T> sourceReader, string aliasName,
        Func<string, ReadOnlyMemory<byte>, IEnumerable<string>> aliasFactory)
    {
        _sourceReader = sourceReader ?? throw new ArgumentNullException(nameof(sourceReader));
        _aliasIndex = sourceReader as IAliasIndex
                      ?? throw new ArgumentException($"{sourceReader.GetType().Name} keeps no aliases", nameof(sourceReader));
        _aliasName = aliasName ?? throw new ArgumentNullException(nameof(aliasName));
        _aliasFactory = aliasFactory ?? throw new ArgumentNullException(nameof(aliasFactory));
    }

    public async Task<(string name, ReadOnlyMemory<byte> entityBytes)?> Get(string alias) =>
        _aliasIndex.ResolveAlias(_aliasName, alias) is { } name ? await _sourceReader.Get(name) : null;

    public async Task<ICollection<(string name, ReadOnlyMemory<byte>? entityBytes)>> BatchGet(IEnumerable<string> aliases,
        string? parent = null)
    {
        var names = aliases.Select(alias => _aliasIndex.ResolveAlias(_aliasName, alias)).OfType<string>().ToList();
        return names.Count == 0 ? [] : await _sourceReader.BatchGet(names, parent);
    }

    /// <summary>All aliases, as new, at the current version.</summary>
    public Task<ChangeSet> GetChangedNames(ulong fromVersion)
    {
        var (aliases, version) = _aliasIndex.ListAliases(_aliasName, "");
        return Task.FromResult(new ChangeSet(version, aliases.Select(alias => (alias, ChangeType.New)).ToList()));
    }

    /// <summary>Waits for the dataset to change; the changes themselves are only reported by the dataset reader.</summary>
    public async Task<ChangeSet> WaitChangedNames(ulong fromVersion, CancellationToken cancellationToken = default)
    {
        var changes = await _sourceReader.WaitChangedNames(fromVersion, cancellationToken);
        return new ChangeSet(changes.ToVersion, []);
    }

    /// <summary>The aliases that start with <paramref name="parent"/>/, sorted.</summary>
    public Task<(List<string> Names, ulong LatestVersion)> List(string parent, CancellationToken cancellationToken = default) =>
        Task.FromResult(ListChildren(parent));

    public Task<uint> CountChildren(string parent, CancellationToken cancellationToken = default) =>
        Task.FromResult((uint)ListChildren(parent).Names.Count);

    private (List<string> Names, ulong LatestVersion) ListChildren(string parent)
    {
        var parentPrefix = parent + "/";
        var (aliases, version) = _aliasIndex.ListAliases(_aliasName, parent);
        var children = aliases.Where(alias => alias.StartsWith(parentPrefix, StringComparison.Ordinal)).ToList();
        children.Sort(StringComparer.Ordinal);
        return (children, version);
    }

    public Task<(string name, ReadOnlyMemory<byte> entityBytes)?> GetHistoric(string name, ulong version,
        CancellationToken ct = default) =>
        throw new InvalidOperationException("Can't provide history by alias");

    public Task<List<HistoryEntry>> GetHistory(string name, int limit = 0, ulong minVersion = 0, CancellationToken ct = default) =>
        throw new InvalidOperationException("Can't provide history by alias");

    public IAsyncEnumerable<HistoryEntry> StreamHistory(string parent, ulong sinceVersion = 0, ulong maxVersion = 0,
        bool waitIfNone = false, CancellationToken ct = default) =>
        throw new InvalidOperationException("Can't provide history by alias");

    public IAsyncEnumerable<HistoryEntry> TailHistory(string parent, ulong sinceVersion = 0, bool follow = false,
        CancellationToken ct = default) =>
        throw new NotImplementedException("TailHistory is not supported for alias datasets");

    /// <summary>
    /// The aliases this alias name would give the entity, as (alias name, alias value); none when the factory can't
    /// evaluate it. The write itself rejects such an entity.
    /// </summary>
    internal IEnumerable<(string Name, string Value)> EvaluateAliases(string entityId, ReadOnlyMemory<byte> bytes)
    {
        List<string> aliases;
        try
        {
            aliases = _aliasFactory(entityId, bytes).ToList();
        }
        catch (Exception)
        {
            aliases = [];
        }

        return aliases.Select(alias => (_aliasName, alias));
    }
}
