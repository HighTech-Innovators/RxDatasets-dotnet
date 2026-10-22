namespace RxDatasets.Abstractions;

/// <summary>Alias lookups on the index a dataset reader is serving, at the same version as its entities.</summary>
public interface IAliasIndex
{
    /// <summary>The name of the entity that owns <paramref name="alias"/> under <paramref name="aliasName"/>, or null.</summary>
    string? ResolveAlias(string aliasName, string alias);

    /// <summary>
    /// The aliases under <paramref name="aliasName"/> that equal <paramref name="prefix"/> or start with it followed
    /// by '/' (all of them for an empty prefix), and the index version they are from.
    /// </summary>
    (IReadOnlyList<string> Aliases, ulong Version) ListAliases(string aliasName, string prefix);
}
