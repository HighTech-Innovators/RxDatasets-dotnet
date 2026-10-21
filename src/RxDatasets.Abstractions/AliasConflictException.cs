namespace RxDatasets.Abstractions;

/// <summary>
/// A write would leave an alias owned by two live entities. Nothing of the write is stored. Not an
/// <see cref="InvalidOperationException"/>, so callers don't mistake it for a dataset that is not ready.
/// </summary>
public class AliasConflictException(string aliasName, string alias, string entityName, string ownerName)
    : Exception($"Alias '{alias}' ({aliasName}) of '{entityName}' is already owned by '{ownerName}'")
{
    public string AliasName { get; } = aliasName;
    public string Alias { get; } = alias;

    /// <summary>The entity whose write claimed the alias.</summary>
    public string EntityName { get; } = entityName;

    /// <summary>The entity that owns the alias.</summary>
    public string OwnerName { get; } = ownerName;
}
