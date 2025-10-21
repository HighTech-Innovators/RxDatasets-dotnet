namespace RxDatasets;

/// <summary>
/// Represents a read-only index for accessing entities by semantic aliases.
/// An index maps alias paths to entity names, enabling efficient lookups via meaningful identifiers.
/// Mimics the response types of IDatasetReader for consistency.
/// </summary>
/// <typeparam name="T">The entity type (for DI registration and type safety)</typeparam>
public interface IDatasetIndex<T>
{
    /// <summary>
    /// Reads a single entity by its alias path, returning the same response type as IDatasetReader.Get.
    /// </summary>
    /// <param name="aliasPath">The semantic alias path to the entity (e.g., "items/item1/links/l001")</param>
    /// <returns>A tuple of (entity name, entity bytes) if found, or null if alias does not exist</returns>
    Task<(string name, ReadOnlyMemory<byte> entityBytes)?> Get(string aliasPath);

    /// <summary>
    /// Reads multiple entities by their alias paths, returning the same response type as IDatasetReader.BatchGet.
    /// </summary>
    /// <param name="aliasPaths">The semantic alias paths to entities</param>
    /// <returns>A collection of tuples with (name, bytes) for found aliases; non-existent aliases return null bytes</returns>
    Task<ICollection<(string name, ReadOnlyMemory<byte>? entityBytes)>> BatchGet(IEnumerable<string> aliasPaths);

    /// <summary>
    /// Counts the number of entities with alias paths matching a given prefix.
    /// Useful for implementing tree/folder navigation queries.
    /// </summary>
    /// <param name="aliasPrefix">The prefix to match (e.g., "items/item1/links")</param>
    /// <returns>The count of matching aliases</returns>
    Task<long> Count(string aliasPrefix);

    /// <summary>
    /// Lists all aliases under a given prefix, returning the same response type as IDatasetReader.List.
    /// </summary>
    /// <param name="aliasPrefix">The prefix to match (e.g., "items/item1/links")</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>A tuple of (list of matching aliases, latest version)</returns>
    Task<(List<string> Names, ulong LatestVersion)> List(string aliasPrefix, CancellationToken ct);
}
