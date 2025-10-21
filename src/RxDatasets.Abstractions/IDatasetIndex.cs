namespace RxDatasets;

/// <summary>
/// Represents a read-only index for accessing entities by semantic aliases.
/// An index maps alias paths to entity primary names, enabling efficient lookups via meaningful identifiers.
/// </summary>
/// <typeparam name="TEntity">The type of entity stored in the dataset</typeparam>
public interface IDatasetIndex<TEntity> where TEntity : class
{
    /// <summary>
    /// Reads a single entity by its alias path.
    /// </summary>
    /// <param name="aliasPath">The semantic alias path to the entity (e.g., "items/item1/links/l001")</param>
    /// <returns>The entity if found</returns>
    /// <exception cref="IndexOutOfRangeException">Thrown if the alias is not found in the index</exception>
    Task<TEntity> Read(string aliasPath);

    /// <summary>
    /// Reads multiple entities by their alias paths, returning results as an async enumerable.
    /// Non-existent aliases are silently skipped without raising errors.
    /// </summary>
    /// <param name="aliasPaths">The semantic alias paths to entities</param>
    /// <returns>An async enumerable yielding entities found in the index</returns>
    IAsyncEnumerable<TEntity> BatchRead(IEnumerable<string> aliasPaths);

    /// <summary>
    /// Counts the number of entities with alias paths matching a given prefix.
    /// Useful for implementing tree/folder navigation queries (e.g., "How many links are under items/item1/links?")
    /// </summary>
    /// <param name="aliasPrefix">The prefix to match (e.g., "items/item1/links")</param>
    /// <returns>The count of matching aliases</returns>
    Task<long> CountChildren(string aliasPrefix);
}
