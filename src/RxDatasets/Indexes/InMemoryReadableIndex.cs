using RxDatasets;
using RxDatasets.Abstractions;

namespace RxDatasets.Indexes;

/// <summary>
/// An in-memory, read-only index implementation that maps semantic aliases to entities.
/// This index maintains two internal maps:
/// 1. Alias → Primary Entity Name (for O(1) alias resolution)
/// 2. Primary Entity Name → Aliases (for efficient change processing)
/// 
/// The index stays synchronized with the underlying dataset via change notifications.
/// </summary>
/// <typeparam name="TEntity">The type of entity stored in the dataset</typeparam>
public class InMemoryReadableIndex<TEntity> : IDatasetIndex<TEntity> where TEntity : class
{
    /// <summary>
    /// Creates a new index.
    /// </summary>
    /// <param name="indexName">Unique name for this index (used in DI lookup)</param>
    /// <param name="underlyingReader">The dataset reader to delegate entity reads to</param>
    /// <param name="aliasMappingFunction">
    /// Function that computes alias paths for an entity.
    /// Called as: aliasMappingFunction(entityName, entity) → string[] of aliases.
    /// Example: (name, link) => new[] { $"items/{link.ItemId}/links/{link.LinkId}" }
    /// </param>
    public InMemoryReadableIndex(
        string indexName,
        IDatasetReader<TEntity> underlyingReader,
        Func<string, TEntity, string[]> aliasMappingFunction)
    {
        throw new NotImplementedException("T014: Constructor and initialization logic to be implemented");
    }

    /// <summary>
    /// Reads a single entity by its alias.
    /// </summary>
    public Task<TEntity> Read(string aliasPath)
    {
        throw new NotImplementedException("T016: Read method to be implemented");
    }

    /// <summary>
    /// Reads multiple entities by their aliases.
    /// Non-existent aliases are silently skipped.
    /// </summary>
    public IAsyncEnumerable<TEntity> BatchRead(IEnumerable<string> aliasPaths)
    {
        throw new NotImplementedException("T017: BatchRead method to be implemented");
    }

    /// <summary>
    /// Counts entities with aliases matching a given prefix.
    /// </summary>
    public Task<long> CountChildren(string aliasPrefix)
    {
        throw new NotImplementedException("T018: CountChildren method to be implemented");
    }
}
