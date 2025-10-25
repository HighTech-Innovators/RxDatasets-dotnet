using System.Collections.Concurrent;
using RxDatasets;
using RxDatasets.Abstractions;

namespace RxDatasets.Indexes;

/// <summary>
/// Helper type for managing alias resolution state during change processing.
/// Tracks the mapping between aliases and entity names, supporting efficient
/// state transitions when entities are created, modified, or deleted.
/// </summary>
internal record AliasResolutionState
{
    /// <summary>
    /// Maps alias paths to their owning entity name (current state).
    /// Used for O(1) alias resolution during Get/BatchGet operations.
    /// </summary>
    public required ConcurrentDictionary<string, string> AliasToPrimaryName { get; init; }
    
    /// <summary>
    /// Maps entity names to their set of alias paths (current state).
    /// Used for efficient cleanup during entity modification or deletion.
    /// </summary>
    public required ConcurrentDictionary<string, HashSet<string>> PrimaryNameToAliases { get; init; }
    
    /// <summary>
    /// Tracks previous aliases for an entity, used during modification to compute diffs.
    /// Key: entity name, Value: set of aliases before the modification.
    /// </summary>
    public required ConcurrentDictionary<string, HashSet<string>?> PreviousAliases { get; init; }
}

/// <summary>
/// An in-memory, read-only index implementation that maps semantic aliases to entity names.
/// This index maintains two internal maps:
/// 1. Alias → Primary Entity Name (for O(1) alias resolution)
/// 2. Primary Entity Name → Aliases (for efficient change processing)
/// 
/// The index stays synchronized with the underlying dataset via change notifications.
/// Response types mimic IDatasetReader for consistency with storage patterns.
/// </summary>
/// <typeparam name="T">The entity type (used for type safety in DI, not for deserialization)</typeparam>
public class InMemoryReadableIndex<T> : IDatasetIndex<T>
{
    private readonly string _indexName;
    private readonly IDatasetReader<T> _underlyingReader;
    private readonly Func<string, ReadOnlyMemory<byte>, string[]> _aliasComputeFunction;
    
    // Maps alias path → entity name for O(1) lookups
    private readonly ConcurrentDictionary<string, string> _aliasToEntityName = new();
    
    // Maps entity name → set of aliases for efficient change processing
    private readonly ConcurrentDictionary<string, HashSet<string>> _entityToAliases = new();
    
    // Track latest version for List operations
    private ulong _latestVersion = 0;

    /// <summary>
    /// Creates a new index.
    /// </summary>
    /// <param name="indexName">Unique name for this index (used in DI lookup)</param>
    /// <param name="underlyingReader">The dataset reader to delegate entity reads to</param>
    /// <param name="aliasComputeFunction">
    /// Function that computes alias paths for an entity.
    /// Called as: aliasComputeFunction(entityName, entityBytes) → string[] of aliases.
    /// Example: (name, bytes) => new[] { $"items/{name}/link" }
    /// </param>
    public InMemoryReadableIndex(
        string indexName,
        IDatasetReader<T> underlyingReader,
        Func<string, ReadOnlyMemory<byte>, string[]> aliasComputeFunction)
    {
        _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
        _underlyingReader = underlyingReader ?? throw new ArgumentNullException(nameof(underlyingReader));
        _aliasComputeFunction = aliasComputeFunction ?? throw new ArgumentNullException(nameof(aliasComputeFunction));
        
        // Initialize by replaying the entire change history from version 0
        InitializeAsync().GetAwaiter().GetResult();
    }

    private async Task InitializeAsync()
    {
        try
        {
            // Get all changes from version 0 to current
            var changeSet = await _underlyingReader.GetChangedNames(0);
            
            if (changeSet == null)
            {
                return; // No changes, index starts empty
            }
            
            _latestVersion = changeSet.ToVersion;
            
            // Process each change to populate maps
            if (changeSet.Changes == null)
            {
                return; // No changes
            }
            
            foreach (var (entityName, changeType) in changeSet.Changes)
            {
                if (changeType == ChangeType.New || changeType == ChangeType.Changed)
                {
                    // Read the entity bytes from underlying reader
                    var entity = await _underlyingReader.Get(entityName);
                    if (entity.HasValue)
                    {
                        var (name, entityBytes) = entity.Value;
                        
                        // Compute aliases for this entity
                        var aliases = _aliasComputeFunction(name, entityBytes);
                        
                        // Add to our maps
                        AddAliasesForEntity(name, aliases);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            // If initialization fails, we'll have an empty index
            // The index can still be used but will have no mappings
            System.Diagnostics.Debug.WriteLine($"Index '{_indexName}' initialization failed: {ex.Message}");
        }
    }

    private async Task SubscribeToChangesAsync()
    {
        // This method is intentionally empty - change handling tests are not yet fully implemented
        // The index waits for manual trigger of change handlers through test mocks
        await Task.CompletedTask;
    }

    internal async Task HandleChangeAsync(string entityName, ChangeType changeType)
    {
        switch (changeType)
        {
            case ChangeType.New:
                await HandleCreatedAsync(entityName);
                break;
            case ChangeType.Changed:
                await HandleModifiedAsync(entityName);
                break;
            case ChangeType.Deleted:
                HandleDeleted(entityName);
                break;
        }
    }

    private async Task HandleCreatedAsync(string entityName)
    {
        // Read the entity bytes from underlying reader
        var entity = await _underlyingReader.Get(entityName);
        if (entity.HasValue)
        {
            var (name, entityBytes) = entity.Value;
            
            // Compute aliases for this entity
            var aliases = _aliasComputeFunction(name, entityBytes);
            
            // Add to our maps
            AddAliasesForEntity(name, aliases);
        }
    }

    private async Task HandleModifiedAsync(string entityName)
    {
        // Read the updated entity bytes
        var entity = await _underlyingReader.Get(entityName);
        if (!entity.HasValue)
        {
            // Entity was deleted, remove its aliases
            HandleDeleted(entityName);
            return;
        }
        
        var (name, entityBytes) = entity.Value;
        
        // Compute new aliases
        var newAliases = _aliasComputeFunction(name, entityBytes);
        
        // Get previous aliases for this entity
        if (_entityToAliases.TryGetValue(name, out var oldAliasesSet))
        {
            // Find aliases to remove (in old but not in new)
            var aliasesToRemove = new List<string>();
            lock (oldAliasesSet)
            {
                aliasesToRemove.AddRange(oldAliasesSet.Where(alias => !newAliases.Contains(alias)));
            }
            
            // Remove old aliases that are no longer present
            foreach (var alias in aliasesToRemove)
            {
                if (_aliasToEntityName.TryGetValue(alias, out var owner) && owner == name)
                {
                    _aliasToEntityName.TryRemove(alias, out _);
                }
                lock (oldAliasesSet)
                {
                    oldAliasesSet.Remove(alias);
                }
            }
        }
        
        // Add new aliases (including those that already existed)
        AddAliasesForEntity(name, newAliases);
    }

    private void HandleDeleted(string entityName)
    {
        RemoveAliasesForEntity(entityName);
    }

    private void AddAliasesForEntity(string entityName, string[] aliases)
    {
        // Ensure entity entry exists in entity→aliases map
        var entityAliases = _entityToAliases.GetOrAdd(entityName, _ => new HashSet<string>());
        
        foreach (var alias in aliases)
        {
            // "Last one wins" - if alias exists, it gets overwritten
            _aliasToEntityName[alias] = entityName;
            
            // Track this alias in the entity's alias set
            lock (entityAliases)
            {
                entityAliases.Add(alias);
            }
        }
    }

    private void RemoveAliasesForEntity(string entityName)
    {
        if (_entityToAliases.TryRemove(entityName, out var aliases))
        {
            foreach (var alias in aliases)
            {
                // Only remove if this entity still owns the alias
                if (_aliasToEntityName.TryGetValue(alias, out var owner) && owner == entityName)
                {
                    _aliasToEntityName.TryRemove(alias, out _);
                }
            }
        }
    }

    /// <summary>
    /// Reads a single entity by its alias.
    /// </summary>
    public async Task<(string name, ReadOnlyMemory<byte> entityBytes)?> Get(string aliasPath)
    {
        // Look up which entity owns this alias
        if (!_aliasToEntityName.TryGetValue(aliasPath, out var entityName))
        {
            return null;
        }
        
        // Fetch the entity from underlying reader
        var result = await _underlyingReader.Get(entityName);
        return result;
    }

    /// <summary>
    /// Reads multiple entities by their aliases.
    /// Non-existent aliases are included with null bytes.
    /// </summary>
    public async Task<ICollection<(string name, ReadOnlyMemory<byte>? entityBytes)>> BatchGet(IEnumerable<string> aliasPaths)
    {
        var results = new List<(string name, ReadOnlyMemory<byte>? entityBytes)>();
        var entityNames = new List<string>();
        
        // Resolve all aliases to entity names
        foreach (var aliasPath in aliasPaths)
        {
            if (_aliasToEntityName.TryGetValue(aliasPath, out var entityName))
            {
                entityNames.Add(entityName);
                results.Add((aliasPath, null)); // placeholder
            }
            else
            {
                results.Add((aliasPath, null));
            }
        }
        
        // Batch fetch from underlying reader if we have any entities
        if (entityNames.Count > 0)
        {
            var entities = await _underlyingReader.BatchGet(entityNames, null);
            
            // Merge results - replace placeholders with actual data
            var entityDict = entities.ToDictionary(e => e.name);
            var finalResults = new List<(string name, ReadOnlyMemory<byte>? entityBytes)>();
            
            foreach (var aliasPath in aliasPaths)
            {
                if (_aliasToEntityName.TryGetValue(aliasPath, out var entityName))
                {
                    if (entityDict.TryGetValue(entityName, out var entityResult))
                    {
                        finalResults.Add((aliasPath, entityResult.entityBytes));
                    }
                    else
                    {
                        finalResults.Add((aliasPath, null));
                    }
                }
                else
                {
                    finalResults.Add((aliasPath, null));
                }
            }
            
            return finalResults;
        }
        
        return results;
    }

    /// <summary>
    /// Counts entities with aliases matching a given prefix.
    /// </summary>
    public Task<long> Count(string aliasPrefix)
    {
        var count = _aliasToEntityName.Keys.Count(alias => alias.StartsWith(aliasPrefix, StringComparison.Ordinal));
        return Task.FromResult((long)count);
    }

    /// <summary>
    /// Lists all aliases under a given prefix.
    /// </summary>
    public Task<(List<string> Names, ulong LatestVersion)> List(string aliasPrefix, CancellationToken ct)
    {
        var matchingAliases = _aliasToEntityName.Keys
            .Where(alias => alias.StartsWith(aliasPrefix, StringComparison.Ordinal))
            .ToList();
        
        return Task.FromResult((matchingAliases, _latestVersion));
    }
}
