using Google.Protobuf;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;

namespace RxDatasets.Aliasing;

/// <summary>
/// An IDatasetReader&lt;T&gt; implementation that allows querying entities by their aliases.
/// It maintains a bidirectional mapping between aliases and entity IDs, automatically
/// synchronizing with the upstream reader to stay current with changes.
/// </summary>
/// <typeparam name="T">The DTO type, must be a protobuf message</typeparam>
public class AliasDatasetReader<T> : IDatasetReader<T>
    where T : class, IMessage<T>, new()
{
    private readonly IDatasetReader<T> _sourceReader;
    private readonly Func<ReadOnlyMemory<byte>, IEnumerable<string>> _aliasFactory;
    private readonly string _aliasName;
    private readonly ILogger _logger;
    private readonly IHostApplicationLifetime _appLifetime;
    private readonly IClock _clock;

    // Bidirectional mappings
    // Maps: alias -> original ID (for resolving aliases to IDs)
    private readonly Dictionary<string, string> _aliasToIdMap = new();
    // Maps: original ID -> list of aliases (for tracking what to remove when entity changes/deletes)
    private readonly Dictionary<string, List<string>> _idToAliasesMap = new();
    // Tracks the current synced version
    private ulong _syncedVersion = 0;

    private Task? _syncTask;
    private CancellationTokenSource? _syncCancellationTokenSource;
    private TaskCompletionSource? _initializationComplete;
    private bool _initialized;

    public AliasDatasetReader(
        IDatasetReader<T> sourceReader,
        Func<ReadOnlyMemory<byte>, IEnumerable<string>> aliasFactory,
        string aliasName,
        ILogger logger,
        IHostApplicationLifetime appLifetime,
        IClock? clock = null)
    {
        _sourceReader = sourceReader ?? throw new ArgumentNullException(nameof(sourceReader));
        _aliasFactory = aliasFactory ?? throw new ArgumentNullException(nameof(aliasFactory));
        _aliasName = aliasName ?? throw new ArgumentNullException(nameof(aliasName));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _appLifetime = appLifetime ?? throw new ArgumentNullException(nameof(appLifetime));
        _clock = clock ?? new SystemClock();

        _initializationComplete = new TaskCompletionSource();
    }

    /// <summary>
    /// Initializes the alias reader by loading all entities from the source and starting background sync.
    /// This should be called after construction and typically done via dependency injection.
    /// </summary>
    public void StartInitialization(CancellationToken cancellationToken = default)
    {
        if (_syncTask != null)
            return; // Already initialized

        _syncCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _appLifetime.ApplicationStopping);
        _syncTask = Task.Run(() => InitializeAndSync(_syncCancellationTokenSource.Token), _syncCancellationTokenSource.Token);
    }

    private async Task InitializeAndSync(CancellationToken cancellationToken)
    {
        // Retry loop for initialization - transient failures should not permanently break the reader
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                // Initial load: get all entities from version 0
                _logger.LogInformation("Initializing alias reader for alias name '{AliasName}'", _aliasName);

                var initialChanges = await _sourceReader.GetChangedNames(0);
                foreach (var entityId in initialChanges.Changes.Select(c => c.Name))
                {
                    try
                    {
                        var entity = await _sourceReader.Get(entityId);
                        if (entity.HasValue)
                        {
                            RegisterAliases(entityId, entity.Value.entityBytes);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Failed to load entity '{EntityId}' during initialization", entityId);
                    }
                }

                _initialized = true;
                _initializationComplete?.TrySetResult();
                _logger.LogInformation("Alias reader '{AliasName}' initialized with {AliasCount} aliases", _aliasName, _aliasToIdMap.Count);

                // Update synced version after initial load
                lock (_aliasToIdMap)
                {
                    _syncedVersion = initialChanges.ToVersion;
                }

                // Background sync: continuously watch for changes
                await SyncChangesInBackground(initialChanges.ToVersion, cancellationToken);
                
                // If SyncChangesInBackground returns normally (e.g., due to cancellation), exit the loop
                break;
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Alias reader '{AliasName}' initialization cancelled", _aliasName);
                _initializationComplete?.TrySetCanceled();
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error initializing alias reader '{AliasName}', will retry in 5 seconds", _aliasName);
                
                // Don't set the exception on _initializationComplete - instead, retry
                // This prevents the reader from being permanently stuck
                try
                {
                    await _clock.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    _initializationComplete?.TrySetCanceled();
                    break;
                }
            }
        }
    }

    private async Task SyncChangesInBackground(ulong fromVersion, CancellationToken cancellationToken)
    {
        ulong currentVersion = fromVersion;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var changes = await _sourceReader.WaitChangedNames(currentVersion, cancellationToken);

                // Phase 1: Detect changes for all entities without touching the maps
                var aliasChanges = new List<(string entityId, List<string> toRemove, List<string> toAdd)>();

                foreach (var change in changes.Changes)
                {
                    try
                    {
                        var entity = await _sourceReader.Get(change.Name);
                        if (entity.HasValue)
                        {
                            var newAliases = _aliasFactory(entity.Value.entityBytes)?.ToList() ?? new List<string>();
                            var oldAliases = new List<string>();

                            // Get old aliases if this entity was already tracked
                            lock (_aliasToIdMap)
                            {
                                if (_idToAliasesMap.TryGetValue(change.Name, out var existingAliases))
                                {
                                    oldAliases = new List<string>(existingAliases);
                                }
                            }

                            // Determine what's being removed and what's being added
                            var toRemove = oldAliases.Where(a => !newAliases.Contains(a)).ToList();
                            var toAdd = newAliases.Where(a => !oldAliases.Contains(a)).ToList();

                            if (toRemove.Any() || toAdd.Any() || oldAliases.Count == 0)
                            {
                                aliasChanges.Add((change.Name, toRemove, toAdd));
                                _logger.LogDebug("Detected change for entity '{EntityId}': removing {RemoveCount} aliases, adding {AddCount} aliases",
                                    change.Name, toRemove.Count, toAdd.Count);
                            }
                        }
                        else
                        {
                            // Entity was deleted - remove all its aliases
                            List<string> oldAliases;
                            lock (_aliasToIdMap)
                            {
                                if (_idToAliasesMap.TryGetValue(change.Name, out var existingAliases))
                                {
                                    oldAliases = new List<string>(existingAliases);
                                }
                                else
                                {
                                    oldAliases = new List<string>();
                                }
                            }

                            if (oldAliases.Any())
                            {
                                aliasChanges.Add((change.Name, oldAliases, new List<string>()));
                                _logger.LogDebug("Detected deletion of entity '{EntityId}': removing {RemoveCount} aliases",
                                    change.Name, oldAliases.Count);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Failed to process change for entity '{EntityId}'", change.Name);
                    }
                }

                // Phase 2: Apply all changes atomically in two sub-phases: deletes first, then adds
                lock (_aliasToIdMap)
                {
                    // Sub-phase 2a: Remove all old aliases
                    foreach (var (entityId, toRemove, _) in aliasChanges)
                    {
                        foreach (var alias in toRemove)
                        {
                            _aliasToIdMap.Remove(alias);
                            _logger.LogDebug("Removed alias '{Alias}' from entity '{EntityId}'", alias, entityId);
                        }

                        // Remove the removed aliases from the cached list
                        if (_idToAliasesMap.TryGetValue(entityId, out var cachedAliases))
                        {
                            foreach (var alias in toRemove)
                            {
                                cachedAliases.Remove(alias);
                            }
                        }
                    }

                    // Sub-phase 2b: Add all new aliases and clean up entities with no aliases
                    foreach (var (entityId, toRemove, toAdd) in aliasChanges)
                    {
                        if (toAdd.Any())
                        {
                            // Update the cached aliases for this entity
                            if (_idToAliasesMap.TryGetValue(entityId, out var existing))
                            {
                                existing.AddRange(toAdd);
                            }
                            else
                            {
                                _idToAliasesMap[entityId] = new List<string>(toAdd);
                            }

                            foreach (var alias in toAdd)
                            {
                                _aliasToIdMap[alias] = entityId;
                                _logger.LogDebug("Added alias '{Alias}' -> '{EntityId}'", alias, entityId);
                            }
                        }

                        // Check if entity should be removed from tracking
                        // This happens when it has no aliases remaining (either deleted or updated to have no aliases)
                        if (_idToAliasesMap.TryGetValue(entityId, out var remainingAliases))
                        {
                            if (remainingAliases.Count == 0)
                            {
                                // No aliases left - remove from tracking
                                _idToAliasesMap.Remove(entityId);
                                _logger.LogDebug("Removed entity '{EntityId}' from tracking (no aliases remaining)", entityId);
                            }
                        }
                    }

                    currentVersion = changes.ToVersion;
                    _syncedVersion = currentVersion;
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Background sync for alias reader '{AliasName}' stopped", _aliasName);
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in background sync for alias reader '{AliasName}', will retry", _aliasName);
                try
                {
                    await _clock.Delay(TimeSpan.FromSeconds(5), cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
    }

    private void RegisterAliases(string entityId, ReadOnlyMemory<byte> entityBytes)
    {
        lock (_aliasToIdMap)
        {
            var aliases = _aliasFactory(entityBytes)?.ToList() ?? new List<string>();

            // Store the aliases for this entity
            _idToAliasesMap[entityId] = aliases;

            // Map each alias to the entity ID
            foreach (var alias in aliases)
            {
                _aliasToIdMap[alias] = entityId;
                _logger.LogDebug("Registered alias '{Alias}' -> '{EntityId}'", alias, entityId);
            }
        }
    }

    private void RemoveAliases(string entityId)
    {
        lock (_aliasToIdMap)
        {
            if (_idToAliasesMap.TryGetValue(entityId, out var aliases))
            {
                foreach (var alias in aliases)
                {
                    _aliasToIdMap.Remove(alias);
                    _logger.LogDebug("Removed alias '{Alias}' -> '{EntityId}'", alias, entityId);
                }

                _idToAliasesMap.Remove(entityId);
            }
        }
    }

    private string? ResolveAliasToId(string aliasOrId)
    {
        lock (_aliasToIdMap)
        {
            // Try to resolve it as an alias
            if (_aliasToIdMap.TryGetValue(aliasOrId, out var resolvedId))
                return resolvedId;

            return null;
        }
    }

    private async Task EnsureInitialized(CancellationToken cancellationToken)
    {
        if (_syncTask == null)
            throw new InvalidOperationException("AliasDatasetReader must be started via StartInitialization()");

        if (!_initialized && _initializationComplete != null)
        {
            await _initializationComplete.Task.ConfigureAwait(false);
        }
    }

    public Task<(string name, ReadOnlyMemory<byte> entityBytes)?> Get(string id)
    {
        return GetWithCancellation(id, CancellationToken.None);
    }

    private async Task<(string name, ReadOnlyMemory<byte> entityBytes)?> GetWithCancellation(string id, CancellationToken cancellationToken)
    {
        await EnsureInitialized(cancellationToken);

        var resolvedId = ResolveAliasToId(id);
        if (resolvedId == null)
        {
            _logger.LogDebug("Alias '{Alias}' could not be resolved", id);
            return null;
        }

        _logger.LogDebug("Resolved alias '{Alias}' to ID '{EntityId}'", id, resolvedId);
        return await _sourceReader.Get(resolvedId);
    }

    public Task<ICollection<(string name, ReadOnlyMemory<byte>? entityBytes)>> BatchGet(IEnumerable<string> ids, string? parent = null)
    {
        return BatchGetWithCancellation(ids, parent, CancellationToken.None);
    }

    private async Task<ICollection<(string name, ReadOnlyMemory<byte>? entityBytes)>> BatchGetWithCancellation(IEnumerable<string> ids, string? parent, CancellationToken cancellationToken)
    {
        await EnsureInitialized(cancellationToken);

        var resolvedIds = ids
            .Select(id => ResolveAliasToId(id))
            .Where(id => id != null)
            .Cast<string>()
            .ToList();

        if (!resolvedIds.Any())
            return new List<(string, ReadOnlyMemory<byte>?)>();

        _logger.LogDebug("BatchGet: Resolved {AliasCount} aliases to {IdCount} entity IDs", ids.Count(), resolvedIds.Count);
        return await _sourceReader.BatchGet(resolvedIds, parent);
    }

    public Task<ChangeSet> GetChangedNames(ulong fromVersion)
    {
        return GetChangedNamesWithCancellation(fromVersion, CancellationToken.None);
    }

    private async Task<ChangeSet> GetChangedNamesWithCancellation(ulong fromVersion, CancellationToken cancellationToken)
    {
        await EnsureInitialized(cancellationToken);

        // For alias reader, we return all known aliases when starting from version 0
        lock (_aliasToIdMap)
        {
            var aliasChanges = _aliasToIdMap.Keys
                .Select(a => (Name: a, ChangeType: ChangeType.New))
                .ToList();

            // Return the current synced version, not 0
            return new ChangeSet(_syncedVersion, aliasChanges);
        }
    }

    public Task<ChangeSet> WaitChangedNames(ulong fromVersion, CancellationToken cancellationToken = default)
    {
        return WaitChangedNamesWithCancellation(fromVersion, cancellationToken);
    }

    private async Task<ChangeSet> WaitChangedNamesWithCancellation(ulong fromVersion, CancellationToken cancellationToken)
    {
        await EnsureInitialized(cancellationToken);

        // Delegate to source reader's change notifications
        // The background sync will keep the alias mappings up to date
        var changes = await _sourceReader.WaitChangedNames(fromVersion, cancellationToken);

        // Return empty changes as we're handling sync in background
        // The client should use the main reader for change notifications
        return new ChangeSet(changes.ToVersion, new List<(string Name, ChangeType ChangeType)>());
    }

    public Task<(List<string> Names, ulong LatestVersion)> List(string parent, CancellationToken cancellationToken = default)
    {
        return ListWithCancellation(parent, cancellationToken);
    }

    private async Task<(List<string> Names, ulong LatestVersion)> ListWithCancellation(string parent, CancellationToken cancellationToken)
    {
        await EnsureInitialized(cancellationToken);

        var children = new List<string>();
        lock (_aliasToIdMap)
        {
            // Find all aliases that start with parent/ pattern (including nested descendants)
            var parentPrefix = parent + "/";
            foreach (var alias in _aliasToIdMap.Keys)
            {
                if (alias.StartsWith(parentPrefix, StringComparison.Ordinal))
                {
                    children.Add(alias);
                }
            }

            children.Sort();
            return (children, _syncedVersion);
        }
    }

    public Task<uint> CountChildren(string parent, CancellationToken cancellationToken = default)
    {
        return CountChildrenWithCancellation(parent, cancellationToken);
    }

    private async Task<uint> CountChildrenWithCancellation(string parent, CancellationToken cancellationToken)
    {
        await EnsureInitialized(cancellationToken);

        lock (_aliasToIdMap)
        {
            uint count = 0;
            // Find all aliases that start with parent/ pattern (including nested descendants)
            var parentPrefix = parent + "/";
            foreach (var alias in _aliasToIdMap.Keys)
            {
                if (alias.StartsWith(parentPrefix, StringComparison.Ordinal))
                {
                    count++;
                }
            }

            return count;
        }
    }

    public void Dispose()
    {
        _syncCancellationTokenSource?.Cancel();
        _syncCancellationTokenSource?.Dispose();
        _syncTask?.Dispose();
    }
}
