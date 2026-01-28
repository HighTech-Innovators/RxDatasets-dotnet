using RxDatasets.Abstractions;

namespace RxDatasets.Abstractions;

/// <summary>
/// Represents a change to an entity in the dataset.
/// </summary>
public class DetailedChange
{
    /// <summary>
    /// The hierarchical identifier of the entity that changed.
    /// Format: kind/id/kind/id/... (e.g., "store/12/label/14" or "org/123/child/item5/details")
    /// </summary>
    public string EntityId { get; set; } = "";

    /// <summary>
    /// The type of change: NEW (created), CHANGED (modified), or DELETED (removed).
    /// </summary>
    public ChangeType ChangeType { get; set; }
}

/// <summary>
/// Provides write operations (PUT, DELETE) for managing entities in a reactive dataset.
/// 
/// Entity Naming Convention:
/// Entity names (ids) must follow a hierarchical format: kind/id/kind/id/...
/// Examples: "store/12/label/14", "org/123/child/item5", "store/12/label/14/details"
/// This hierarchical structure supports hierarchical queries and operations.
/// </summary>
public interface IDatasetWriter<T>
{
    /// <summary>
    /// Response from a write operation containing version information and change details.
    /// 
    /// Fields:
    /// - NewDatasetVersion: The dataset version after this operation completes
    /// - NumAddedEntities: Count of newly created entities
    /// - NumUpdatedEntities: Count of entities that were modified (content changed)
    /// - NumDeletedEntities: Count of entities that were removed
    /// - DetailedChanges: Optional list of individual entity changes (only populated if includeDetailedChanges=true)
    /// </summary>
    record struct WriteResponse(
        ulong NewDatasetVersion, 
        uint NumAddedEntities, 
        uint NumUpdatedEntities, 
        uint NumDeletedEntities,
        IReadOnlyList<DetailedChange>? DetailedChanges = null);
    
    /// <summary>
    /// Deletes a single entity by name.
    /// </summary>
    /// <param name="name">Hierarchical entity name (e.g., "store/12/label/14")</param>
    /// <param name="includeDetailedChanges">If true, populates DetailedChanges field in response with the deleted entity</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>WriteResponse with version and change counts</returns>
    /// <remarks>
    /// - Deleting a non-existent entity is treated as a no-op (not an error)
    /// - The deleted entity appears in DetailedChanges with ChangeType.Deleted if requested
    /// </remarks>
    Task<WriteResponse> Delete(string name, bool includeDetailedChanges = false, CancellationToken ct = default);

    /// <summary>
    /// Deletes a single entity by name.
    /// </summary>
    /// <param name="name">Hierarchical entity name (e.g., "store/12/label/14")</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>WriteResponse with version and change counts</returns>
    /// <remarks>
    /// This method is deprecated. Use Delete(string, bool, CancellationToken) instead.
    /// Kept for backward compatibility.
    /// </remarks>
    [Obsolete("Use Delete(string, bool, CancellationToken) instead.", false)]
    Task<WriteResponse> Delete(string name, CancellationToken ct) => Delete(name, false, ct);

    /// <summary>
    /// Deletes multiple entities by name in a single operation.
    /// </summary>
    /// <param name="names">Async enumerable of hierarchical entity names to delete</param>
    /// <param name="includeDetailedChanges">If true, populates DetailedChanges field with all deleted entities</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>WriteResponse with version and change counts</returns>
    /// <remarks>
    /// - Non-existent entities are silently ignored
    /// - All changes are applied transactionally as a single delta
    /// - Only actually-deleted entities appear in DetailedChanges
    /// </remarks>
    Task<WriteResponse> DeleteMany(IAsyncEnumerable<string> names, bool includeDetailedChanges = false, CancellationToken ct = default);

    /// <summary>
    /// Deletes multiple entities by name in a single operation.
    /// </summary>
    /// <param name="names">Async enumerable of hierarchical entity names to delete</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>WriteResponse with version and change counts</returns>
    /// <remarks>
    /// This method is deprecated. Use DeleteMany(IAsyncEnumerable&lt;string&gt;, bool, CancellationToken) instead.
    /// Kept for backward compatibility.
    /// </remarks>
    [Obsolete("Use DeleteMany(IAsyncEnumerable<string>, bool, CancellationToken) instead.", false)]
    Task<WriteResponse> DeleteMany(IAsyncEnumerable<string> names, CancellationToken ct) => DeleteMany(names, false, ct);

    /// <summary>
    /// Deletes all entities under a hierarchical ancestor path.
    /// </summary>
    /// <param name="ancestor">Ancestor path with even number of path elements (e.g., "org/123" or "store/12/branch/5")</param>
    /// <param name="includeDetailedChanges">If true, populates DetailedChanges field with all deleted descendants</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>WriteResponse with version and change counts</returns>
    /// <remarks>
    /// - Ancestor must have an even number of path elements (kind/id pairs)
    /// - Example: "org/123" deletes all entities starting with "org/123/"
    /// - Example: "store/12/branch/5" deletes all entities under that branch
    /// - All descendant deletions are applied transactionally
    /// </remarks>
    Task<WriteResponse> DeleteChildren(string ancestor, bool includeDetailedChanges = false, CancellationToken ct = default);

    /// <summary>
    /// Deletes all entities under a hierarchical ancestor path.
    /// </summary>
    /// <param name="ancestor">Ancestor path with even number of path elements (e.g., "org/123" or "store/12/branch/5")</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>WriteResponse with version and change counts</returns>
    /// <remarks>
    /// This method is deprecated. Use DeleteChildren(string, bool, CancellationToken) instead.
    /// Kept for backward compatibility.
    /// </remarks>
    [Obsolete("Use DeleteChildren(string, bool, CancellationToken) instead.", false)]
    Task<WriteResponse> DeleteChildren(string ancestor, CancellationToken ct) => DeleteChildren(ancestor, false, ct);

    /// <summary>
    /// Puts (creates or updates) a single entity.
    /// </summary>
    /// <param name="name">Hierarchical entity name (e.g., "store/12/label/14")</param>
    /// <param name="entityBytes">Entity content as byte array</param>
    /// <param name="includeDetailedChanges">If true, populates DetailedChanges field with the created/updated entity</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>WriteResponse with version and change counts</returns>
    /// <remarks>
    /// - If entity doesn't exist, it's created (ChangeType.New)
    /// - If entity exists with different content, it's updated (ChangeType.Changed)
    /// - If entity exists with identical content, no change is recorded
    /// - Change detection uses CRC32 hash comparison for efficiency
    /// </remarks>
    Task<WriteResponse> Put(string name, byte[] entityBytes, bool includeDetailedChanges = false, CancellationToken ct = default);

    /// <summary>
    /// Puts (creates or updates) a single entity.
    /// </summary>
    /// <param name="name">Hierarchical entity name (e.g., "store/12/label/14")</param>
    /// <param name="entityBytes">Entity content as byte array</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>WriteResponse with version and change counts</returns>
    /// <remarks>
    /// This method is deprecated. Use Put(string, byte[], bool, CancellationToken) instead.
    /// Kept for backward compatibility.
    /// </remarks>
    [Obsolete("Use Put(string, byte[], bool, CancellationToken) instead.", false)]
    Task<WriteResponse> Put(string name, byte[] entityBytes, CancellationToken ct) => Put(name, entityBytes, false, ct);

    /// <summary>
    /// Puts (creates or updates) multiple entities and performs implicit deletes for all entities NOT in the new set.
    /// This replaces the entire dataset contents with the provided entities.
    /// </summary>
    /// <param name="entities">Async enumerable of (name, bytes) tuples for the complete new dataset</param>
    /// <param name="includeDetailedChanges">If true, populates DetailedChanges field with all created, updated, and deleted entities</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>WriteResponse with version and change counts</returns>
    /// <remarks>
    /// - All entities currently in the dataset but NOT in the provided set are deleted (implicit delete)
    /// - Net effect consolidation: If an entity is put multiple times, only the final state matters
    /// - All changes are applied transactionally as a single delta
    /// - Performance: Scales with dataset size and number of changes
    /// </remarks>
    Task<WriteResponse> PutAll(IAsyncEnumerable<(string name, byte[] entityBytes)> entities, bool includeDetailedChanges = false, CancellationToken ct = default);

    /// <summary>
    /// Puts (creates or updates) multiple entities and performs implicit deletes for all entities NOT in the new set.
    /// This replaces the entire dataset contents with the provided entities.
    /// </summary>
    /// <param name="entities">Async enumerable of (name, bytes) tuples for the complete new dataset</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>WriteResponse with version and change counts</returns>
    /// <remarks>
    /// This method is deprecated. Use PutAll(IAsyncEnumerable&lt;(string, byte[])&gt;, bool, CancellationToken) instead.
    /// Kept for backward compatibility.
    /// </remarks>
    [Obsolete("Use PutAll(IAsyncEnumerable<(string, byte[])>, bool, CancellationToken) instead.", false)]
    Task<WriteResponse> PutAll(IAsyncEnumerable<(string name, byte[] entityBytes)> entities, CancellationToken ct) => PutAll(entities, false, ct);

    /// <summary>
    /// Puts (creates or updates) multiple entities WITHOUT implicit deletes.
    /// Unchanged entities remain in the dataset.
    /// </summary>
    /// <param name="entities">Async enumerable of (name, bytes) tuples to add or update</param>
    /// <param name="includeDetailedChanges">If true, populates DetailedChanges field with all created and updated entities</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>WriteResponse with version and change counts</returns>
    /// <remarks>
    /// - No implicit deletes; any entity not mentioned is left unchanged
    /// - Entities with identical content to existing versions are not counted as changes
    /// - Net effect consolidation: If an entity is put multiple times, only the final state matters
    /// - All changes are applied transactionally as a single delta
    /// </remarks>
    Task<WriteResponse> PutMany(IAsyncEnumerable<(string name, byte[] entityBytes)> entities, bool includeDetailedChanges = false, CancellationToken ct = default);

    /// <summary>
    /// Puts (creates or updates) multiple entities WITHOUT implicit deletes.
    /// Unchanged entities remain in the dataset.
    /// </summary>
    /// <param name="entities">Async enumerable of (name, bytes) tuples to add or update</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>WriteResponse with version and change counts</returns>
    /// <remarks>
    /// This method is deprecated. Use PutMany(IAsyncEnumerable&lt;(string, byte[])&gt;, bool, CancellationToken) instead.
    /// Kept for backward compatibility.
    /// </remarks>
    [Obsolete("Use PutMany(IAsyncEnumerable<(string, byte[])>, bool, CancellationToken) instead.", false)]
    Task<WriteResponse> PutMany(IAsyncEnumerable<(string name, byte[] entityBytes)> entities, CancellationToken ct) => PutMany(entities, false, ct);

    /// <summary>
    /// Puts (creates or updates) multiple entities under a hierarchical ancestor and performs implicit deletes for descendants NOT in the new set.
    /// </summary>
    /// <param name="entities">Async enumerable of (name, bytes) tuples for new/updated children</param>
    /// <param name="includeDetailedChanges">If true, populates DetailedChanges field with all created, updated, and deleted children</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>WriteResponse with version and change counts</returns>
    /// <remarks>
    /// - The common parent is extracted from entity names (determined by the shortest common ancestor)
    /// - All existing children under that ancestor are deleted, except those in the provided set
    /// - Net effect consolidation: If a child is put multiple times, only the final state matters
    /// - All changes are applied transactionally as a single delta
    /// - Example: If entities are "org/123/child/a" and "org/123/child/b", ancestor "org/123" is implicit
    /// </remarks>
    Task<WriteResponse> PutAllChildren(IAsyncEnumerable<(string name, byte[] entityBytes)> entities, bool includeDetailedChanges = false, CancellationToken ct = default);

    /// <summary>
    /// Puts (creates or updates) multiple entities under a hierarchical ancestor and performs implicit deletes for descendants NOT in the new set.
    /// </summary>
    /// <param name="entities">Async enumerable of (name, bytes) tuples for new/updated children</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>WriteResponse with version and change counts</returns>
    /// <remarks>
    /// This method is deprecated. Use PutAllChildren(IAsyncEnumerable&lt;(string, byte[])&gt;, bool, CancellationToken) instead.
    /// Kept for backward compatibility.
    /// </remarks>
    [Obsolete("Use PutAllChildren(IAsyncEnumerable<(string, byte[])>, bool, CancellationToken) instead.", false)]
    Task<WriteResponse> PutAllChildren(IAsyncEnumerable<(string name, byte[] entityBytes)> entities, CancellationToken ct) => PutAllChildren(entities, false, ct);

    /// <summary>
    /// Performs a mix of put and delete operations in a single transactional delta.
    /// Enables explicit deletion of individual entities alongside creates/updates in one operation.
    /// </summary>
    /// <param name="changes">Async enumerable of (name, bytes?) tuples where null bytes indicates deletion</param>
    /// <param name="includeDetailedChanges">If true, populates DetailedChanges field with all created, updated, and deleted entities</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>WriteResponse with version and change counts</returns>
    /// <remarks>
    /// - If entityBytes is non-null: create or update the entity
    /// - If entityBytes is null: delete the entity
    /// - Deleting a non-existent entity is silently ignored
    /// - Putting an entity with identical content to existing version is ignored (not a change)
    /// - Net effect consolidation (Spec 002): If an entity is modified multiple times in one operation, only final state matters
    ///   Example: Put entity A, then delete A, then put A again → entity A is recorded as NEW (net effect)
    /// - No implicit deletes are performed (unlike PutAll/PutAllChildren)
    /// - All changes are applied transactionally as a single delta
    /// </remarks>
    Task<WriteResponse> PutOrDeleteMany(IAsyncEnumerable<(string name, byte[]? entityBytes)> changes, bool includeDetailedChanges = false, CancellationToken ct = default);

    /// <summary>
    /// Performs a mix of put and delete operations in a single transactional delta.
    /// Enables explicit deletion of individual entities alongside creates/updates in one operation.
    /// </summary>
    /// <param name="changes">Async enumerable of (name, bytes?) tuples where null bytes indicates deletion</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>WriteResponse with version and change counts</returns>
    /// <remarks>
    /// This method is deprecated. Use PutOrDeleteMany(IAsyncEnumerable&lt;(string, byte[]?)&gt;, bool, CancellationToken) instead.
    /// Kept for backward compatibility.
    /// </remarks>
    [Obsolete("Use PutOrDeleteMany(IAsyncEnumerable<(string, byte[]?)>, bool, CancellationToken) instead.", false)]
    Task<WriteResponse> PutOrDeleteMany(IAsyncEnumerable<(string name, byte[]? entityBytes)> changes, CancellationToken ct) => PutOrDeleteMany(changes, false, ct);
}
