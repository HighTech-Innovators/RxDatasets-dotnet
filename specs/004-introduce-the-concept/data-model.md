# Phase 1: Data Model & Entities

**Feature**: Dataset Indexes (Aliases) for Entities  
**Branch**: `004-introduce-the-concept`  
**Date**: October 21, 2025

## Data Model Overview

This document defines the data model for Dataset Indexes, including entities, interfaces, and state management structures.

---

## 1. IDatasetIndex<TEntity> Interface

### Location
- **Namespace**: `RxDatasets.Abstractions`
- **File**: `IDatasetIndex.cs`
- **Scope**: Public interface for library consumers

### Definition
```csharp
public interface IDatasetIndex<TEntity> where TEntity : class
{
    /// <summary>
    /// Read a single entity by its alias path.
    /// </summary>
    /// <param name="aliasPath">The alias path to resolve (e.g., "items/item1/links/l001")</param>
    /// <returns>The entity data if alias resolves successfully</returns>
    /// <exception cref="IndexOutOfRangeException">Alias does not exist</exception>
    Task<TEntity> Read(string aliasPath);

    /// <summary>
    /// Read multiple entities by their alias paths in a single batch operation.
    /// </summary>
    /// <param name="aliasPaths">Collection of alias paths to resolve</param>
    /// <returns>Async enumerable of entities in corresponding order</returns>
    /// <remarks>
    /// Non-existent aliases are skipped (not included in results).
    /// Results may be fewer than requested paths if some aliases don't resolve.
    /// </remarks>
    IAsyncEnumerable<TEntity> BatchRead(IEnumerable<string> aliasPaths);

    /// <summary>
    /// Count entities that have an alias with the given prefix.
    /// </summary>
    /// <param name="aliasPrefix">The prefix to match (e.g., "items/item1/links")</param>
    /// <returns>Count of entities with aliases matching the prefix</returns>
    Task<long> CountChildren(string aliasPrefix);
}
```

### Inheritance Notes
- **Does NOT inherit from IDatasetReader<TEntity>** (explicit interface separation)
- **Excludes ListChanges()** (index is read-only; manages subscriptions internally)
- **Excludes ListChildren()** (use CountChildren for prefix-based queries)
- **Methods are identical** to IDatasetReader equivalents for Read and BatchRead
- **CountChildren** is new; will be added to IDatasetReader in future iteration

---

## 2. InMemoryReadableIndex<TEntity>

### Location
- **Namespace**: `RxDatasets.Indexes`
- **File**: `InMemoryReadableIndex.cs`
- **Scope**: Implementation of IDatasetIndex<TEntity>

### Constructor
```csharp
public InMemoryReadableIndex(
    string indexName,
    IDatasetReader<TEntity> underlyingReader,
    Func<string name, TEntity entity, string[]> aliasComputeFunction)
```

### Parameters
- **indexName**: Unique identifier for this index (used in DI registration and gRPC routing)
- **underlyingReader**: The IDatasetReader to delegate actual entity reads to
- **aliasComputeFunction**: Function that takes (entityName, entityContent) and returns array of alias paths

### Example Usage
```csharp
var index = new InMemoryReadableIndex<LinkEntity>(
    indexName: "links_by_item",
    underlyingReader: reader,
    aliasComputeFunction: (name, link) => new[]
    {
        $"items/{link.Item1}/links/{name}",
        $"items/{link.Item2}/links/{name}",
        $"storeesl/{link.Barcode}/link"
    }
);
```

### Lifecycle
1. **Construction**: Minimal initialization (no state changes)
2. **Startup**: On first use or explicit Initialize(), replay ListChanges() stream to populate maps
3. **Running**: Subscribe to ListChanges() for real-time updates
4. **Disposal**: Unsubscribe from streams, clear maps

---

## 3. Internal State Management

### AliasResolutionState (Internal Structure)

```csharp
internal record AliasResolutionState
{
    // Alias path → Primary entity name mapping
    // Used for O(1) lookup during Read/BatchRead
    public Dictionary<string, string> AliasToPrimaryName { get; init; }

    // Primary entity name → Array of aliases
    // Used for O(1) diff calculation during entity updates
    public Dictionary<string, string[]> PrimaryNameToAliases { get; init; }

    // Previous aliases for each entity (used during updates to compute diff)
    public Dictionary<string, string[]> PreviousAliases { get; init; }
}
```

### State Access Pattern
- Initialized empty on construction
- Populated during initialization phase (replay)
- Updated incrementally as changes arrive from ListChanges() stream
- Never exposed to consumers (internal only)

---

## 4. Change Processing Logic

### Initialization Phase
```
Input: Empty index state
Action: Replay ListChanges() stream from beginning (historical dump)
For each ChangeNotification:
  - If ChangeType.Created: Compute aliases → add to both maps
  - If ChangeType.Modified: Compute aliases → diff old vs new → update maps
  - If ChangeType.Deleted: Remove all aliases from maps
Output: Index fully populated with all current entities/aliases
```

### Ongoing Phase
```
Input: Index state from initialization
Action: Subscribe to ListChanges() stream (continue from where we left off)
For each ChangeNotification:
  - If ChangeType.Created: Compute aliases → add to both maps
  - If ChangeType.Modified: 
    1. Compute new aliases
    2. Diff against previous aliases stored in PrimaryNameToAliases
    3. Remove old aliases not in new set
    4. Add new aliases not in old set
  - If ChangeType.Deleted: Remove all aliases from maps
Output: Index stays synchronized with underlying data
```

### Diff Algorithm Example
```
Entity: link1 with items [item1, item2]
Previous aliases: ["items/item1/links/link1", "items/item2/links/link1", "storeesl/barcode1/link"]

Update: link1 now has items [item2, item3]
New aliases: ["items/item2/links/link1", "items/item3/links/link1", "storeesl/barcode1/link"]

Diff:
  Remove: ["items/item1/links/link1"]  (in previous, not in new)
  Keep: ["items/item2/links/link1", "storeesl/barcode1/link"]  (in both)
  Add: ["items/item3/links/link1"]  (in new, not in previous)

State Update:
  1. Delete "items/item1/links/link1" from AliasToPrimaryName
  2. Add "items/item3/links/link1" → "link1" to AliasToPrimaryName
  3. Update PrimaryNameToAliases["link1"] = new alias array
```

---

## 5. Conflict Handling: "Last One Wins"

### Scenario
Two entities generate the same alias path.

### Resolution
The entity that appears LAST in the change feed wins ownership of that alias. Previous entity's mapping is overwritten.

### Example
```
Timeline:
1. Time T1: Entity A created → generates alias "items/item1/link"
   State: "items/item1/link" → A

2. Time T2: Entity B created → generates alias "items/item1/link"
   State: "items/item1/link" → B (overwrites A's mapping)

3. Query Read("items/item1/link") → returns B

4. Time T3: Entity B deleted
   State: "items/item1/link" removed entirely (does NOT revert to A)
```

### Implementation
- Simple overwrite in AliasToPrimaryName dictionary
- No special conflict detection logic needed
- Order determined by change feed order (deterministic)

### Future Enhancement
When writer hooks implemented, transaction-level conflict detection will:
- Detect when new aliases would collide with existing ones
- Return explicit ConflictException
- Abort transaction instead of overwriting

---

## 6. Read Operations Implementation

### Read(aliasPath)
```
Input: alias path (string)
1. Lookup alias in AliasToPrimaryName → get primary entity name
2. If not found → throw IndexOutOfRangeException
3. Call underlyingReader.Read(primaryEntityName)
4. Return entity data
```

### BatchRead(aliasPaths)
```
Input: collection of alias paths
1. For each alias path:
   - Lookup in AliasToPrimaryName → get primary entity name
   - If found, add to batch set
2. Call underlyingReader.BatchRead(primaryEntityNames)
3. Yield results as IAsyncEnumerable<TEntity>
```

### CountChildren(aliasPrefix)
```
Input: alias prefix (e.g., "items/item1/links")
1. Iterate all keys in AliasToPrimaryName
2. Count keys that start with aliasPrefix
3. Return count
```

---

## 7. Public API Surface

### Methods (on InMemoryReadableIndex)

| Method | Signature | Returns | Exceptions |
|--------|-----------|---------|-----------|
| Read | `Task<TEntity> Read(string aliasPath)` | Single entity | IndexOutOfRangeException if not found |
| BatchRead | `IAsyncEnumerable<TEntity> BatchRead(IEnumerable<string> aliasPaths)` | Stream of entities | None (skips non-existent aliases) |
| CountChildren | `Task<long> CountChildren(string aliasPrefix)` | Count | None (returns 0 if no matches) |

### No Public Methods for State Inspection (Initial Version)
- Internal maps are read-only for consumers
- Future version could expose GetAliases(entityName) for debugging

---

## 8. Thread Safety

### Initial Implementation
- Use Dictionary (not thread-safe)
- Assume single-threaded or proper synchronization at caller level

### Future Enhancement (if needed)
- Use ConcurrentDictionary for thread-safety
- Add ReaderWriterLockSlim if high contention

---

## 9. Memory Model

### Per-Entity Overhead
- Alias paths stored once (string references)
- Two dictionary entries per entity
- Estimated ~50-100 bytes per entity depending on alias count

### Example: 1,000 entities with 3 aliases each
```
Dict 1 (alias → entity): 3,000 entries, ~120KB
Dict 2 (entity → aliases): 1,000 entries, ~50KB
Total: ~170KB (negligible for most applications)
```

---

## 10. No Breaking Changes

### Existing APIs Unaffected
- IDatasetReader unchanged
- gRPC reader service extended (new optional parameter)
- No modifications to existing data structures

### Backward Compatibility
- All existing code continues to work unchanged
- Index is opt-in (must explicitly register and use)

---

## Summary

**New Entities**:
- `IDatasetIndex<TEntity>` - Public interface
- `InMemoryReadableIndex<TEntity>` - Core implementation
- `AliasResolutionState` - Internal state management

**No Modified Entities**:
- IDatasetReader left unchanged
- Entity models unchanged
- Storage unchanged

**Data Flow**:
1. Initialization: Replay → build maps
2. Runtime: Change notifications → update maps
3. Queries: Alias lookup → delegate to reader

**Status**: ✅ **Phase 1 Data Model Complete**
