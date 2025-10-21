# Phase 0: Research & Analysis

**Feature**: Dataset Indexes (Aliases) for Entities  
**Branch**: `004-introduce-the-concept`  
**Date**: October 21, 2025

## Research Summary

This document captures the findings from Phase 0 research to clarify design decisions and resolve unknowns identified in the feature specification.

---

## 1. IDatasetReader Interface Analysis

### Decision
The `IDatasetIndex<TEntity>` interface will expose a subset of `IDatasetReader<TEntity>` methods: `Read()`, `BatchRead()`, and `CountChildren()`, excluding `ListChanges()` and `ListChildren()`.

### Rationale
- **Read()**: Direct entity lookup via alias - primary use case for index
- **BatchRead()**: Efficient multi-entity lookup via alias list - essential for performance
- **CountChildren()**: Prefix-based counting at alias hierarchy levels - enables aggregation queries
- **Excluded ListChanges()**: Index is read-only; subscriptions managed internally via ListChanges stream
- **Excluded ListChildren()**: Alias prefix queries require special handling; CountChildren used instead for parent-level queries

### Research Findings
- IDatasetReader provides: Read(name), BatchRead(names), ListChildren(parentPath), CountChildren(parentPath), ListChanges()
- Method signatures are async (return Task/IAsyncEnumerable)
- Protobuf serialization for entity content
- Change notifications include ChangeType: Created, Modified, Deleted with entity name/content

### Alternatives Considered
1. **Full interface inheritance**: Would include ListChanges (unwanted) and ListChildren (prefix query semantics differ on aliases)
2. **No interface inheritance**: Would require duplication; rejected for maintainability

---

## 2. GrpcReaderServiceImpl Integration Strategy

### Decision
Extend `GrpcReaderServiceImpl` with optional `indexName` parameter on Read(), BatchRead(), CountChildren() methods. When indexName is provided, route to registered index; otherwise use direct reader.

### Rationale
- Maintains backward compatibility (indexName is optional)
- Single extension point for all read operations
- Leverages existing gRPC infrastructure
- Clear routing logic: if indexName exists → resolve from DI and use index; else → use reader directly

### Research Findings
- GrpcReaderServiceImpl constructor receives IDatasetReader instance
- Can extend methods without breaking existing contracts
- gRPC proto generation supports optional fields (nullable in C#)
- DI container available at request handling level (via IServiceProvider)

### Alternatives Considered
1. **Separate gRPC service for indexes**: Adds complexity; rejected for simplicity
2. **Middleware-based routing**: Possible but harder to trace; explicit parameter clearer

---

## 3. Change Feed Integration

### Decision
InMemoryReadableIndex will:
1. **On startup**: Replay entire ListChanges() stream to initialize all alias maps
2. **Ongoing**: Subscribe to ListChanges() stream for real-time delta updates

### Rationale
- Change feed is authoritative source of truth
- Eliminates need for external synchronization
- Handles offline scenarios naturally (backlog replay on reconnect)
- Reactive streams provide natural backpressure

### Research Findings
- ListChanges() returns `IObservable<ChangeNotification>` (Rx stream)
- ChangeNotification includes: ChangeType, EntityName, EntityContent (protobuf)
- Stream handles reconnection/retry automatically
- No built-in backpressure drops; reliable delivery

### Implementation Notes
- Use `Replay()` operator for initialization (replay historical changes)
- Use `Share()` for efficiency (multiple subscribers to same stream)
- Handle disposal properly; index subscription lifecycle tied to host lifecycle

---

## 4. DI Container Integration

### Decision
Use `Microsoft.Extensions.DependencyInjection` extension method pattern. Create static `IndexExtensions` class with `AddDatasetIndex<TEntity>()` method.

### Rationale
- Matches existing RxDatasets patterns (see DatasetExtensions.cs, ConfigExtensions)
- Type-safe generic registration
- Fluent builder pattern for composability
- Single registration point handles all setup

### Research Findings
- Existing codebase uses: `services.AddXxx()` extension methods
- DatasetExtensions.cs provides ConfigExtensions with Configure<T>() pattern
- IServiceProvider available at runtime for index lookup
- Keyed registration not needed; use IIndexProvider or dictionary lookup

### Implementation Notes
- Store indexes in IServiceProvider with unique key (index name string)
- Or use separate IIndexRegistry service registered as singleton
- Prefer explicit registration over automatic discovery

---

## 5. Alias Conflict Resolution

### Decision
**"Last one wins"** approach for this iteration:
- When two entities generate the same alias, the latest entity in the change feed wins
- That alias maps to the latest entity; previous entity's mapping is overwritten
- Deterministic based on change feed order

Future iteration (with writer hooks):
- Detect conflicts during write transaction
- Reject transaction if conflict detected
- Return explicit conflict error to caller

### Rationale
- Simplicity for read-only index (this iteration)
- No transaction machinery needed yet
- Predictable behavior (change feed order is single source of truth)
- Clear upgrade path to transaction-level conflict detection

### Conflict Example
```
1. Entity A generates alias "items/item1/link" → maps to A
2. Entity B also generates alias "items/item1/link" → maps to B (overwrites)
3. Read("items/item1/link") → returns B
4. If B is later deleted → alias removed entirely (doesn't revert to A)
```

### Alternatives Considered
1. **First wins**: Would require tracking alias creation order; less intuitive
2. **Reject duplicates**: Would require conflict detection on every change; complex for read-only
3. **Multiple entity mapping**: Would complicate API (return array); rejected

---

## 6. State Management: Two-Map Model

### Decision
InMemoryReadableIndex maintains two internal maps:
1. `aliasPath → primaryEntityName` (lookup during read)
2. `primaryEntityName → aliasArray[]` (track all aliases for entity)

### Rationale
- Fast O(1) alias resolution during reads
- Fast O(1) all-aliases lookup during updates (to compute diff)
- Enables efficient diff calculation when entity content changes
- Clear separation of concerns (forward/reverse mappings)

### Example Flow
```
Entity: Link l001 with items [item1, item2]
Generated aliases: ["items/item1/links/l001", "items/item2/links/l001", "storeesl/barcode1/link"]

Map 1 (alias → entity):
  "items/item1/links/l001" → "l001"
  "items/item2/links/l001" → "l001"
  "storeesl/barcode1/link" → "l001"

Map 2 (entity → aliases):
  "l001" → ["items/item1/links/l001", "items/item2/links/l001", "storeesl/barcode1/link"]

Update: Link l001 now has items [item2, item3]
New aliases: ["items/item2/links/l001", "items/item3/links/l001", "storeesl/barcode1/link"]

Diff:
  Remove: "items/item1/links/l001"
  Keep: "items/item2/links/l001", "storeesl/barcode1/link"
  Add: "items/item3/links/l001"

State update: Delete old alias from Map 1, add new to Map 1, update Map 2
```

### Alternatives Considered
1. **Single map (alias → entity)**: Can't efficiently diff on update (need previous state)
2. **Cache previous aliases per entity**: Extra storage; two-map model sufficient

---

## 7. Technology Stack Validation

### C# .NET 8.0 ✅
- Mature async/await support
- Generic constraints support for IDatasetIndex<TEntity>
- Dictionary/ConcurrentDictionary for efficient maps

### System.Reactive (Rx) ✅
- Existing dependency in RxDatasets
- IObservable<T> for change stream
- Operators: Replay(), Share(), Subscribe() for stream management

### Microsoft.Extensions.Hosting ✅
- IHostedService base class for initialization if needed
- Dependency injection integration

### Google.Protobuf ✅
- Entity serialization already used
- No changes needed for index

### Grpc.Core.Api ✅
- gRPC service integration
- Proto generation for optional indexName field

---

## 8. Testing Strategy Validation

### Unit Testing (xUnit)
- Index state management tests (maps, diffs)
- Alias resolution tests (read, batch read, count children)
- Conflict handling tests ("last one wins" behavior)
- Change feed processing tests (initialization, delta updates)

### Integration Testing (testhost)
- Real DatasetWriter + InMemoryReadableIndex
- gRPC service with index routing
- End-to-end scenarios (multi-entity, hierarchy queries)

### Key Test Scenarios
1. Entity creation → aliases added
2. Entity update → alias diff correctly computed
3. Entity deletion → all aliases removed
4. Alias collision → last one wins
5. BatchRead via multiple aliases
6. CountChildren at various hierarchy levels

---

## 9. Performance Characteristics

### Expected Performance
- **Read(aliasPath)**: O(1) - hash table lookup + IDatasetReader.Read() call
- **BatchRead(aliasPaths)**: O(n) - n alias lookups + single IDatasetReader.BatchRead() call
- **CountChildren(aliasPrefix)**: O(m) - iterate m aliases matching prefix; aggregate count

### Memory Impact
- Two hash tables per index (minimal for typical entity counts)
- Estimated ~50 bytes per entity for alias mapping (entity name string + alias paths)
- Scales linearly with entity count

### No Identified Bottlenecks
- Hash table operations are constant time
- Change feed subscription efficient (internal Rx machinery)
- No locks needed (initial implementation; single-threaded or ConcurrentDictionary if needed)

---

## 10. Integration Points Summary

| Component | Integration Point | Method | Notes |
|-----------|------------------|--------|-------|
| IDatasetReader | Change stream | Subscribe to ListChanges() | Replay for init, subscribe for deltas |
| DI Container | Extension method | AddDatasetIndex<T>() | Register in startup |
| gRPC Service | Optional parameter | indexName in Read/BatchRead/CountChildren | Route based on parameter |
| Protobuf | Entity content | Deserialize for alias computation | Via mapping function |
| Rx | Stream processing | IObservable<ChangeNotification> | Replay() for initialization |

---

## 11. Known Constraints & Future Work

### This Iteration (Out of Scope)
- Writer integration (no write hooks yet)
- Alias sub-typing (link.v1/itemlink vs link.v1/esllink)
- Change feed notifications for alias changes
- Distributed indexing

### Future Enhancements
- Writer hooks for transaction-level conflict detection
- Alias sub-typing for change feed filtering
- Change notifications that include alias resolution events
- Replicated index across multiple nodes (distributed version)

---

## Conclusion

All NEEDS CLARIFICATION resolved. Design is sound, implementable, and maintains existing architectural patterns. Ready to proceed to Phase 1: Design & Contracts.

**Status**: ✅ **Phase 0 Research Complete**
