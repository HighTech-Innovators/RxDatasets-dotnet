# Feature Specification: Dataset Indexes (Aliases) for Entities

**Feature Branch**: `004-introduce-the-concept`  
**Created**: October 21, 2025  
**Status**: Draft  
**Input**: User description: Introduce the concept of Indexes (aliases) for our entities with IDatasetIndex interface for readable access and in-memory indexing based on entity aliases

## Execution Flow (main)

```
1. Parse user description from Input
   → Identify: create IDatasetIndex interface, implement in-memory indexing, integrate with gRPC reader
2. Extract key concepts from description
   → Actors: Index service, Reader, gRPC service
   → Actions: read via alias, create index, maintain in-memory state
   → Data: entity aliases, index mappings
   → Constraints: aliases are derived from entity content, no writer integration yet
3. For each unclear aspect:
   → Clarified through braindump and requirements
4. Fill User Scenarios & Testing section
   → Index initialization, alias-based reads, alias updates
5. Generate Functional Requirements
   → Each requirement is testable and specific
6. Identify Key Entities
   → Index, Alias, Entity reference
7. Run Review Checklist
   → No ambiguities, clear scope, focused on reader operations
8. Return: SUCCESS (spec ready for planning)
```

---

## ⚡ Quick Guidelines

- ✅ Focus on WHAT users need and WHY: Simplified entity access via meaningful aliases
- ❌ Avoid HOW to implement: Implementation details deferred to planning phase
- 👥 Oriented toward developer stakeholders working with indexed datasets

---

## User Scenarios & Testing *(mandatory)*

### Primary User Story
As a client service, I want to access entities in a dataset using meaningful alias names (in addition to their primary IDs), so that I can simplify queries and support flexible identity resolution patterns. For example, I can reference a link entity via multiple paths: the primary link ID, or via the item IDs it contains.

**Example Flow**:

1. A Link entity is stored at path `t/ttt/s/sss/links/l001` with content `{barcode: barcode1, items: [item1, item2]}`
2. Index is configured with a mapping function that generates aliases: `[t/ttt/s/sss/storeesl/barcode1/link, t/ttt/s/sss/items/item1/links/l001, t/ttt/s/sss/items/item2/links/l001]`
3. Client can now call `Read(t/ttt/s/sss/items/item1/links/l001)` via the reader and get the Link entity
4. Client can call `ListChildren(t/ttt/s/sss/items/item1/links)` and discover all links associated with item1

### Acceptance Scenarios
1. **Given** an Index is registered with a mapping function, **When** an entity is created/updated in the dataset, **Then** the Index maintains current alias mappings for that entity
2. **Given** a client reads an entity by alias path via the gRPC reader, **When** the alias resolves to a valid entity, **Then** the actual entity data is returned as if the primary ID was used
3. **Given** an entity's content changes (affecting its aliases), **When** the Index receives the change notification, **Then** the Index updates its in-memory state to reflect old and new aliases
4. **Given** a query for children at an alias path, **When** the Index resolves the alias, **Then** all entities with that alias in the set are returned
5. **Given** a delete request using an alias path, **When** the delete affects an alias, **Then** the behavior matches the primary ID deletion semantics

### Edge Cases

- What happens when an alias maps to multiple different entities? In this iteration, 'last one wins' — the latest entity wins the alias. When writer hooks are implemented in future iterations, conflicting alias assignments will be detected and the transaction will be aborted instead
- What happens when an entity's aliases change? The Index tracks previous aliases and current aliases, updating its in-memory maps accordingly
- What happens when an alias is removed (entity content changes such that an alias is no longer generated)? The Index removes it from its maps
- What happens if an alias temporarily conflicts with another alias during a transaction? In this iteration, 'last one wins' — the latest entity wins the alias. When writer hooks are implemented in future iterations, conflicting alias assignments will be detected and the transaction will be aborted instead

---

## Requirements *(mandatory)*

### Functional Requirements

#### Core Interface & Abstraction

- **FR-001**: System MUST provide an `IDatasetIndex<TEntity>` interface that inherits read capabilities from `IDatasetReader<TEntity>` (specifically: `Read()`, `ListChildren()`, `CountChildren()` methods)
- **FR-002**: System MUST NOT include `ListChanges()` in the Index read interface (ListChanges is not inherited/exposed from IDatasetIndex)
- **FR-003**: System MUST support registration of Index instances with the Dependency Injection container via a helper class

#### In-Memory Index Implementation

- **FR-004**: System MUST provide an `InMemoryReadableIndex<TEntity>` implementation that accepts a `Func<string name, TEntity entity, string[]>` delegate to compute aliases for any entity
- **FR-005**: System MUST use the underlying `IDatasetReader` and its `ListChanges()` stream to initialize the Index on startup and stay up-to-date with entity changes
- **FR-006**: System MUST maintain in-memory maps of current and previous aliases for all entities to detect when aliases are added, removed, or changed
- **FR-007**: System MUST handle write operations by immediately returning `Task.CompletedTask` (write operations are a no-op in this iteration)
- **FR-008**: When reading via an alias, the Index MUST resolve the alias to its actual entity name/ID and then perform a batch read on the underlying Reader

#### Integration with gRPC Reader Service

- **FR-009**: System MUST extend the gRPC reader service with additional methods to support reading from indexes
- **FR-010**: System MUST include the index name/identifier in gRPC read/delete requests so the service knows which index (if any) to use for resolution
- **FR-011**: The gRPC service MUST support operations like `Read(indexName, aliasPath)`, `ListChildren(indexName, aliasPath)`, and `CountChildren(indexName, aliasPath)`

#### DI and Service Setup

- **FR-012**: System MUST provide a static helper class for creating and registering indexes with DI, accepting the index name and alias-mapping function
- **FR-013**: DI registration MUST instantiate the Index with the underlying Reader and the mapping function
- **FR-014**: The helper MUST support multiple Index instances for different entity types or different aliasing strategies

#### Change Tracking & State Management

- **FR-015**: System MUST correctly handle scenarios where entity content changes:
  - If content changes that affects alias generation, update internal maps
  - Track which aliases were previously pointing to this entity and which aliases point to it now
  - Support diff-based updates to the in-memory state
- **FR-016**: System MUST support entity deletion: when an entity is deleted, remove all its aliases from the in-memory maps
- **FR-017**: System MUST support new entity creation: when an entity is created, compute its aliases and add them to the in-memory maps

#### Scope Exclusions (Future Work)

- **FR-018**: Writer operations for indexes are not implemented in this iteration
- **FR-019**: Alias sub-typing (e.g., `link.v1/itemlink` vs `link.v1/esllink`) is not addressed in this iteration
- **FR-020**: Change feed notifications for alias changes are not included in this iteration

### Key Entities *(include if feature involves data)*

- **Index**: An in-memory data structure that maps alias paths to entity primary IDs, initialized and maintained from a dataset's change feed
  - Properties: Index name, underlying Reader, alias mapping function, internal maps (alias → primary ID, primary ID → aliases)
  - Relationships: Associated with one entity type and one IDatasetReader instance

- **Alias**: A secondary path/name that resolves to an entity's primary ID
  - Properties: Alias path (string), resolved entity primary ID, associated entity type
  - Lifecycle: Created when entity is added/updated, updated when entity content changes, removed when entity is deleted or alias is no longer generated

- **Entity Reference**: The primary identifier and content of an entity within the dataset
  - Properties: Primary ID/name (string), content (TEntity), computed aliases (string[])
  - Relationships: One entity may have multiple aliases generated by the mapping function

---

## Review & Acceptance Checklist
*GATE: Automated checks run during main() execution*

### Content Quality
- [x] No implementation details (languages, frameworks, APIs) — kept to architectural level
- [x] Focused on user value and business needs — simplified alias-based access
- [x] Written for developer stakeholders
- [x] All mandatory sections completed

### Requirement Completeness
- [x] No [NEEDS CLARIFICATION] markers remain
- [x] Requirements are testable and unambiguous  
- [x] Success criteria are measurable
- [x] Scope is clearly bounded (explicitly excludes writer integration, subtypes, change feed notifications)
- [x] Dependencies and assumptions identified (depends on existing IDatasetReader, ListChanges stream)

### Feature Scope
- [x] Clearly limited to reader operations in this iteration
- [x] In-memory index implementation is primary deliverable
- [x] DI integration and gRPC reader extension are supporting deliverables
- [x] No writer/change-feed/subtyping work in scope

---

## Execution Status
*Updated by main() during processing*

- [x] User description parsed
- [x] Key concepts extracted
- [x] Ambiguities resolved
- [x] User scenarios defined
- [x] Requirements generated
- [x] Entities identified
- [x] Review checklist passed
- [x] Spec ready for planning phase

---

## Supporting Details

### Example: Link with Multi-Item Alias

**Primary Entity**:

```
Name: t/ttt/s/sss/links/l001
Content: { "barcode": "barcode1", "items": ["item1", "item2"] }
```

**Generated Aliases** (via mapping function):

```
- t/ttt/s/sss/storeesl/barcode1/link (store-ESL link)
- t/ttt/s/sss/items/item1/links/l001 (item1's links)
- t/ttt/s/sss/items/item2/links/l001 (item2's links)
```

**Usage**:

- `index.Read("t/ttt/s/sss/items/item1/links/l001")` → returns the Link entity
- `index.ListChildren("t/ttt/s/sss/items/item1/links")` → returns all links with "item1" in their items array
- `index.CountChildren("t/ttt/s/sss/storeesl/barcode1")` → returns count of entities with that barcode

### Lifecycle Example: Alias Update

1. **Initial State**: Link `l001` has items `[item1, item2]` → aliases include `items/item1/links/l001`, `items/item2/links/l001`
2. **Change**: Link content updated to items `[item2, item3]` (item1 removed, item3 added)
3. **Index Processing**:
   - Previous aliases: `[items/item1/links/l001, items/item2/links/l001]`
   - New aliases: `[items/item2/links/l001, items/item3/links/l001]`
   - Diff: Remove `items/item1/links/l001`, Keep `items/item2/links/l001`, Add `items/item3/links/l001`
4. **Result**: Index is updated; `items/item1/links/l001` no longer resolves to `l001`
