# Tasks: Hierarchical Index for Memory Optimization

**Input**: Design documents from `/specs/056-hierarchical-index/`
**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Tests are included as per the "Test-First" constitution principle.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Library**: `src/RxDatasets/`
- **Tests**: `tst/RxDatasets.Tests/`

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [x] T001 Create feature branch `056-hierarchical-index` (already done)
- [x] T002 Update `DatasetEntities.proto` with `SnapshotNode`, `DeltaNode`, and `root` fields in `src/RxDatasets/Models/DatasetEntities.proto`
- [x] T003 [P] Regenerate Protobuf C# classes

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

- [x] T004 [P] Create `ImmutableIndexNode` class in `src/RxDatasets/Models/ImmutableIndexNode.cs`
- [x] T005 [P] Implement `ImmutableIndexNode` methods (Add, Remove, Get) with immutable semantics in `src/RxDatasets/Models/ImmutableIndexNode.cs`
- [x] T006 Create unit tests for `ImmutableIndexNode` in `tst/RxDatasets.Tests/Models/ImmutableIndexNodeTests.cs`

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Hierarchical In-Memory Index (Priority: P1) 🎯 MVP

**Goal**: Replace the flat in-memory index with the hierarchical structure to reduce memory usage.

**Independent Test**: Load a large dataset with path-like IDs and verify reduced memory usage.

### Tests for User Story 1

- [x] T007 [P] [US1] Create benchmark test for memory usage in `tst/RxDatasets.Tests/Benchmarks/IndexMemoryBenchmark.cs`

### Implementation for User Story 1

- [x] T008 [US1] Update `ImmutableDatasetState` to use `ImmutableIndexNode` instead of flat dictionary in `src/RxDatasets/Models/ImmutableDatasetState.cs`
- [x] T009 [US1] Update `DatasetReader` to build `ImmutableIndexNode` from flat entities (temporary adapter) in `src/RxDatasets/DatasetReader.cs`
- [x] T010 [US1] Update `DatasetWriter` to use `ImmutableIndexNode` for state tracking in `src/RxDatasets/DatasetWriter.cs`
- [x] T011 [US1] Verify all existing tests pass with the new in-memory structure

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 - Hierarchical Disk Format (Priority: P1)

**Goal**: Persist the hierarchical index to disk using the new Protobuf structure (Snapshot and Delta).

**Independent Test**: Generate a snapshot and delta, then verify the file structure uses `SnapshotNode` and `DeltaNode`.

### Tests for User Story 2

- [x] T012 [P] [US2] Create test to verify snapshot and delta file structure in `tst/RxDatasets.Tests/Storage/SnapshotFormatTests.cs`

### Implementation for User Story 2

- [x] T013 [US2] Implement `ToSnapshotNode` and `ToDeltaNode` conversion logic in `src/RxDatasets/DatasetExtensions.cs`
- [x] T014 [US2] Update `DatasetWriter` to populate `root` field in `SnapshotIndex` and `DeltaIndex` when saving in `src/RxDatasets/DatasetWriter.cs`
- [x] T015 [US2] Update `DatasetReader` to prefer reading `root` field if present (for both snapshots and deltas) in `src/RxDatasets/DatasetReader.cs`

**Checkpoint**: At this point, User Story 2 should be fully functional and testable independently

---

## Phase 5: User Story 3 - Backward Compatibility & Migration (Priority: P1)

**Goal**: Ensure legacy files can be read and delta files can be migrated.

**Independent Test**: Load legacy files and verify data access; run migrator on delta files.

### Tests for User Story 3

- [x] T016 [P] [US3] Create integration test with legacy snapshot file in `tst/RxDatasets.Tests/Compatibility/LegacySnapshotTests.cs`
- [x] T017 [P] [US3] Create integration test for delta migration in `tst/RxDatasets.Tests/Compatibility/DeltaMigrationTests.cs`

### Implementation for User Story 3

- [x] T018 [US3] Implement legacy flat-to-hierarchical conversion logic in `DatasetReader` (if not fully covered in T009) in `src/RxDatasets/DatasetReader.cs`
- [x] T019 [US3] Implement `MigrateDeltaIndex` method in `DataMigrator` to convert flat deltas to hierarchical in `src/RxDatasets/DataMigrator.cs`
- [x] T020 [US3] Update `DataMigrator` to iterate and migrate all delta files in a dataset in `src/RxDatasets/DataMigrator.cs`

**Checkpoint**: At this point, User Story 3 should be fully functional and testable independently

---

## Phase 6: User Story 4 - Efficient Hierarchical Query Lookups (Priority: P2)

**Goal**: Optimize entity lookups using the hierarchical structure for parent path queries instead of string matching.

**Current Issue**: `DatasetReader.IterateHistory()` uses inefficient `StartsWith()` comparisons on all entities (O(n) per parent path query). Should instead split path once and walk the tree structure with dictionary lookups (O(depth)).

**Optimization Technique**: Use **array slicing (alloc-free)** when recursing from the top through the hierarchy. This approach:
- Splits parent path once: `var pathParts = parent.Split('/', StringSplitOptions.RemoveEmptyEntries);`
- Passes slices of the array as recursion parameters: `Navigate(rootNode, pathParts, 0)` where `0` is the current index
- Avoids allocating new arrays for each recursion level (alloc-free)
- Makes counting children and subtree operations efficient
- Apply throughout: DatasetReader queries, DatasetWriter updates, history traversal, and any parent path operations

**Independent Test**: Benchmark parent path queries with tree traversal vs. StartsWith string matching on a large hierarchical dataset.

### Tests for User Story 4

- [x] T021 [P] [US4] Create benchmark test for hierarchical query performance in `tst/RxDatasets.Tests/Benchmarks/HierarchicalQueryBenchmark.cs`
  - Compare StartsWith approach (current) vs tree-walk approach with array slicing (optimized)
  - Test with parent paths at various depths (3, 5, 10 levels deep)
  - Measure entity enumeration speed
  - Verify alloc-free approach reduces GC pressure

### Implementation for User Story 4

- [x] T022 [US4] Optimize `DatasetReader.IterateHistory()` to use tree traversal with array slicing in `src/RxDatasets/DatasetReader.cs` (lines 573, 581, 589)
  - Split parent path once: `var pathParts = parent.Split('/', StringSplitOptions.RemoveEmptyEntries);`
  - Create helper method: `Navigate(ImmutableIndexNode node, string[] pathParts, int index)` that passes array slice
  - Walk tree: `Navigate(rootNode, pathParts, 0)` → recursively navigate with `index + 1`
  - At target level (`index == pathParts.Length`), enumerate `node.ChangedEntities.Values`
  - Expected improvement: O(n) string comparisons → O(depth) dictionary lookups + zero allocations
- [x] T023 [US4] Apply array slicing technique to all parent path operations in `src/RxDatasets/DatasetReader.cs`
  - `GetHistory()`: Use tree-walk with array slicing
  - `ListEntities()`: Use tree-walk with array slicing
  - Child counting operations: Use efficient tree traversal (makes counting very efficient)
  - Ensure consistent use of alloc-free array slicing throughout reader and any similar operations in writer and history APIs

**Checkpoint**: At this point, User Story 4 should be fully functional and testable independently

---

## Final Phase: Polish & Cross-Cutting Concerns

**Purpose**: Final cleanup, documentation, and non-functional requirements

- [~] T024 Update documentation (DtoScope tool documentation complete, general feature docs in progress)
- [ ] T025 [P] Verify performance against goals (>50% memory reduction, hierarchical queries O(depth))
- [ ] T026 [P] Ensure all tests pass (unit, integration, benchmarks)
- [ ] T027 Clean up any temporary code or TODOs

## Dependencies

1. **Phase 1 & 2** (Setup/Foundational) must be completed first.
2. **Phase 3** (US1) establishes the in-memory structure required by all other stories.
3. **Phase 4** (US2) depends on US1 for the source data structure.
4. **Phase 5** (US3) depends on US1 (for reading into memory) and US2 (for writing migrated files).
5. **Phase 6** (US4) is an optimization that can be done after functional correctness is established.

## Parallel Execution Examples

- **US1**: T007 (Benchmark) can run parallel to T008-T010 (Implementation).
- **US2**: T012 (Test) can run parallel to T013-T015 (Implementation).
- **US3**: T016/T017 (Tests) can run parallel to T018-T020 (Implementation).

## Implementation Strategy

1. **Foundation**: Build the `ImmutableIndexNode` and ensure it behaves correctly as a standalone unit.
2. **MVP (US1)**: Switch the in-memory state to use the new node structure. This immediately delivers the memory benefit, even if we still read/write flat files (converting on the fly).
3. **Persistence (US2)**: Update the disk format to match the memory structure, optimizing I/O.
4. **Compatibility (US3)**: Ensure we didn't break old files and add the explicit migration tool.
5. **Optimization (US4)**: Refine the delta application logic for maximum speed.
