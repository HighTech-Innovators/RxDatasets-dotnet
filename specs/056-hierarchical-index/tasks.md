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

- [ ] T001 Create feature branch `056-hierarchical-index` (already done)
- [ ] T002 Update `DatasetEntities.proto` with `SnapshotNode`, `DeltaNode`, and `root` fields in `src/RxDatasets/Models/DatasetEntities.proto`
- [ ] T003 [P] Regenerate Protobuf C# classes

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

- [ ] T004 [P] Create `ImmutableIndexNode` class in `src/RxDatasets/Models/ImmutableIndexNode.cs`
- [ ] T005 [P] Implement `ImmutableIndexNode` methods (Add, Remove, Get) with immutable semantics in `src/RxDatasets/Models/ImmutableIndexNode.cs`
- [ ] T006 Create unit tests for `ImmutableIndexNode` in `tst/RxDatasets.Tests/Models/ImmutableIndexNodeTests.cs`

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Hierarchical In-Memory Index (Priority: P1) 🎯 MVP

**Goal**: Replace the flat in-memory index with the hierarchical structure to reduce memory usage.

**Independent Test**: Load a large dataset with path-like IDs and verify reduced memory usage.

### Tests for User Story 1

- [ ] T007 [P] [US1] Create benchmark test for memory usage in `tst/RxDatasets.Tests/Benchmarks/IndexMemoryBenchmark.cs`

### Implementation for User Story 1

- [ ] T008 [US1] Update `ImmutableDatasetState` to use `ImmutableIndexNode` instead of flat dictionary in `src/RxDatasets/Models/ImmutableDatasetState.cs`
- [ ] T009 [US1] Update `DatasetReader` to build `ImmutableIndexNode` from flat entities (temporary adapter) in `src/RxDatasets/DatasetReader.cs`
- [ ] T010 [US1] Update `DatasetWriter` to use `ImmutableIndexNode` for state tracking in `src/RxDatasets/DatasetWriter.cs`
- [ ] T011 [US1] Verify all existing tests pass with the new in-memory structure

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 - Hierarchical Disk Format (Priority: P1)

**Goal**: Persist the hierarchical index to disk using the new Protobuf structure (Snapshot and Delta).

**Independent Test**: Generate a snapshot and delta, then verify the file structure uses `SnapshotNode` and `DeltaNode`.

### Tests for User Story 2

- [ ] T012 [P] [US2] Create test to verify snapshot and delta file structure in `tst/RxDatasets.Tests/Storage/SnapshotFormatTests.cs`

### Implementation for User Story 2

- [ ] T013 [US2] Implement `ToSnapshotNode` and `ToDeltaNode` conversion logic in `src/RxDatasets/DatasetExtensions.cs`
- [ ] T014 [US2] Update `DatasetWriter` to populate `root` field in `SnapshotIndex` and `DeltaIndex` when saving in `src/RxDatasets/DatasetWriter.cs`
- [ ] T015 [US2] Update `DatasetReader` to prefer reading `root` field if present (for both snapshots and deltas) in `src/RxDatasets/DatasetReader.cs`

**Checkpoint**: At this point, User Story 2 should be fully functional and testable independently

---

## Phase 5: User Story 3 - Backward Compatibility & Migration (Priority: P1)

**Goal**: Ensure legacy files can be read and delta files can be migrated.

**Independent Test**: Load legacy files and verify data access; run migrator on delta files.

### Tests for User Story 3

- [ ] T016 [P] [US3] Create integration test with legacy snapshot file in `tst/RxDatasets.Tests/Compatibility/LegacySnapshotTests.cs`
- [ ] T017 [P] [US3] Create integration test for delta migration in `tst/RxDatasets.Tests/Compatibility/DeltaMigrationTests.cs`

### Implementation for User Story 3

- [ ] T018 [US3] Implement legacy flat-to-hierarchical conversion logic in `DatasetReader` (if not fully covered in T009) in `src/RxDatasets/DatasetReader.cs`
- [ ] T019 [US3] Implement `MigrateDeltaIndex` method in `DataMigrator` to convert flat deltas to hierarchical in `src/RxDatasets/DataMigrator.cs`
- [ ] T020 [US3] Update `DataMigrator` to iterate and migrate all delta files in a dataset in `src/RxDatasets/DataMigrator.cs`

**Checkpoint**: At this point, User Story 3 should be fully functional and testable independently

---

## Phase 6: User Story 4 - Efficient Delta Application (Priority: P2)

**Goal**: Optimize delta application using the hierarchical structure.

**Independent Test**: Measure time to apply deltas to a specific sub-tree.

### Tests for User Story 4

- [ ] T021 [P] [US4] Create benchmark test for delta application in `tst/RxDatasets.Tests/Benchmarks/DeltaApplicationBenchmark.cs`

### Implementation for User Story 4

- [ ] T022 [US4] Optimize `ApplyDelta` in `ImmutableDatasetState` to use tree traversal in `src/RxDatasets/Models/ImmutableDatasetState.cs`
- [ ] T023 [US4] Ensure thread safety and atomicity of delta application in `src/RxDatasets/Models/ImmutableDatasetState.cs`

**Checkpoint**: At this point, User Story 4 should be fully functional and testable independently

---

## Final Phase: Polish & Cross-Cutting Concerns

**Purpose**: Final cleanup, documentation, and non-functional requirements

- [ ] T024 Update documentation (README, etc.) with new index details
- [ ] T025 [P] Verify performance against goals (>50% memory reduction)
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
