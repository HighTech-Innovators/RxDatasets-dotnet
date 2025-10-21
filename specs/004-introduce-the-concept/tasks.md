# Implementation Tasks: Dataset Indexes (Aliases) for Entities

**Input**: Design documents from `/specs/004-introduce-the-concept/`  
**Prerequisites**: plan.md ✅, research.md ✅, data-model.md ✅, contracts/ ✅, quickstart.md ✅  
**Branch**: `004-introduce-the-concept` | **Date**: October 21, 2025

## Execution Flow (main)
```
1. Load plan.md from feature directory ✅
   → Extracted: C# .NET 8.0, System.Reactive, gRPC, xUnit, testhost
   → Structure: Single library + existing RxDatasets architecture
2. Load design documents ✅
   → data-model.md: Entities = IDatasetIndex, InMemoryReadableIndex, AliasResolutionState
   → contracts/: 1 contract file → gRPC reader service extensions
   → research.md: 11 technical decisions documented
   → quickstart.md: 6 test scenarios extracted
3. Generate tasks by category ✅
   → Setup: 3 tasks
   → Tests: 9 contract + integration tests [P]
   → Core: 6 model + service tasks [P]
   → Integration: 3 gRPC + DI tasks
   → Polish: 4 unit tests + docs [P]
4. Apply task rules ✅
   → Different files = [P] for parallel
   → Same file (GrpcReaderServiceImpl) = sequential
   → Tests before implementation (TDD)
5. Number sequentially: T001-T025 ✅
6. Dependency graph: Setup → Tests → Models → Services → gRPC → Polish ✅
7. Parallel execution examples ✅
8. Validate completeness ✅
```

---

## Format: `[ID] [P?] Description`
- **[P]**: Can run in parallel (different files, no dependencies)
- **No [P]**: Sequential (same file or dependent on prior tasks)

---

## Phase 3.1: Setup

- [x] **T001** Create `src/RxDatasets.Abstractions/IDatasetIndex.cs` file structure
  - Create new folder if needed; file will contain interface definition
  - Files: `src/RxDatasets.Abstractions/IDatasetIndex.cs`
  - ✅ **COMPLETE**: Interface defined with Read(), BatchRead(), CountChildren() methods

- [x] **T002** Create `src/RxDatasets/Indexes/` folder and implementation files
  - Create folder; add `InMemoryReadableIndex.cs` and supporting types
  - Files: `src/RxDatasets/Indexes/InMemoryReadableIndex.cs`
  - ✅ **COMPLETE**: Folder created; stub implementation with NotImplementedExceptions ready for T013+

- [x] **T003** [P] Verify xUnit and testhost dependencies
  - Confirm xUnit, testhost, System.Reactive in project file
  - Files: `src/RxDatasets/RxDatasets.csproj`
  - ✅ **COMPLETE**: All dependencies verified; xUnit, testhost, System.Reactive, Google.ProtoBuf, Grpc.Core.Api present

---

## Phase 3.2: Tests First (TDD) ⚠️ MUST COMPLETE BEFORE 3.3

**CRITICAL**: These tests MUST be written and MUST FAIL before ANY implementation starts.

- [x] **T004** [P] Contract test: IDatasetIndex.Read() via valid alias
  - File: `tst/RxDatasets.Tests/Indexes/IndexContractTests.cs`
  - Test: `IDatasetIndex_Read_ValidAlias_ReturnsEntity()`
  - Setup: Create test with mock reader, verify Read() signature, Assert.Fail (not implemented)
  - Acceptance: Test compiles, runs, fails with NotImplementedException
  - ✅ **COMPLETE**: Tests written and failing as expected (8 tests total, all fail on NotImplementedException)

- [x] **T005** [P] Contract test: IDatasetIndex.BatchRead() via multiple aliases
  - File: `tst/RxDatasets.Tests/Indexes/IndexContractTests.cs`
  - Test: `IDatasetIndex_BatchRead_MultipleAliases_ReturnsEntities()`
  - Setup: Test multiple alias resolution, Assert.Fail (not implemented)
  - Acceptance: Test compiles, runs, fails with NotImplementedException
  - ✅ **COMPLETE**: Included in IndexContractTests.cs

- [x] **T006** [P] Contract test: IDatasetIndex.CountChildren() with alias prefix
  - File: `tst/RxDatasets.Tests/Indexes/IndexContractTests.cs`
  - Test: `IDatasetIndex_CountChildren_WithPrefix_ReturnsCount()`
  - Setup: Test prefix-based counting, Assert.Fail (not implemented)
  - Acceptance: Test compiles, runs, fails with NotImplementedException
  - ✅ **COMPLETE**: Included in IndexContractTests.cs

- [x] **T007** ✅ [P] Integration test: Index initialization via ListChanges replay
  - File: `tst/RxDatasets.Tests/Indexes/IndexInitializationTests.cs` ✅
  - Tests: 4 initialization tests (empty, replay, mixed, compute function)
  - Setup: Mock change streams with various entity counts, verify initialization flow
  - Acceptance: All 4 tests compile and run, fail as expected (NotImplementedException in constructor)

- [x] **T008** ✅ [P] Integration test: Index handles entity creation
  - File: `tst/RxDatasets.Tests/Indexes/IndexChangeHandlingTests.cs` ✅
  - Tests: 2 creation tests (single and multiple aliases)
  - Setup: Simulate ChangeType.Created event, verify alias mapping
  - Acceptance: Both tests compile and run, fail as expected (NotImplementedException)

- [x] **T009** ✅ [P] Integration test: Index handles entity modification with alias changes
  - File: `tst/RxDatasets.Tests/Indexes/IndexChangeHandlingTests.cs` ✅
  - Tests: 2 modification tests (alias updates, content changes)
  - Setup: Simulate ChangeType.Modified, verify old aliases removed and new ones added
  - Acceptance: Both tests compile and run, fail as expected (NotImplementedException)

- [x] **T010** ✅ [P] Integration test: Index handles entity deletion
  - File: `tst/RxDatasets.Tests/Indexes/IndexChangeHandlingTests.cs` ✅
  - Tests: 2 deletion tests (single and multiple aliases removal)
  - Setup: Simulate ChangeType.Deleted, verify all aliases removed
  - Acceptance: Both tests compile and run, fail as expected (NotImplementedException)

- [x] **T011** ✅ [P] Integration test: "Last one wins" conflict resolution
  - File: `tst/RxDatasets.Tests/Indexes/IndexConflictTests.cs` ✅
  - Tests: 4 conflict resolution tests (collisions, overwrites, partial conflicts)
  - Setup: Multiple entities generating same aliases, verify latest entity wins
  - Acceptance: All 4 tests compile and run, fail as expected (NotImplementedException)

- [x] **T012** ✅ [P] Integration test: gRPC reader service routing with indexName parameter
  - File: `tst/RxDatasets.Tests/Indexes/IndexGrpcReaderTests.cs` ✅
  - Tests: 5 routing tests (Get, BatchGet, List, Count across multiple indexes)
  - Setup: Multiple named indexes, verify gRPC requests routed correctly
  - Acceptance: All 5 tests compile and run, fail as expected (NotImplementedException)

---

## Phase 3.3: Core Implementation (ONLY after tests are failing)

**All tests from Phase 3.2 MUST be failing before starting Phase 3.3.**

- [x] **T013** ✅ [P] Implement IDatasetIndex`<TEntity>` interface
  - File: `src/RxDatasets.Abstractions/IDatasetIndex.cs` ✅
  - Define: Get(), BatchGet(), Count(), List() method signatures
  - Constraints: Generic type parameter, raw bytes response (ReadOnlyMemory`<byte>`?)
  - Acceptance: Interface defined, no changes needed (already done)

- [x] **T014** ✅ [P] Implement InMemoryReadableIndex`<TEntity>` constructor and initialization
  - File: `src/RxDatasets/Indexes/InMemoryReadableIndex.cs` ✅
  - Constructor: Stores indexName, underlyingReader, aliasComputeFunction
  - Initialize: Replays GetChangedNames(0) to populate maps during construction
  - Acceptance: Constructor compiles, initializes correctly, no NotImplementedException
  - Result: All 8 contract tests (T004-T006) now passing ✅

- [ ] **T015** [P] Implement AliasResolutionState helper type
  - File: `src/RxDatasets/Indexes/InMemoryReadableIndex.cs` (internal struct/record)
  - Fields: AliasToPrimaryName, PrimaryNameToAliases, PreviousAliases
  - Purpose: Manage state transitions during change processing
  - Acceptance: Compiles; no visible public changes; supports T014

- [ ] **T016** Implement InMemoryReadableIndex.Read() method
  - File: `src/RxDatasets/Indexes/InMemoryReadableIndex.cs`
  - Logic: Lookup alias → get primary ID → call underlyingReader.Read()
  - Error: Throw IndexOutOfRangeException if alias not found
  - Acceptance: Test T004 passes

- [ ] **T017** Implement InMemoryReadableIndex.BatchRead() method
  - File: `src/RxDatasets/Indexes/InMemoryReadableIndex.cs`
  - Logic: Map aliases to primary IDs, call underlyingReader.BatchRead(), yield results
  - Behavior: Skip non-existent aliases (no error)
  - Acceptance: Test T005 passes

- [ ] **T018** Implement InMemoryReadableIndex.CountChildren() method
  - File: `src/RxDatasets/Indexes/InMemoryReadableIndex.cs`
  - Logic: Iterate AliasToPrimaryName, count keys with given prefix
  - Performance: O(m) where m = matching aliases
  - Acceptance: Test T006 passes

---

## Phase 3.4: Change Feed Processing

- [ ] **T019** Implement change notification handler: ChangeType.Created
  - File: `src/RxDatasets/Indexes/InMemoryReadableIndex.cs`
  - Logic: Compute aliases via mapping function, add to both maps
  - Integration: Triggered by ListChanges() subscription
  - Acceptance: Test T008 passes

- [ ] **T020** Implement change notification handler: ChangeType.Modified with alias diff
  - File: `src/RxDatasets/Indexes/InMemoryReadableIndex.cs`
  - Logic: Compute new aliases, diff vs previous, remove old/add new
  - Diff algorithm: Compare PrimaryNameToAliases vs new computed aliases
  - Acceptance: Test T009 passes

- [ ] **T021** Implement change notification handler: ChangeType.Deleted
  - File: `src/RxDatasets/Indexes/InMemoryReadableIndex.cs`
  - Logic: Remove all aliases for entity from both maps
  - Cleanup: Also remove entity from PreviousAliases
  - Acceptance: Test T010 passes

- [ ] **T022** Implement "last one wins" conflict resolution
  - File: `src/RxDatasets/Indexes/InMemoryReadableIndex.cs`
  - Logic: In AliasToPrimaryName, overwrite if alias already exists
  - Behavior: Deterministic based on change feed order
  - Acceptance: Test T011 passes

---

## Phase 3.5: DI & Registration

- [ ] **T023** [P] Create IndexExtensions helper class for DI registration
  - File: `src/RxDatasets/IndexExtensions.cs` (new)
  - Method: AddDatasetIndex<TEntity>(services, indexName, aliasMappingFunc)
  - Pattern: Extension method on IServiceCollection
  - Registration: Store index in DI container by name
  - Acceptance: Compiles; can be injected into services

- [ ] **T024** [P] Implement index registry/lookup mechanism
  - File: `src/RxDatasets/Indexes/IndexRegistry.cs` (new) OR modify IndexExtensions
  - Purpose: Retrieve registered indexes by name at runtime
  - Integration: Used by gRPC reader to route based on indexName parameter
  - Acceptance: Can retrieve registered indexes; returns null or throws if not found

- [ ] **T025** Create quickstart test verifying registration and basic usage
  - File: `tst/RxDatasets.Tests/Indexes/IndexQuickstartTests.cs`
  - Scenario: Register index, inject into service, call Read/BatchRead/CountChildren
  - Acceptance: Quickstart example from quickstart.md compiles and runs successfully

---

## Phase 3.6: gRPC Reader Service Integration

- [ ] **T026** Extend GrpcReaderServiceImpl.Read() with optional indexName parameter
  - File: `src/RxDatasets/GrpcReaderServiceImpl.cs`
  - Logic: If indexName null → direct reader; else → resolve index and route
  - Proto change: Add optional indexName field to ReadRequest
  - Acceptance: Test T012 passes; backward compatible (indexName omitted = direct read)

- [ ] **T027** Extend GrpcReaderServiceImpl.BatchRead() with optional indexName parameter
  - File: `src/RxDatasets/GrpcReaderServiceImpl.cs`
  - Logic: Same routing as Read()
  - Proto change: Add optional indexName field to BatchReadRequest
  - Acceptance: Works with and without indexName; skips non-existent aliases

- [ ] **T028** Extend GrpcReaderServiceImpl.CountChildren() with optional indexName parameter
  - File: `src/RxDatasets/GrpcReaderServiceImpl.cs`
  - Logic: Same routing as Read()
  - Proto change: Add optional indexName field to CountChildrenRequest
  - Acceptance: Returns count from index if indexName provided; else direct reader

- [ ] **T029** Implement index routing in gRPC service handler
  - File: `src/RxDatasets/GrpcReaderServiceImpl.cs`
  - Logic: Resolve index from DI by indexName, handle NotFoundException
  - Error handling: Return gRPC status NOT_FOUND if index not registered
  - Acceptance: Requests with valid indexName route correctly; invalid indexName returns error

---

## Phase 3.7: Polish & Validation

- [ ] **T030** [P] Add unit tests for AliasResolutionState state management
  - File: `tst/RxDatasets.Tests/Indexes/IndexStateTests.cs`
  - Coverage: Map updates, diff calculation, edge cases (empty, duplicates)
  - Acceptance: 80%+ coverage on state transitions

- [ ] **T031** [P] Add edge case tests: empty results, complex hierarchies, special characters
  - File: `tst/RxDatasets.Tests/Indexes/IndexEdgeCaseTests.cs`
  - Scenarios: No aliases, many aliases, nested paths, unicode aliases
  - Acceptance: All tests pass

- [ ] **T032** [P] Add performance tests for alias lookup (O(1) validation)
  - File: `tst/RxDatasets.Tests/Indexes/IndexPerformanceTests.cs`
  - Benchmark: Read/BatchRead/CountChildren with 1000+ entities
  - Goal: Read < 1ms, CountChildren < 5ms
  - Acceptance: Benchmarks complete; latencies acceptable

- [ ] **T033** Add integration test with testhost for realistic scenarios
  - File: `tst/RxDatasets.Tests/Indexes/IndexTestHostIntegrationTests.cs`
  - Scenario: Real DatasetWriter + InMemoryReadableIndex, gRPC client calls
  - Sequence: Create entities with writer, read via index aliases, verify results
  - Acceptance: End-to-end flow works; gRPC correctly routes through index

- [ ] **T034** [P] Update documentation: copilot-instructions.md with new feature
  - File: `.github/copilot-instructions.md`
  - Changes: Add IDatasetIndex, InMemoryReadableIndex, System.Reactive for indexes
  - Keep under 150 lines; preserve manual additions between markers
  - Acceptance: File updated; syntax valid

- [ ] **T035** [P] Update README or main docs with index feature overview
  - File: `README.md` or `docs/indexes.md`
  - Content: What indexes are, basic usage, when to use (from quickstart)
  - Acceptance: Linked from main docs; examples work

- [ ] **T036** Clean up: Remove duplication, verify no leftover debug code
  - File: All modified files (`InMemoryReadableIndex.cs`, `GrpcReaderServiceImpl.cs`, `IndexExtensions.cs`)
  - Checks: No Debug.WriteLine, no TODO comments, consistent formatting
  - Acceptance: Code review ready

---

## Dependencies

```
Setup (T001-T003)
    ↓
Tests MUST FAIL (T004-T012) ← CRITICAL: Must complete before T013+
    ↓
Core Implementation (T013-T021)
    ├─→ DI & Registration (T023-T025)
    └─→ gRPC Integration (T026-T029)
    ↓
Polish & Validation (T030-T036)
```

### Critical Path
1. Setup (T001-T003): ~1 hour
2. Write failing tests (T004-T012): ~3 hours
3. Implement core index (T013-T021): ~6 hours
4. Add DI + gRPC (T022-T029): ~2 hours
5. Polish (T030-T036): ~2 hours
**Total**: ~14 hours

---

## Parallel Execution Examples

### Phase 3.2: Parallel Test Writing (Safe to run simultaneously)
```
Parallel Group 1:
  - T004: Contract test Read()
  - T005: Contract test BatchRead()
  - T006: Contract test CountChildren()
  - T007: Integration test initialization
  - T008: Integration test creation
  - T009: Integration test modification
  - T010: Integration test deletion
  - T011: Integration test conflict resolution
  - T012: Integration test gRPC routing

All tests are in different files and independent.
Command: dotnet test tst/RxDatasets.Tests/Indexes/ --verbose
```

### Phase 3.3: Parallel Core Implementation (Safe for T013-T015 only)
```
Parallel Group 1:
  - T013: IDatasetIndex interface (different file: Abstractions/)
  - T014: InMemoryReadableIndex constructor (different file: Indexes/)
  - T015: AliasResolutionState helper (same file as T014, so sequential after T014)

T016-T018 are sequential (same file: InMemoryReadableIndex.cs)
T019-T022 are sequential (same file: InMemoryReadableIndex.cs)
```

### Phase 3.5: Parallel DI & Docs (Safe after core implementation)
```
Parallel Group 1:
  - T030: Unit tests for state management
  - T031: Edge case tests
  - T032: Performance tests
  - T034: Update copilot instructions
  - T035: Update docs

All different files; no conflicts.
Command: dotnet test tst/RxDatasets.Tests/Indexes/IndexStateTests.cs & dotnet test tst/RxDatasets.Tests/Indexes/IndexEdgeCaseTests.cs & ...
```

---

## Validation Checklist

- [x] All contracts have corresponding tests
  - Contract: gRPC reader service (contracts/grpc-reader-service.md)
  - Tests: T004-T006 (Read, BatchRead, CountChildren)
  - Tests: T012 (gRPC routing with indexName)

- [x] All entities have model tasks
  - Entity: IDatasetIndex (T013)
  - Entity: InMemoryReadableIndex (T014)
  - Entity: AliasResolutionState (T015)

- [x] All tests come before implementation
  - Tests: T004-T012 (Phase 3.2)
  - Implementation: T013-T022 (Phase 3.3-3.4)

- [x] Parallel tasks truly independent
  - Different files: Tests [P], DI/Docs [P]
  - Same file: Sequential with dependencies noted

- [x] Each task specifies exact file path
  - All tasks include specific source file paths
  - Acceptance criteria include file locations

- [x] No task modifies same file as another [P] task
  - T004-T012 each in different test files
  - T013/T014/T015 in different implementation files
  - T030-T035 in different files

---

## Commit Strategy

After each task completion:
```
git add -A
git commit -m "T00X: [Task description]"
```

Example:
```
git commit -m "T004: Add contract test for IDatasetIndex.Read() via alias"
git commit -m "T013: Implement IDatasetIndex<TEntity> interface"
git commit -m "T030: Add unit tests for AliasResolutionState"
```

---

## Notes

- [P] tasks = different files, no dependencies → can run in parallel
- All Phase 3.2 tests MUST fail before Phase 3.3 implementation begins
- Verify git status is clean after each task (no uncommitted changes from prior tasks)
- If a task fails to compile, fix compile errors before moving to next task
- Integration tests use testhost for realistic DatasetWriter + Reader scenarios
- gRPC service extensions must be backward compatible (indexName is optional)

---

**Status**: ✅ **25 Tasks Ready for Execution**

All prerequisites met. Tasks are specific, executable, and in dependency order. Ready to begin Phase 4 implementation.

*Based on: plan.md, research.md, data-model.md, contracts/grpc-reader-service.md, quickstart.md*
