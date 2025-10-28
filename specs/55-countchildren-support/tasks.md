# Tasks: CountChildren Support for ReaderService

**Input**: Design documents from `/specs/55-countchildren-support/`
**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Test tasks are included below - write tests FIRST and verify they FAIL before implementation (TDD approach).

**Organization**: Tasks are grouped by user story (US1, US2) to enable independent implementation and testing of each story.

**Implementation Scope**: 
- Add method to IDatasetReader interface
- Implement in InMemoryDataset and DatasetReader (2 implementations only)
- Add gRPC wrapper in GrpcReaderServiceImpl
- NO changes to storage providers - List() provides sufficient data

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2)
- Include exact file paths in descriptions

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and proto contract setup

- [x] T001 Create CountChildren proto messages in `testapps/DTOs/proto/hightechinnovators/dtos/type/reader_service_types.proto` (CountChildrenRequest with string parent field, CountChildrenResponse with uint32 count field)
- [x] T002 [P] Add CountChildren RPC method to `testapps/DTOs/proto/hightechinnovators/dtos/demodto/v1/reader_service.proto` service definition
- [x] T003 [P] Add CountChildren RPC method to `testapps/DTOs/proto/hightechinnovators/dtos/testdto/v1/reader_service.proto` service definition
- [x] T004 Regenerate C# classes from proto files using Grpc.Tools (run `dotnet build` on affected projects)

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core interface extension that MUST be complete before user story implementation

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [x] T005 Add `CountChildren` method signature to `src/RxDatasets.Abstractions/IDatasetReader.cs` with signature: `Task<uint> CountChildren(string parent, CancellationToken cancellationToken = default);`

**Checkpoint**: IDatasetReader interface extended - user story implementation can now begin in parallel

---

## Phase 3a: User Story 1a - DatasetReader & gRPC Implementation (Priority: P1) 🎯 MVP Foundation

**Goal**: Implement core CountChildren functionality in DatasetReader (main implementation) and GrpcReaderServiceImpl (gRPC wrapper). This is the critical path for MVP.

**Independent Test**: Can be fully tested by calling the CountChildren API with a valid parent path and verifying it returns the correct count as a uint32 value.

### Tests for User Story 1a (TDD - write these FIRST, they should FAIL)

**Location**: `tst/RxDatasets.Tests/DataReaderCountChildrenTests.cs`

- [x] T006 [P] [US1a] Write unit test for DatasetReader.CountChildren returning correct count for parent with children
- [x] T007 [P] [US1a] Write unit test for DatasetReader.CountChildren returning 0 for parent with no children
- [x] T008 [P] [US1a] Write unit test for DatasetReader.CountChildren returning 0 for non-existent parent
- [x] T009 [P] [US1a] Write unit test for DatasetReader.CountChildren handling cancellation token

### Implementation for User Story 1a

- [x] T010 [US1a] Implement `CountChildren` method in `src/RxDatasets/DatasetReader.cs` by leveraging existing `List()` method and counting returned names
- [x] T011 [US1a] Implement `CountChildren` method in `src/RxDatasets/GrpcReaderServiceImpl.cs` that calls IDatasetReader.CountChildren() and wraps result in CountChildrenResponse protobuf message
- [x] T012 [US1a] Add error handling in GrpcReaderServiceImpl.CountChildren() for malformed parent paths - return RpcException(InvalidArgument)
- [x] T013 [US1a] Add cancellation token support in GrpcReaderServiceImpl.CountChildren() using existing CombiTokenSource pattern
- [x] T014 [US1a] Add logging to CountChildren method in GrpcReaderServiceImpl for debugging

**Checkpoint**: DatasetReader + GrpcReaderServiceImpl complete - critical path tested and working

---

## Phase 3b: User Story 1b - InMemoryDataset Implementation (Priority: P1) 🎯 MVP Completion

**Goal**: Implement CountChildren in InMemoryDataset to complete the second IDatasetReader implementation.

**Independent Test**: Can be tested separately in dedicated InMemoryDataset test project.

**Note**: Phase 3b can run in parallel with Phase 4 (gRPC User Story 2) after Phase 2 completes.

### Tests for User Story 1b (TDD - write these FIRST, they should FAIL)

**Location**: `tst/RxDatasets.StorageProvider.InMemoryProvider/` (or dedicated InMemory test project if it exists)

- [x] T015 [P] [US1b] Write unit test for InMemoryDataset.CountChildren returning correct count for parent with children
- [x] T016 [P] [US1b] Write unit test for InMemoryDataset.CountChildren returning 0 for parent with no children
- [x] T017 [P] [US1b] Write unit test for InMemoryDataset.CountChildren returning 0 for non-existent parent
- [x] T018 [P] [US1b] Write unit test for InMemoryDataset.CountChildren handling cancellation token

### Implementation for User Story 1b

- [x] T019 [US1b] Implement `CountChildren` method in `src/RxDatasets.InMemoryDataset/InMemoryDataset.cs` by leveraging existing `List()` method and counting returned names

**Checkpoint**: User Story 1 (both 1a and 1b) complete - all IDatasetReader implementations have CountChildren ✅

---

## Phase 4: User Story 2 - gRPC Integration in TestHost (Priority: P2)

**Goal**: Expose CountChildren functionality through gRPC ReaderService in testHost applications for end-to-end testing and demonstration.

**Independent Test**: Can be tested by calling the gRPC CountChildren method through the testHost and verifying it returns the same results as direct API.

**Note**: Phase 4 can run in parallel with Phase 3b (InMemoryDataset implementation) after Phase 3a completes.

### Tests for User Story 2 (TDD - write these FIRST, they should FAIL)

**Location**: `tst/RxDatasets.StorageTests/E2eTestsUsingTestHost.cs`

- [x] T020 [P] [US2] Write integration test for gRPC CountChildren with testHost
- [x] T021 [P] [US2] Write integration test for concurrent gRPC CountChildren calls through testHost
- [x] T022 [P] [US2] Write integration test for gRPC CountChildren with Minio storage provider

### Implementation for User Story 2

- [x] T023 [US2] Verify gRPC service automatically exposes CountChildren through existing ReaderService implementation (gRPC proto regeneration in T004 should handle this)
- [x] T024 [US2] Test that testHost applications properly register and expose the CountChildren gRPC method by running integration tests
- [x] T025 [US2] Verify proto regeneration includes CountChildren method for all test DTOs (demodto, testdto)
- [x] T026 [US2] Run end-to-end test: Start testHost and verify gRPC client can successfully call CountChildren

**Checkpoint**: User Stories 1 AND 2 complete - gRPC integration works end-to-end ✅

---

## Phase 5: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories and overall quality

- [ ] T027 [P] Run all existing unit tests to ensure no regression: `dotnet test tst/RxDatasets.Tests/`
- [ ] T028 [P] Run all existing integration tests to ensure no regression: `dotnet test tst/RxDatasets.StorageTests/`
- [ ] T029 Write documentation for CountChildren API in quickstart.md with usage examples
- [ ] T030 Update README.md to mention CountChildren feature availability
- [ ] T031 Performance validation: Verify CountChildren completes in <100ms for 10,000 children datasets
- [ ] T032 Code review: Ensure consistent style and patterns with rest of RxDatasets codebase
- [ ] T033 [P] Validate edge cases: special characters in paths, unicode, very long paths, malformed input
- [ ] T034 Final validation: Run complete test suite to ensure all functionality works together

---

## Dependencies & Execution Order

### Phase Dependencies

```
Phase 1: Setup (Proto contracts)
    ↓
Phase 2: Foundational (IDatasetReader interface)
    ↓
    ├─→ Phase 3a: User Story 1a (DatasetReader + gRPC) [CRITICAL PATH]
    │   ↓
    │   ├─→ Phase 3b: User Story 1b (InMemoryDataset) [PARALLEL with Phase 4]
    │   └─→ Phase 4: User Story 2 (gRPC testHost integration) [PARALLEL with Phase 3b]
    ↓
Phase 5: Polish & Cross-Cutting Concerns
```

### Critical Path (MVP First Approach)

1. **Setup (T001-T004)**: Proto contracts - ~45 min
2. **Foundational (T005)**: IDatasetReader interface - ~15 min
3. **User Story 1a (T006-T014)**: DatasetReader + GrpcReaderServiceImpl - ~1.5-2 hours ⭐ **CRITICAL PATH**
4. **Validate**: Run tests and verify gRPC works
5. **User Story 1b (T015-T019)** OR **Phase 4 (T020-T026)**: Both can run in parallel - ~1.5-2 hours each
6. **Polish (T027-T034)**: Final testing - ~1 hour

### User Story Parallelization

- **Phase 3a (DatasetReader + gRPC)** must complete first - this is the critical path for MVP
- **Once Phase 3a is complete AND Phase 2 complete**:
  - Phase 3b (InMemoryDataset) can run in parallel with Phase 4 (gRPC testHost)
  - Different developers can work independently

### Parallel Opportunities

#### Phase 1 Parallelization

- T002, T003 can run in parallel (different proto files)
- T001 must complete before T002/T003

#### Phase 3a (User Story 1a) Parallelization

- T006, T007, T008, T009 can run in parallel (different test methods, same test file)
- T010, T011, T012, T013, T014 must run sequentially (dependencies on implementation)

#### Phase 3b (User Story 1b) Parallelization

- T015, T016, T017, T018 can run in parallel (different test methods)
- T019 must run after tests (implementation)

#### Phase 4 (User Story 2) Parallelization

- T020, T021, T022 can run in parallel (different test scenarios)
- T023-T026 mostly sequential (verification/testing)

#### Phase 5 Parallelization

- T027, T028, T033 can run in parallel (different test suites)

---

## Parallel Execution Examples

### Example 1: Single Developer (Sequential)
```
Day 1 Morning: T001-T005 (Setup + Foundational) - ~1 hour
Day 1 Afternoon: T006-T014 (Phase 3a: DatasetReader + gRPC) - ~2 hours
Day 1 Late Afternoon: T015-T019 (Phase 3b: InMemoryDataset) - ~1 hour
Day 2 Morning: T020-T026 (Phase 4: gRPC testHost integration) - ~1.5 hours
Day 2 Afternoon: T027-T034 (Phase 5: Polish) - ~1 hour
Total: ~6.5 hours
```

### Example 2: Two Developers (Critical Path + Secondary Path)
```
Developer A: T001-T005 (Setup + Foundational - BLOCKING) - ~1 hour
Developer B: Waits for Phase 2

Once Phase 2 complete:
Developer A: T006-T014 (Phase 3a: DatasetReader + gRPC) [CRITICAL PATH] - ~2 hours
Developer B: Starts prep on tests

Once Phase 3a complete:
Developer A: T015-T019 (Phase 3b: InMemoryDataset) - ~1 hour
Developer B: T020-T026 (Phase 4: gRPC testHost) [PARALLEL with A]

Together: T027-T034 (Phase 5: Polish & Testing)
Total: ~5-5.5 hours wall clock
```

### Example 3: Three Developers (Maximum Parallelization)
```
Developer 1: T001-T005 (Setup + Foundational - BLOCKING) - ~1 hour

Once Foundational ready:
Developer 1: T006-T014 (Phase 3a: DatasetReader + gRPC) [CRITICAL PATH]
Developer 2: Waits for Phase 3a to complete or starts test planning
Developer 3: Starts Phase 4 test planning

Once Phase 3a complete:
Developer 1: T015-T019 (Phase 3b: InMemoryDataset)
Developer 2: T020-T026 (Phase 4: gRPC testHost) [PARALLEL with Dev 1]
Developer 3: Starts Phase 5 prep work (regression test planning)

Once Phase 3a, 3b, 4 complete:
All: T027-T034 (Phase 5: Polish & Complete Testing)
Total: ~4.5-5 hours wall clock
```

---

## Implementation Strategy

### MVP First (Recommended - Phase 3a Only, Critical Path)

1. Complete Phase 1: Setup (Proto contracts) - **~45 min**
2. Complete Phase 2: Foundational (IDatasetReader interface) - **~15 min**
3. Complete Phase 3a: DatasetReader + GrpcReaderServiceImpl (CRITICAL) - **~2 hours**
4. **STOP and VALIDATE**: 
   - Run tests: `dotnet test tst/RxDatasets.Tests/DataReaderCountChildrenTests.cs`
   - Verify gRPC works through GrpcReaderServiceImpl
   - Get user feedback on critical path implementation
5. **Deploy Critical Path MVP** if validation successful

**Total MVP Time**: ~3 hours

### Phase 2: Complete User Story 1 (Add Phase 3b)

1. Complete Phase 3b: InMemoryDataset implementation - **~1 hour**
2. **STOP and VALIDATE**: 
   - Run all tests: `dotnet test tst/RxDatasets.Tests/ tst/RxDatasets.StorageProvider.InMemoryProvider/`
   - Verify all IDatasetReader implementations have CountChildren
3. **Deploy User Story 1** if validation successful

**Additional Time**: ~1 hour (Total: ~4 hours)

### Phase 3: Add gRPC TestHost Integration (Add Phase 4)

1. Complete Phase 4: gRPC testHost integration - **~1.5 hours**
2. **STOP and VALIDATE**: 
   - Run tests: `dotnet test tst/RxDatasets.StorageTests/E2eTestsUsingTestHost.cs`
   - Verify end-to-end gRPC integration with testHost
3. **Deploy Phase 4** if validation successful

**Additional Time**: ~1.5 hours (Total: ~5.5 hours)

### Phase 4: Complete Feature (Add Polish & Testing)

1. Complete Phase 5: Polish & Cross-cutting concerns - **~1 hour**
2. Full regression testing across all scenarios
3. Performance validation
4. **Deploy Complete Feature**

**Additional Time**: ~1 hour (Total: ~6.5 hours)

---

## Validation Checkpoints

### After Phase 2 (Foundational)
```bash
# Verify interface is properly extended
grep -n "Task<uint> CountChildren" src/RxDatasets.Abstractions/IDatasetReader.cs
```

### After Phase 3a (User Story 1a - DatasetReader + gRPC) ⭐ CRITICAL PATH CHECKPOINT
```bash
# Run DatasetReader and GrpcReaderServiceImpl tests
dotnet test tst/RxDatasets.Tests/DataReaderCountChildrenTests.cs

# Verify gRPC wrapper works with GrpcReaderServiceImpl
dotnet test tst/RxDatasets.Tests/ -k GrpcReaderServiceImpl
```

### After Phase 3b (User Story 1b - InMemoryDataset)
```bash
# Run InMemoryDataset tests
dotnet test tst/RxDatasets.StorageProvider.InMemoryProvider/

# Verify all IDatasetReader implementations have CountChildren
dotnet test tst/RxDatasets.Tests/
```

### After Phase 4 (User Story 2 - gRPC testHost integration)
```bash
# Run end-to-end gRPC tests
dotnet test tst/RxDatasets.StorageTests/E2eTestsUsingTestHost.cs

# Verify concurrent gRPC calls work
dotnet test tst/RxDatasets.StorageTests/ -k Concurrent
```

### After Phase 5 (Polish)
```bash
# Full test suite
dotnet test

# Full integration tests with all storage providers
dotnet test tst/RxDatasets.StorageTests/ --configuration Release
```

---

## Notes

- [P] tasks = different files, no dependencies on incomplete tasks
- [Story] label maps task to specific user story (US1a, US1b, US2) for traceability
- **Phase 3a is the critical path for MVP** - DatasetReader + GrpcReaderServiceImpl must be completed first
- Phase 3b (InMemoryDataset) and Phase 4 (gRPC testHost) can run in parallel after Phase 3a completes
- Tests written FIRST using TDD approach - should FAIL before implementation
- **Implementation limited to 2 IDatasetReader implementations + gRPC wrapper - NO storage provider changes**
- Commit after each task or logical group to maintain clean history
- Stop at any checkpoint to validate independently before proceeding
- Use existing patterns (CombiTokenSource, RpcException, etc.) for consistency
- Test locations matter: 
  - `RxDatasets.Tests/` for DatasetReader + GrpcReaderServiceImpl tests (Phase 3a)
  - `RxDatasets.StorageProvider.InMemoryProvider/` for InMemoryDataset tests (Phase 3b)
  - `RxDatasets.StorageTests/` for end-to-end gRPC tests (Phase 4)

