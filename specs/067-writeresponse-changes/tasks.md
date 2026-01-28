# Implementation Tasks: Extend WriteResponse with Optional Change Details

**Feature**: Spec 067 - Extend WriteResponse with Optional Change Details  
**Branch**: `067-writeresponse-changes`  
**Created**: January 28, 2026  
**Status**: Task Breakdown Complete  

**Related Plans**: [plan.md](plan.md) | [spec.md](spec.md) | [data-model.md](data-model.md)

---

## Task Breakdown Overview

**Total Tasks**: 50  
**Phases**: 4 (Proto Setup, Core Implementation, Testing, Documentation)  
**Estimated Duration**: 3-4 sprints (6-8 weeks)  
**Dependencies**: Minimal external; builds on existing RxDatasets infrastructure

### Phase Breakdown

- **Phase 1 (Setup)**: 3 tasks - Proto files and project setup
- **Phase 2 (Implementation)**: 13 tasks - Core feature development
- **Phase 3 (Testing)**: 18 tasks - Unit and integration testing
- **Phase 4 (Documentation & Release)**: 4 tasks - Final documentation and deployment

---

## Phase 1: Proto Setup & Project Configuration

### Setup Tasks

- [x] T001 Create ChangeType enum definition in proto files with values NEW=0, CHANGED=1, DELETED=2
- [x] T002 Create DetailedChange proto message with entity_id (string) and change_type (ChangeType) fields
- [x] T003 [P] Extend WriteResponse proto with field 5: repeated DetailedChange detailed_changes
- [x] T004 [P] Verify proto changes and regenerate C# code via `dotnet build`
- [x] T005 [P] Add testing references: Google.Protobuf test utilities, xUnit, integration test helpers

---

## Phase 2: Core Implementation

### 2.1 Data Model & Change Tracking

- [x] T006 Create DetailedChange model class in src/RxDatasets/Models/ if not auto-generated
- [x] T007 [P] Implement change tracking logic: identify newly created entities (didn't exist → exists)
- [x] T008 [P] Implement change tracking logic: identify updated entities (CRC32 differs)
- [x] T009 [P] Implement change tracking logic: identify deleted entities (existed → deleted)
- [x] T010 [P] Implement filtering logic to exclude unchanged entities from detailed_changes list

### 2.2 Header Detection & Service Layer

- [ ] T011 Implement X-Debug header detection in GrpcReaderServiceImpl.cs (case-insensitive matching)
- [ ] T012 [P] Implement X-Debug header detection in DtoServerBuilder.cs (testhost HTTP requests)
- [x] T013 [P] Create shared header detection utility: ExtractXDebugHeader() method, case-insensitive, reusable

### 2.3 Write Operation Implementation

- [ ] T014 Implement detailed_changes population in DatasetWriter.Put() method
- [ ] T015 [P] Implement detailed_changes population in DatasetWriter.Delete() method
- [ ] T016 [P] Implement detailed_changes population in DatasetWriter.PutAll() method (streaming)
- [ ] T017 [P] Implement detailed_changes population in DatasetWriter.PutMany() method (streaming)
- [ ] T018 Implement detailed_changes population in DatasetWriter.PutOrDeleteMany() method with net effect consolidation per Spec 002

### 2.4 Integration & Net Effect Logic

- [ ] T019 Verify net effect consolidation: delete(X) + create(X, same_data) → no changes reported
- [ ] T020 [P] Verify net effect consolidation: delete(X) + create(X, different_data) → CHANGED reported
- [ ] T021 [P] Verify net effect consolidation: multiple puts to same entity → final state determines change_type
- [ ] T022 Performance testing: verify sub-50ms baseline without X-Debug header
- [ ] T023 Performance testing: verify sub-100ms with X-Debug for <1000 entities

---

## Phase 3: Testing

### 3.1 Unit Tests (Change Type & Message Serialization)

- [ ] T024 [P] Unit test: ChangeType enum values (NEW=0, CHANGED=1, DELETED=2)
- [ ] T025 [P] Unit test: DetailedChange message serialization and deserialization
- [ ] T026 [P] Unit test: WriteResponse backward compatibility (field 5 optional)
- [ ] T027 [P] Unit test: Header detection utility - case-insensitive matching (X-Debug, x-debug, X-DEBUG)

### 3.2 Integration Tests (Per Storage Provider)

- [ ] T028 [P] [Provider:Local] Integration test: Put with X-Debug → detailed_changes includes entity
- [ ] T029 [P] [Provider:Local] Integration test: Delete with X-Debug → detailed_changes shows DELETED
- [ ] T030 [P] [Provider:Local] Integration test: PutMany with X-Debug → all changes collected
- [ ] T031 [P] [Provider:Local] Integration test: PutOrDeleteMany with net effect consolidation + X-Debug
- [ ] T032 [P] [Provider:Local] Integration test: Large batch (10k entities, 10 changes) → response size scales with changes

- [ ] T033 [P] [Provider:S3] Integration test: All above operations against S3 storage
- [ ] T034 [P] [Provider:AzureBlob] Integration test: All above operations against Azure Blob storage
- [ ] T035 [P] [Provider:GoogleCloud] Integration test: All above operations against Google Cloud storage
- [ ] T036 [P] [Provider:Minio] Integration test: All above operations against Minio storage

### 3.3 Header & Response Tests

- [ ] T037 [P] Integration test: X-Debug header absent → detailed_changes empty
- [ ] T038 [P] Integration test: X-Debug header present (any value) → detailed_changes populated
- [ ] T039 [P] Integration test: Case-insensitive header matching works across HTTP clients

### 3.4 Edge Cases & Net Effect

- [ ] T040 [P] Integration test: No entities changed → detailed_changes empty even with X-Debug
- [ ] T041 [P] Integration test: Batch with mostly unchanged entities → only changed ones in list
- [ ] T042 [P] Integration test: Net effect consolidation - verify against Spec 002 rules

---

## Phase 4: Documentation & Release

### 4.1 Documentation Updates

- [ ] T043 Update WRITER-CONTRACT.md: add WriteResponse proto with field 5 definition
- [ ] T044 [P] Update API documentation: add X-Debug header documentation
- [ ] T045 [P] Create RELEASE_NOTES.md entry: non-breaking change, backward compatible, new feature
- [ ] T046 [P] Update quick start guide with production best practices and performance guidance

### 4.2 Final Validation & Deployment

- [ ] T047 Code review: proto changes, backward compatibility verification
- [ ] T048 [P] Final integration test run: all storage providers, all operations
- [ ] T049 Build verification: no breaking changes, all tests passing
- [ ] T050 Merge to main branch and tag release

---

## Task Dependency Graph

```
Phase 1 (Setup):
  T001 → T002 → T003 → T004 ─┐
                  ↓           ├→ T005 (can run parallel with T004)
                (regenerate)  │
                              ↓
Phase 2 (Implementation):
              T006 ──┐
              T007 ──┼→ T010 (filtering logic)
              T008 ──┤
              T009 ──┘

              T011 ─┐
              T012 ─┼→ T013 (shared utility)
                    ┘

              T014 ──┐
              T015 ──┤
              T016 ──┼→ T018 (consolidate all with net effect)
              T017 ──┤
                    ┘

              T019 ─┐
              T020 ─┼→ T021 (net effect verification)
              T022 ─┤
              T023 ─┘

Phase 3 (Testing):
              T024 ─┐
              T025 ─┼→ T027 (unit tests)
              T026 ─┘

              T028 ─┐
              T029 ─┤
              T030 ─┼→ T032 (Local provider tests)
              T031 ─┤
                    ┘

              T033, T034, T035, T036 (parallel provider tests)

              T037 ─┐
              T038 ─┼→ T039 (header tests)
              T040 ─┤
              T041 ─┤
              T042 ─┘

Phase 4 (Documentation & Release):
              T043 ─┐
              T044 ─┼→ T047 (finalization)
              T045 ─┤
              T046 ─┘

              T048 ─┐
              T049 ─┼→ T050 (deployment)
```

---

## Parallel Execution Groups

The following task groups can run independently and in parallel once their dependencies are satisfied:

### Group A (Core Implementation - Parallel after T005)
- **Tasks**: T006-T010 (Change tracking logic)
- **Dependency**: T005 complete
- **Duration**: 2-3 days
- **Ownership**: 1-2 developers

### Group B (Header Detection - Parallel after T005)
- **Tasks**: T011-T013 (Header detection utility)
- **Dependency**: T005 complete
- **Duration**: 1-2 days
- **Ownership**: 1 developer
- **Can merge with**: Group C once available

### Group C (Write Operations - After T005, T006-T010)
- **Tasks**: T014-T023 (All write operations with net effect)
- **Dependency**: Group A complete, T013 available
- **Duration**: 4-5 days
- **Ownership**: 2-3 developers
- **Parallelizable**: T014-T017 can run in parallel, T018 consolidates

### Group D (Unit Tests - Parallel after T004)
- **Tasks**: T024-T027 (Unit tests)
- **Dependency**: T004 complete
- **Duration**: 1-2 days
- **Ownership**: 1 developer
- **Can start early**: Independent of implementation

### Group E (Integration Tests - After Group C)
- **Tasks**: T028-T042 (All integration tests)
- **Dependency**: Group C complete
- **Duration**: 5-7 days
- **Ownership**: 2-3 developers
- **Highly parallelizable**: Each storage provider can have dedicated tester

### Group F (Documentation & Release - After Group E)
- **Tasks**: T043-T050 (Finalization)
- **Dependency**: Group E complete
- **Duration**: 2-3 days
- **Ownership**: 1 developer + code review

---

## Independent Test Criteria by Phase

### Phase 1 (Setup)
**MVP Completeness**: Not tested independently (prerequisite only)

### Phase 2 (Core Implementation)
**MVP Completeness**: Can verify:
- ✅ Proto compiles and generates C# code
- ✅ ChangeType enum values correct
- ✅ DetailedChange message serializes
- ✅ X-Debug header detection works (case-insensitive)
- ✅ WriteResponse field 5 optional (backward compatible)

**Independent Test**: Single Put operation with X-Debug header returns detailed_changes with ONE entity marked NEW

### Phase 3 (Testing)
**MVP Completeness**: Full test coverage:
- ✅ All change types (NEW, CHANGED, DELETED) tested
- ✅ All write operations tested (Put, Delete, PutAll, PutMany, PutOrDeleteMany)
- ✅ All storage providers covered
- ✅ Performance targets verified
- ✅ Edge cases (large batches, net effect consolidation)

**Independent Test**: Run complete test suite against all storage providers; verify all tests pass

### Phase 4 (Documentation & Release)
**MVP Completeness**: Ready for deployment:
- ✅ Documentation complete
- ✅ Release notes prepared
- ✅ Backward compatibility verified
- ✅ All tests passing

---

## Task Estimates & Sequencing

### Critical Path
```
T001 → T002 → T003 → T004 (proto setup: ~2 days)
         ↓
        T005 (test setup: ~1 day)
         ↓
      [Parallel Groups B, D start]
         ↓
      [Group A: change tracking - 2-3 days]
         ↓
      [Group C: write operations - 4-5 days, blocks Group E]
         ↓
      [Group E: integration tests - 5-7 days]
         ↓
      [Group F: documentation & release - 2-3 days]

Total Critical Path: ~17-21 days (3 weeks)
```

### Optimal Timeline (with Parallelization)

```
Week 1:
  Day 1-2: Phase 1 (Setup) - T001-T005
  Day 2-3: Parallel Groups B, D (Header Detection, Unit Tests)
  Day 3-5: Parallel Group A (Change Tracking)

Week 2:
  Day 1-5: Parallel Group C (Write Operations + Group E prep)

Week 3:
  Day 1-5: Parallel Group E (Integration Tests - all providers)

Week 4 (Partial):
  Day 1-3: Parallel Group F (Documentation & Release)

Total: 3-4 weeks with 2-3 developers
```

---

## Success Metrics

| Metric | Target | How to Verify |
|--------|--------|---------------|
| Proto backward compatibility | 100% | Integration tests with old clients |
| Code coverage | ≥95% for new code | Coverage report |
| Performance (no X-Debug) | Sub-50ms baseline | Performance test T022 |
| Performance (X-Debug) | Sub-100ms for <1k entities | Performance test T023 |
| Storage provider coverage | All 5 providers | T033-T036 passing |
| Header matching | Case-insensitive | T039 passing |
| Net effect consolidation | Matches Spec 002 | T042 passing |
| Documentation | Complete & examples | T043-T046 reviews |

---

## Implementation Strategy

### MVP Scope (Week 1-2)
**Tasks**: T001-T023 + Unit tests (T024-T027)
**Deliverable**: Core feature working on Local storage provider
**Testability**: Single Put with X-Debug header works correctly

### Full Scope (Week 1-4)
**Tasks**: All 50 tasks
**Deliverable**: Feature complete with all storage providers, full test coverage
**Testability**: All tests passing, backward compatible, performance targets met

### Minimum Viable Approach
Start with:
1. Proto changes (T001-T005)
2. Single Put operation implementation (T014 + T024-T026)
3. Basic unit tests
4. Enable Local storage only

Then incrementally add:
- Other write operations (T015-T017)
- Storage providers (T033-T036)
- Complete test coverage (T028-T042)
- Documentation (T043-T046)

---

## Known Constraints & Mitigations

| Constraint | Impact | Mitigation |
|-----------|--------|-----------|
| Proto regeneration requires build | Minor delay | Automated via `dotnet build` |
| Storage provider variety | Test effort | Parallel testing groups (T033-T036) |
| Net effect logic complexity | Implementation risk | Leverage existing Spec 002 code |
| Performance targets (50/100ms) | Could delay release | Early performance testing (T022-T023) |
| Case-insensitive header matching | Edge case bugs | Explicit test coverage (T039) |

---

## Risk Register

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|-----------|
| Proto change breaks old clients | Low | High | T026 backward compat test |
| Large batch performance issue | Low | Medium | T023 performance test early |
| Storage provider inconsistency | Low | Medium | T033-T036 parallel testing |
| Net effect consolidation bugs | Low | High | T019-T021 verification, T042 edge cases |
| Header detection edge cases | Low | Low | T039 comprehensive header test |

---

## Notes for Development Team

1. **Proto Regeneration**: Always run `dotnet build` after modifying .proto files
2. **Storage Providers**: Each provider test should be independent; run in parallel
3. **Net Effect Logic**: Refer to Spec 002 for consolidation rules; don't reinvent
4. **Performance**: Measure early (T022-T023); large batches should scale with changes, not batch size
5. **Backward Compatibility**: Verify with T026; proto field 5 must remain optional
6. **Header Detection**: Must be case-insensitive; use shared utility (T013)

---

## Related Documentation

- [spec.md](spec.md) - Feature specification
- [plan.md](plan.md) - Implementation phases
- [data-model.md](data-model.md) - Proto data model details
- [contracts/detailed-changes-contract.md](contracts/detailed-changes-contract.md) - Proto contract
- [quickstart.md](quickstart.md) - Developer integration guide
