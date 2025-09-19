# Tasks: PUTORDELETEMANY Corner Cases Bug Fix

**Input**: Design documents from `/specs/002-putordeletemany-corner-cases/`
**Prerequisites**: plan.md (required), research.md, data-model.md, contracts/, quickstart.md

## Execution Flow (main)
```
1. Load plan.md from feature directory ✓
   → Feature spec loaded successfully
   → Extract: C# .NET 8.0, Google.Protobuf, System.Reactive, Microsoft.Extensions.Hosting, Grpc.Core.Api
   → Structure: Single project with existing RxDatasets architecture
2. Load optional design documents: ✓
   → data-model.md: Enhanced batch operation processing logic
   → contracts/: putordeletemany-contract.md → contract test task
   → research.md: Delta processing logic fix, CRC32-based change detection
   → quickstart.md: Three test scenarios for realistic server testing
3. Generate tasks by category: ✓
   → Setup: None needed (existing project)
   → Tests: Contract tests, integration tests with testhost, unit tests
   → Core: DatasetWriter.cs batch operation logic enhancement
   → Integration: Testhost scenarios with TestClient
   → Polish: Unit tests, performance validation
4. Apply task rules: ✓
   → Different files = mark [P] for parallel
   → Same file = sequential (no [P])
   → Tests before implementation (TDD)
5. Number tasks sequentially (T001, T002...) ✓
6. Generate dependency graph ✓
7. Create parallel execution examples ✓
8. Validate task completeness: ✓
   → All contracts have tests ✓
   → All enhancement scenarios tested ✓
   → All endpoints validated ✓
9. Return: SUCCESS (tasks ready for execution)
```

## Format: `[ID] [P?] Description`
- **[P]**: Can run in parallel (different files, no dependencies)
- Include exact file paths in descriptions

## Path Conventions
- **Single project**: `src/`, `tst/`, `testapps/` at repository root
- Paths relative to repository root: d:\reactive-datasets\

## Phase 3.1: Setup
*No setup needed - using existing RxDatasets project structure*

## Phase 3.2: Tests First (TDD) ⚠️ MUST COMPLETE BEFORE 3.3
**CRITICAL: These tests MUST be written and MUST FAIL before ANY implementation**
- [ ] T001 [P] Contract test for PutOrDeleteMany same entity delete+put in `tst\RxDatasets.Tests\DatasetWriterTests\PutOrDeleteManyContractTests.cs`
- [ ] T002 [P] Contract test for PutOrDeleteMany different entity delete+put in `tst\RxDatasets.Tests\DatasetWriterTests\PutOrDeleteManyContractTests.cs`
- [ ] T003 [P] Contract test for PutOrDeleteMany multiple puts ending with original in `tst\RxDatasets.Tests\DatasetWriterTests\PutOrDeleteManyContractTests.cs`
- [ ] T004 Integration test delete+put same entity scenario in `tst\RxDatasets.StorageTests\E2eTestsUsingTestHost.cs` - Add Test_PutOrDeleteMany_DeletePutSameEntity_ReportsNoChange method
- [ ] T005 Integration test delete+put different entity scenario in `tst\RxDatasets.StorageTests\E2eTestsUsingTestHost.cs` - Add Test_PutOrDeleteMany_DeletePutDifferentEntity_ReportsChange method
- [ ] T006 Integration test multiple operations same entity in `tst\RxDatasets.StorageTests\E2eTestsUsingTestHost.cs` - Add Test_PutOrDeleteMany_MultiplePutsOriginal_ReportsNoChange method

## Phase 3.3: Core Implementation (ONLY after tests are failing)
- [ ] T007 Enhance Delta processing logic in `src\RxDatasets\DatasetWriter.cs` - Add operation consolidation
- [ ] T008 Implement net effect calculation in `src\RxDatasets\DatasetWriter.cs` - CRC32-based change detection
- [ ] T009 Update change reporting logic in `src\RxDatasets\DatasetWriter.cs` - Correct ChangeType reporting
- [ ] T010 Add operation sequence validation in `src\RxDatasets\DatasetWriter.cs` - Input validation enhancement

## Phase 3.4: Integration
- [ ] T011 [P] Update TestClient scenarios in `testapps\TestClient` - Add delete-put-same scenario
- [ ] T012 [P] Update TestClient scenarios in `testapps\TestClient` - Add delete-put-different scenario
- [ ] T013 [P] Update TestClient scenarios in `testapps\TestClient` - Add multiple-puts-original scenario
- [ ] T014 Verify Host testapp compatibility in `testapps\Host` - Ensure existing functionality works

## Phase 3.5: Polish
- [ ] T015 [P] Unit tests for operation consolidation logic in `tst\RxDatasets.Tests\DatasetWriterTests\OperationConsolidationTests.cs`
- [ ] T016 [P] Unit tests for net effect calculation in `tst\RxDatasets.Tests\DatasetWriterTests\NetEffectCalculationTests.cs`
- [ ] T017 [P] Unit tests for enhanced change reporting in `tst\RxDatasets.Tests\DatasetWriterTests\ChangeReportingTests.cs`
- [ ] T018 Performance validation - Ensure no regression in batch operation throughput
- [ ] T019 [P] Verify backward compatibility - Existing API contracts unchanged
- [ ] T020 Manual testing following `specs\002-putordeletemany-corner-cases\quickstart.md`

## Dependencies
```
Phase 3.2: Tests First (T001-T006) → Phase 3.3: Core Implementation (T007-T010)
Phase 3.3: Core Implementation → Phase 3.4: Integration (T011-T014)
Phase 3.4: Integration → Phase 3.5: Polish (T015-T020)

Within Phase 3.2:
T001-T003 can run in parallel (different contract test methods)
T004-T006 are sequential (same file: E2eTestsUsingTestHost.cs)

Within Phase 3.3 (same file src\RxDatasets\DatasetWriter.cs):
T007 → T008 → T009 → T010 (sequential, same file)

Within Phase 3.4 (different files):
T011, T012, T013 can run in parallel (different scenarios)
T014 depends on T007-T010 completion
```

## Parallel Example
```
# Launch T001-T003 together (different contract test methods):
Task: "Contract test for PutOrDeleteMany same entity delete+put in tst\RxDatasets.Tests\DatasetWriterTests\PutOrDeleteManyContractTests.cs"
Task: "Contract test for PutOrDeleteMany different entity delete+put in tst\RxDatasets.Tests\DatasetWriterTests\PutOrDeleteManyContractTests.cs"
Task: "Contract test for PutOrDeleteMany multiple puts ending with original in tst\RxDatasets.Tests\DatasetWriterTests\PutOrDeleteManyContractTests.cs"

# T004-T006 are sequential (same file E2eTestsUsingTestHost.cs):
Task: "Integration test delete+put same entity scenario in tst\RxDatasets.StorageTests\E2eTestsUsingTestHost.cs"
# Then:
Task: "Integration test delete+put different entity scenario in tst\RxDatasets.StorageTests\E2eTestsUsingTestHost.cs"
# Then:
Task: "Integration test multiple operations same entity in tst\RxDatasets.StorageTests\E2eTestsUsingTestHost.cs"

# Launch T011-T013 together (different client scenarios):
Task: "Update TestClient scenarios in testapps\TestClient - Add delete-put-same scenario"
Task: "Update TestClient scenarios in testapps\TestClient - Add delete-put-different scenario"
Task: "Update TestClient scenarios in testapps\TestClient - Add multiple-puts-original scenario"

# Launch T015-T017 together (different unit test files):
Task: "Unit tests for operation consolidation logic in tst\RxDatasets.Tests\DatasetWriterTests\OperationConsolidationTests.cs"
Task: "Unit tests for net effect calculation in tst\RxDatasets.Tests\DatasetWriterTests\NetEffectCalculationTests.cs"
Task: "Unit tests for enhanced change reporting in tst\RxDatasets.Tests\DatasetWriterTests\ChangeReportingTests.cs"
```

## Notes
- [P] tasks = different files, no dependencies
- Verify tests fail before implementing (TDD approach)
- Focus on `src\RxDatasets\DatasetWriter.cs` for core implementation
- Use testhost approach for realistic server testing as specified in quickstart.md
- Maintain backward compatibility - no API contract changes
- Commit after each task completion

## Task Generation Rules
1. **Contract-based**: putordeletemany-contract.md → T001-T003 (contract tests)
2. **Scenario-based**: quickstart.md scenarios → T004-T006 (integration tests in existing E2eTestsUsingTestHost.cs), T011-T013 (client scenarios)
3. **Implementation-based**: plan.md core fix → T007-T010 (DatasetWriter.cs changes)
4. **Quality-based**: data-model.md logic → T015-T017 (unit tests), T018-T020 (validation)
5. **Dependency-ordered**: Tests → Implementation → Integration → Polish
6. **Parallel-optimized**: Different files marked [P], same file sequential
7. **Existing infrastructure**: Use E2eTestsUsingTestHost.cs patterns for realistic test host scenarios
