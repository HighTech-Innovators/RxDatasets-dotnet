# Tasks: Lock Handover Mechanism

**Feature**: `001-lock-handover`
**Spec**: [specs/001-lock-handover/spec.md](specs/001-lock-handover/spec.md)

## Phase 1: Setup
*(Project initialization and configuration)*

- [x] T001 Define public `LockServiceState` enum in `src/RxDatasets.Abstractions/BackgroundServices/LockServiceState.cs`
- [x] T002 Add `HandoverTimeout` property to `src/RxDatasets/BackgroundServices/DistributedLockOptions.cs`
- [x] T003 Implement configuration validation for `HandoverTimeout > PollingInterval` in `src/RxDatasets/BackgroundServices/DistributedLockService.cs`
- [x] T004 Define `request-lock` file path constant in `src/RxDatasets/BackgroundServices/DistributedLockService.cs`

## Phase 2: Foundational
*(Blocking prerequisites for all user stories)*

- [x] T005 Create `DistributedLockServiceTests.cs` if not exists in `tst/RxDatasets.Tests/DistributedLockServiceTests.cs` (setup test class)

## Phase 3: User Story 1 - Lock Handover
*(When a new instance starts, it takes over the lock)*

**Goal**: Enable smooth handover of the lock from an active instance to a newly started one.
**Test Criteria**: Instance A releases lock when Instance B requests it; Instance B acquires it.

- [x] T006 [P] [US1] Create unit test for `WriteRequestLockFile` on startup in `tst/RxDatasets.Tests/DistributedLockServiceTests.cs`
- [x] T007 [US1] Implement "Request on Start" logic (write file, overwrite if exists) in `src/RxDatasets/BackgroundServices/DistributedLockService.cs`
- [x] T008 [P] [US1] Create unit test for `MonitorForRequests` triggering yielding in `tst/RxDatasets.Tests/DistributedLockServiceTests.cs`
- [x] T009 [US1] Implement "Monitor for Requests" loop (check file, verify GUID) in `src/RxDatasets/BackgroundServices/DistributedLockService.cs`
- [x] T010 [US1] Implement `Yielded` state transition and stop renewal in `src/RxDatasets/BackgroundServices/DistributedLockService.cs`
- [x] T011 [P] [US1] Create unit test for "Cleanup on Acquire" in `tst/RxDatasets.Tests/DistributedLockServiceTests.cs`
- [x] T012 [US1] Implement "Cleanup on Acquire" (delete request file) in `src/RxDatasets/BackgroundServices/DistributedLockService.cs`
- [x] T013 [US1] Wire up end-to-end handover flow in `StartAsync` loop in `src/RxDatasets/BackgroundServices/DistributedLockService.cs`

## Phase 4: User Story 2 - Cleanup and Stability
*(Ensure system recovers from crashes and cleans up files)*

**Goal**: Prevent orphaned request files and lost locks.
**Test Criteria**: Request files deleted on stop; Lock reclaimed if requester crashes.

- [x] T014 [P] [US2] Create unit test for "Cleanup on Stop" in `tst/RxDatasets.Tests/DistributedLockServiceTests.cs`
- [x] T015 [US2] Implement "Cleanup on Stop" (delete own request file) inside `StopAsync` in `src/RxDatasets/BackgroundServices/DistributedLockService.cs`
- [x] T016 [P] [US2] Create unit test for "Fallback Reclaim" (timeout expiry) in `tst/RxDatasets.Tests/DistributedLockServiceTests.cs`
- [x] T017 [US2] Implement "Fallback Reclaim" logic (timer/check in `Yielded` state) in `src/RxDatasets/BackgroundServices/DistributedLockService.cs`
- [x] T018 [US2] Implement orphaned `request-lock` cleanup (delete on acquire even if GUID mismatch) in `src/RxDatasets/BackgroundServices/DistributedLockService.cs` (refines T012)

## Phase 5: Polish & Cross-Cutting
*(Refactoring, logging, and performance tuning)*

- [x] T019 Review and enhance logging for state transitions (`Idle` -> `Acquiring` -> `Active` -> `Yielded`) in `src/RxDatasets/BackgroundServices/DistributedLockService.cs`
- [x] T020 Run full integration test suite to ensure no regressions in basic lock behavior

## Dependencies

1. **Setup** (T001-T004) -> **US1** (T006-T013)
2. **US1** (T006-T013) -> **US2** (T014-T018)
3. **US2** -> **Polish** (T019-T020)

## Parallel Execution Examples

- **US1**: T006 (Test) and T008 (Test) can be written while T007 (Impl) is being developed.
- **US2**: T014 (Test) can be written in parallel with US1 implementation.
