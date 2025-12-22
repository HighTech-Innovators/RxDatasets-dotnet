# Implementation Tasks: GCS Write Queue for Rate Limit Optimization

**Feature**: 056-gcs-rate-limit-queue  
**Branch**: `056-gcs-rate-limit-queue`  
**Date**: 2025-12-22  
**Status**: Ready for Implementation

**Overall Summary**:
- **Total Tasks**: 21
- **Phases**: 5 (Setup, Foundational, US1, US2, US3, Polish)
- **User Stories**: 3 (all P1 priority)
- **Estimated Implementation Time**: 5-8 days
- **Suggested MVP Scope**: User Story 1 + Foundational tasks (T001-T010, T012-T016)

---

## Dependencies & Execution Order

```
Phase 1: Setup (T001-T003)
  ↓
Phase 2: Foundational (T004-T010)
  ├─→ Phase 3: User Story 1 - Queued Writes (T012-T016) [P1]
  ├─→ Phase 3b: User Story 2 - Graceful Shutdown (T017-T019) [P1]
  └─→ Phase 3c: User Story 3 - Client Disposal (T020-T021) [P1]
  
Phase 4: Polish & Cross-Cutting (T022-T023)
```

**Parallel Opportunities**:
- US1, US2, US3 can be implemented simultaneously after foundational work (different classes)
- T012-T016 (US1) and T017-T019 (US2) are mostly independent
- T020-T021 (US3) must be reviewed together with US2 for shutdown coordination

---

## Phase 1: Project Setup

- [x] T001 Create QueuedWrite.cs internal class in src/RxDatasets.StorageProvider.GoogleCloud/
  - Contains: ObjectKey, Bytes, Stream, IsStreamSource, QueuedAt, SequenceNumber
  - Add XML documentation and validation
  - Reference: data-model.md - Entity: QueuedWrite

- [x] T002 Create FileWriteQueueState.cs internal class in src/RxDatasets.StorageProvider.GoogleCloud/
  - Contains: ObjectKey, PendingWrites Queue, LastSuccessfulWrite, FlushScheduled, FlushTask, Lock
  - Implement IsFlushEligible() and GetMillisecondsUntilFlushEligible() helper methods
  - Reference: data-model.md - Entity: FileWriteQueueState

- [x] T003 Create WriteQueueManager.cs internal class in src/RxDatasets.StorageProvider.GoogleCloud/
  - Contains: _queues (ConcurrentDictionary), _sequenceNumber, _isDisposing, _logger, _flushAction
  - Implement TryQueueWrite(objectKey, bytes/stream), MarkSuccessfulWrite(), ScheduleFlush(), ExecuteFlush(), FlushAndDisposeAsync()
  - Reference: data-model.md - Entity: WriteQueueManager

---

## Phase 2: Foundational Infrastructure

- [x] T004 [P] Modify GoogleCloudStorage<T> constructor in src/RxDatasets.StorageProvider.GoogleCloud/GoogleCloudStorage.cs
  - Add _writeQueueManager initialization
  - Pass _logger and FlushQueuedWriteAsync delegate to manager
  - Ensure _disposed volatile field is added

- [x] T005 [P] Implement FlushQueuedWriteAsync() helper method in GoogleCloudStorage.cs
  - Private async Task method that executes queued write (calls UploadBytes or UploadFileStream)
  - Used as delegate for WriteQueueManager

- [x] T006 [P] Add IAsyncDisposable implementation to GoogleCloudStorage<T> in GoogleCloudStorage.cs
  - Implement ValueTask DisposeAsync() method
  - Set _disposed flag, call _writeQueueManager.FlushAndDisposeAsync(), dispose _storageClient, GC.SuppressFinalize()

- [x] T007 Add disposal guards to public methods in GoogleCloudStorage.cs
  - Add check at start of UploadBytes(): if (_disposed) throw ObjectDisposedException
  - Add check at start of UploadFileStream(): if (_disposed) throw ObjectDisposedException
  - Prevents use after disposal

- [x] T008 [P] Modify UploadBytes() in GoogleCloudStorage.cs to integrate queue check
  - Before existing rate-limit retry loop: if (_writeQueueManager.TryQueueWrite(objectKey, bytes)) return;
  - After successful write: _writeQueueManager.MarkSuccessfulWrite(objectKey);

- [x] T009 [P] Modify UploadFileStream() in GoogleCloudStorage.cs to integrate queue check
  - Before existing write logic: if (_writeQueueManager.TryQueueWrite(objectKey, stream)) return;
  - After successful write: _writeQueueManager.MarkSuccessfulWrite(objectKey);

- [x] T010 [P] Update RxDatasets.StorageProvider.GoogleCloud.csproj if needed
  - Verify dependencies: Google.Cloud.Storage.V1, System.Reactive, Microsoft.Extensions.Logging
  - Add .NET 8.0 target if not already present
  - Verify no new NuGet packages needed (all types are in BCL/existing dependencies)

---

## Phase 3a: User Story 1 - Queued Writes Avoid Client Blocking [P1]

**Story Goal**: Clients get immediate response (< 50ms) when write is queued, instead of waiting 5.5+ seconds.

**Independent Test Criteria**: 
- Client calls to UploadBytes return immediately when queued
- Queue depth increases with rapid writes < 2 seconds apart
- Flush occurs automatically after 2-second window
- Final data in GCS matches expected state

- [x] T012 [US1] Create WriteQueueTests.cs in tst/RxDatasets.StorageTests/GoogleCloudStorage/
  - Test: RapidWritesToSameFileAreQueued() - Verify 2+ writes < 2s apart result in queue behavior
  - Test: WriteQueueBypassesWhenLastWriteOldEnough() - Verify writes > 2s apart don't queue
  - Test: QueuedWritesFlushAfterTwoSeconds() - Verify automatic flush after 2s delay
  - Test: MultipleFilesHaveIndependentQueues() - Verify different files don't block each other
  - Test: QueuedWritesPreserveOrder() - Verify writes are flushed in FIFO order

- [x] T013 [US1] Implement queue entry logic in TryQueueWrite() method
  - Lock acquisition (EnterReadLock) for checking LastSuccessfulWrite timestamp
  - Create QueuedWrite with sequence number
  - Enqueue to PendingWrites
  - Log debug message with context
  - Call ScheduleFlush()
  - Return true to indicate queuing

- [x] T014 [US1] Implement ScheduleFlush() background task scheduling
  - Calculate delay: GetMillisecondsUntilFlushEligible()
  - If already scheduled, return early
  - Create Task.Delay() → ContinueWith(ExecuteFlush)
  - Set FlushScheduled flag and FlushTask reference

- [x] T015 [US1] Implement ExecuteFlush() dequeue and write logic
  - Acquire write lock
  - Dequeue all pending writes in order
  - For each: call _flushAction (executes UploadBytes/FileStream)
  - Update LastSuccessfulWrite on success
  - Log success with sequence number
  - On failure: log warning with full context, throw to propagate
  - Release flush state (set FlushScheduled = false, FlushTask = null)

- [x] T016 [P] [US1] Implement rapid write performance test in WriteQueueTests.cs
  - Use testcontainer for GCS emulator (gcloud-emulator Docker image or mock)
  - Write 5 rapid updates to same file (< 100ms apart)
  - Assert all calls return in < 50ms (use Stopwatch)
  - Wait 2.5 seconds for flush window to elapse
  - Assert all writes persisted to GCS
  - Assert logs contain "Write queued" entries
  - Reference: quickstart.md - Testing Examples

---

## Phase 3b: User Story 2 - Graceful Queue Flushing on Shutdown [P1]

**Story Goal**: All pending queued writes flushed to GCS before DisposeAsync() returns; no data loss on shutdown.

**Independent Test Criteria**:
- Queued writes are present when disposal starts
- All queued writes flushed before DisposeAsync returns
- DisposeAsync waits for in-flight flush tasks
- No data is lost (final state matches all queued writes)

- [x] T017 [US2] Create ShutdownFlushTests.cs in tst/RxDatasets.StorageTests/GoogleCloudStorage/
  - Test: DisposalFlushesQueuedWrites() - Queue pending writes and verify all flushed on disposal
  - Test: DisposalWaitsForInFlightFlush() - In-flight flush task completed before disposal returns
  - Test: DisposalFlushesPendingWritesFromMultipleFiles() - Multiple files all flushed
  - Test: DisposalTimingExeedsFlushWindow() - Disposal takes >= 2 seconds due to flush

- [x] T018 [US2] Implement FlushAndDisposeAsync() in WriteQueueManager
  - Set _isDisposing = true to prevent new writes
  - Collect all in-flight FlushTask objects
  - If tasks exist: await Task.WhenAll(tasks)
  - For each queue with pending writes: call ExecuteFlush()
  - Log information about flush during shutdown
  - Dispose all FileWriteQueueState.Lock instances
  - Clear _queues dictionary

- [x] T019 [US2] Implement disposal guard in TryQueueWrite() methods
  - Check if _isDisposing flag is true
  - If true: don't queue, let normal write path handle (will fail with proper error)
  - Alternative: queue but don't schedule flush (write gets flushed by FlushAndDisposeAsync)
  - Document which approach taken and why in method comment

---

## Phase 3c: User Story 3 - Reliable Google Cloud Client Disposal [P1]

**Story Goal**: Google Cloud Storage client properly disposed; zero resource leaks; no connection hangs during shutdown.

**Independent Test Criteria**:
- GoogleCloudStorage instance after disposal throws ObjectDisposedException on use
- Google Cloud client is fully released (no dangling handles)
- No connection timeouts or hangs during disposal
- Disposal completes within reasonable time (< 5 seconds)

- [x] T020 [US3] Create DispatcherCleanupTests.cs in tst/RxDatasets.StorageTests/GoogleCloudStorage/
  - Test: ObjectDisposedExceptionThrownAfterDisposal() - Verify ObjectDisposedException on UploadBytes after disposal
  - Test: ClientDisposedBeforeFlagSet() - Verify Google Cloud client is disposed during DisposeAsync
  - Test: DisposalCompletesWithinTimeLimit() - Measure DisposeAsync and verify completes < 5s
  - Test: MultipleDisposalCallsAreIdempotent() - Verify calling DisposeAsync twice is safe

- [x] T021 [US3] Verify client disposal sequence in DisposeAsync()
  - Ensure _writeQueueManager.FlushAndDisposeAsync() completes first
  - Then: _storageClient?.Dispose()
  - Set _disposed = true flag before any operations (or after? depends on threading model)
  - Call GC.SuppressFinalize(this)
  - Document the sequence in code comments explaining why order matters
  - Reference: api-contract.md - Graceful Shutdown Contract

---

## Phase 4: Polish & Cross-Cutting Concerns

- [x] T023 Add comprehensive logging throughout queue operations
  - DEBUG: "Write queued for {ObjectKey} ({Sequence}). Last write was {SecondsSince}s ago"
  - DEBUG: "Flush scheduled for {ObjectKey}, delay: {DelayMs}ms"
  - DEBUG: "Flushed queued write for {ObjectKey} ({Sequence})"
  - WARNING: "Failed to flush queued write for {ObjectKey} ({Sequence}): {Exception}"
  - INFO: "Waiting for {Count} in-flight flush(es) before shutdown"
  - INFO: "Flushing {Count} remaining writes for {ObjectKey} during shutdown"
  - Ensure all log messages include enough context for debugging (keys, sequence numbers, timings)

- [x] T024 Create PR description file PULL_REQUEST.md in repository root (NON-GIT-COMMITTED)
  - Title: "feat: Implement GCS write queue to reduce rate limit latency"
  - Summary: Feature overview (eliminating 5.5s waits via queuing)
  - Changes: Modified files (GoogleCloudStorage.cs), new files (QueuedWrite.cs, FileWriteQueueState.cs, WriteQueueManager.cs)
  - Testing: Automated tests using testcontainers with IClock for time simulation (WriteQueueTests.cs, ShutdownFlushTests.cs, DispatcherCleanupTests.cs)
  - Performance Impact: Sub-50ms returns, no breaking changes
  - Graceful Shutdown: Explain queue flushing on disposal
  - Backward Compatibility: ✓ No breaking changes
  - Reference: Link to feature branch, spec.md, data-model.md
  - Note: File created in root directory but NOT committed to git (copy content to PR when ready)

---

## Implementation Strategy

### MVP Scope (Minimum Viable Product)

Complete Phase 1 + Phase 2 + Phase 3a + Phase 3b:
- Tasks T001-T019 (all queue and shutdown functionality)
- All automated tests passing via testcontainer with IClock time simulation (T012-T017 tests)
- Deliverable: Working queue that reduces client latency for rapid writes
- Time: 5-6 days
- MVP Success: Clients get immediate response for queued writes; queue flushes automatically; all data persisted; safe shutdown; no data loss

### Incremental Delivery

1. **Iteration 1**: Setup + Foundational (T001-T010)
   - Creates all classes and integrations
   - No tests yet - just infrastructure
   - Review: Code structure, API integration

2. **Iteration 2**: User Story 1 - Queueing Logic (T012-T016)
   - Implement queue detection, entry, and flush
   - Automated tests via testcontainer with IClock (WriteQueueTests.cs)
   - Test: Rapid writes, automatic flush, per-file independence (using IClock.Advance() for time simulation)
   - Deliverable: Basic queue functionality
   - Review: Queueing behavior, timestamp tracking, flush logic

3. **Iteration 3**: User Story 2 - Shutdown (T017-T019)
   - Add graceful shutdown with queue flushing
   - Automated tests via testcontainer with IClock (ShutdownFlushTests.cs)
   - Test: Queue flushing on disposal, in-flight flush coordination (using IClock.Advance() for timing)
   - Deliverable: Safe shutdown with no data loss
   - Review: Async disposal, queue flushing coordination

4. **Iteration 4**: User Story 3 - Client Disposal (T020-T021)
   - Verify client resource cleanup
   - Automated tests via testcontainer with IClock (DispatcherCleanupTests.cs)
   - Test: Disposal safety, ObjectDisposedException, resource leaks (using IClock for timeout simulation)
   - Deliverable: Proper resource management
   - Review: Disposal safety, resource leaks

5. **Iteration 5**: Polish (T023-T024)
   - Add comprehensive logging
   - Write PR description
   - Final test run
   - Deliverable: Production-ready code

### Parallelization Opportunities

After T001-T010 (foundational), these can be done in parallel:
- **Developer A**: T012-T016 (User Story 1)
- **Developer B**: T017-T019 (User Story 2)  
- **Developer C**: T020-T021 (User Story 3)
- **Integration**: T022-T023 (Polish)

### Testing Strategy

Tests use **TESTCONTAINERS** with **ICLOCK** for automated verification (zero manual testing needed):
- All tests use `IClock` interface from `RxDatasets.BackgroundServices` for time simulation
- No `Task.Delay()` or actual time waits - use `IClock.Advance(TimeSpan)` to fast-forward
- Tests complete in milliseconds (not seconds)
- Reference implementation: `tst/RxDatasets.AliasingTests/TrackingRxSchedulerClock.cs` shows IClock test patterns
- Integration tests with GCS emulator via testcontainer (gcloud-emulator or mock)
- Load tests for queue depth / concurrent operations
- Unit tests for queue state management
- Shutdown scenario tests (critical for Kubernetes)

**Test Files (Recommended, Automated via Testcontainer)**:
- tst/RxDatasets.StorageTests/GoogleCloudStorage/WriteQueueTests.cs
- tst/RxDatasets.StorageTests/GoogleCloudStorage/ShutdownFlushTests.cs
- tst/RxDatasets.StorageTests/GoogleCloudStorage/DispatcherCleanupTests.cs

All tests run against testcontainer instance; no manual testing required.

---

## Task Completion Checklist Format

Each task uses this format for clarity:
```
- [ ] [TaskID] [Optional: P] [Optional: Story] Description | file-path
```

Example:
- `- [ ] T001 Create QueuedWrite.cs | src/RxDatasets.StorageProvider.GoogleCloud/QueuedWrite.cs`
- `- [ ] T012 [P] [US1] Implement queue entry logic | src/RxDatasets.StorageProvider.GoogleCloud/WriteQueueManager.cs`

---

## Quality Gates

**Before Code Review**:
- [ ] All 10 setup/foundational tasks (T001-T010) complete
- [ ] No compilation errors
- [ ] All modified public methods have null/disposed checks
- [ ] _writeQueueManager properly initialized and disposed

**Before Merging User Story**:
- [ ] All story-specific tasks complete (T012-T016 for US1, etc.)
- [ ] All automated tests passing (via testcontainer)
- [ ] Test coverage includes: queue behavior, shutdown flushing, resource cleanup
- [ ] Logging verified working for all operations

**Before Final Merge**:
- [ ] All tasks T001-T023 complete
- [ ] Polish tasks done (logging, PR description)
- [ ] Code review approved
- [ ] All integration tests passing
- [ ] Performance validated (< 50ms response times verified)

---

## File Structure Reference

```
src/RxDatasets.StorageProvider.GoogleCloud/
├── GoogleCloudStorage.cs           [MODIFIED] T004-T009
├── QueuedWrite.cs                  [NEW] T001
├── FileWriteQueueState.cs           [NEW] T002
├── WriteQueueManager.cs             [NEW] T003
└── RxDatasets.StorageProvider.GoogleCloud.csproj [MODIFIED] T010

tst/RxDatasets.StorageTests/
└── GoogleCloudStorage/
    ├── WriteQueueTests.cs           [NEW] T012 (optional)
    ├── ShutdownFlushTests.cs        [NEW] T017 (optional)
    └── DispatcherCleanupTests.cs    [NEW] T020 (optional)

Repository Root/
└── PULL_REQUEST.md                  [NEW] T023
```

---

## Next Steps

1. **Setup Review**: Have code reviewer approve T001-T010 structure
2. **Parallel Development**: Assign T012-T021 to developers (can be done in parallel)
3. **Integration Testing**: After each iteration, run full test suite
4. **Documentation Review**: Review quickstart.md and API contract matches implementation
5. **Performance Testing**: Verify < 50ms response times with rapid writes
6. **Kubernetes Testing**: Test graceful shutdown with pod termination signals
7. **Merge**: Create PR with description from T023, get approval, merge to main

---

## Success Criteria Mapping

| Success Criterion | Related Tasks | Verification |
|---|---|---|
| SC-001: Sub-50ms returns | T012-T016 | Measure response time in T016 test |
| SC-002: No data loss on shutdown | T017-T019 | Verify writes in GCS after shutdown |
| SC-003: Zero resource leaks | T020-T021 | Profiling/disposal tests |
| SC-004: Flush within 2-3s | T014-T015 | Automatic in flush scheduling |
| SC-005: 100+ queued writes | T016 | Load test with many simultaneous writes |
| SC-006: Thread-safe operations | T012-T015 | Concurrent write tests in T012 |

---

## Assumptions & Notes

- All timestamps use `DateTimeOffset.UtcNow` (UTC only)
- Queue is per-file; different files have independent queues
- Queued writes are in-memory only (no persistence)
- Only the LAST write to a file persists if multiple queued writes exist for same file
- Existing rate limit retry logic (5x1.1s) is preserved for non-queued writes
- Public API signatures unchanged; queue is transparent implementation detail
- `IAsyncDisposable` required (not `IDisposable`) for async queue flushing
