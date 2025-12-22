# Phase 0 Research: GCS Write Queue Implementation

**Feature**: 056-gcs-rate-limit-queue  
**Date**: 2025-12-22  
**Status**: Research Complete - All decisions documented

---

## Research Topics Investigated

### Topic 1: Thread-Safe Queue Implementation in C# .NET 8.0

**Decision**: Use `ConcurrentDictionary<string, Queue<(byte[] bytes, long position)>>` for per-file queuing with `ReaderWriterLockSlim` for flushing operations.

**Rationale**: 
- `ConcurrentDictionary` is purpose-built for thread-safe key-value operations without explicit locking for common cases
- `Queue<T>` is simple, ordered FIFO collection; wrapping in lock provides atomic dequeue/flush operations
- Avoids unnecessary lock contention on per-file level; each file's queue can be managed independently
- .NET 8.0 provides high-performance concurrent collections with good benchmarking data

**Alternatives considered**:
- `System.Collections.Concurrent.ConcurrentQueue<T>`: Single queue for all files - rejected because feature requires per-file queuing and last-write-time tracking
- `System.Threading.Channels`: More complex, designed for producer-consumer pipelines - overkill for this use case
- Custom lock-based dictionary: Would require manual lock management; `ConcurrentDictionary` is proven and tested
- `BlockingCollection<T>`: Not needed; feature doesn't require blocking dequeue operations

**Source**: [MSDN ConcurrentDictionary](https://docs.microsoft.com/en-us/dotnet/api/system.collections.concurrent.concurrentdictionary-2)

---

### Topic 2: Timing Mechanism for 2-Second Flush Window

**Decision**: Use `DateTimeOffset.UtcNow` for tracking last-write timestamps; flush timer via `Task.Delay()` in async background task per file.

**Rationale**:
- `DateTimeOffset.UtcNow` provides wall-clock time suitable for GCS rate limit detection (not dependent on CPU-bound timings)
- `Task.Delay()` is lightweight, doesn't block threads, integrates naturally with async/await patterns already in GoogleCloudStorage
- Each file's pending writes trigger a single background flush task that waits 2 seconds then processes the queue
- Simpler than `System.Timers.Timer` or `PeriodicTimer`; doesn't require maintaining timer state across operations

**Alternatives considered**:
- `System.Timers.Timer`: Requires disposal management; callback threading complications - rejected
- `PeriodicTimer` (.NET 6+): Would flush repeatedly at intervals; feature needs one-time flush after 2-second delay - rejected
- `Stopwatch` + polling: Less precise; would require background threads constantly checking elapsed time - rejected
- `Thread.Sleep()`: Blocks thread; GoogleCloudStorage is async-first - rejected

**Source**: Current GoogleCloudStorage.cs uses async patterns; Task.Delay() is standard async delay mechanism

---

### Topic 3: Graceful Shutdown & Disposal Pattern

**Decision**: Implement async `Dispose()` override; track flush-in-progress flag and wait for it before disposing Google Cloud client.

**Rationale**:
- `IAsyncDisposable` interface allows async cleanup in .NET - can await background flush tasks
- Flag-based state machine prevents new writes after Dispose() starts
- Two-phase disposal: (1) stop accepting writes, flush all queues, (2) dispose Google Cloud client
- Kubernetes-compatible: Pods receive graceful shutdown signal; provider has time to flush before hard termination

**Alternatives considered**:
- Synchronous `Dispose()`: Would block indefinitely if flush takes time; breaks async patterns - rejected
- Fire-and-forget flush: Loses data on shutdown - violates success criterion SC-002 - rejected
- `using` pattern without tracking state: No way to prevent writes after disposal starts - rejected

**Source**: [IAsyncDisposable - Microsoft Docs](https://docs.microsoft.com/en-us/dotnet/standard/garbage-collection/implementing-disposeasync)

---

### Topic 4: Rate Limit Error Detection & Retry Strategy

**Decision**: Keep existing retry logic in `UploadBytes()` for 429 errors; queue absorption happens BEFORE retry loop starts.

**Rationale**:
- Current code already detects GCS 429 (rate limit) and retries with 1.1s delay
- Queue mechanism is transparent layer; rate limit detection stays as-is
- When rate limit is detected, subsequent writes for same file are queued instead of blocking
- Flush waits full 2 seconds from last successful write before attempting queued writes
- Failures during flush are logged; errors propagate to caller just like non-queued writes

**Alternatives considered**:
- Suppress rate limit errors and silently retry: Would hide problems - rejected
- Increase delay between retries: Current 5x1.1s=5.5s is fine; queue optimization is better - rejected
- Write queue as optional feature: User requested it for core behavior - rejected

**Source**: GoogleCloudStorage.cs line 204-223 (UploadBytes rate limit handling)

---

### Topic 5: Public API Impact & Backward Compatibility

**Decision**: No changes to public methods. Queue is purely internal. Public API remains: UploadBytes, DownloadBytes, DeleteFile, DeleteFolder, GetObjectContentLength, ListObjects.

**Rationale**:
- Feature requirement: "Existing public API signatures should remain compatible"
- Queue is implementation detail (like connection pooling is in HTTP clients)
- Clients don't need to know about queueing; it happens transparently on their behalf
- Tests can verify queue behavior internally without exposing it

**Constraints**: 
- Return times from `UploadBytes()` may be faster (good), not slower (still acceptable - queued writes flush automatically)
- Error handling semantics maintained - failures still propagate to caller
- No new Task<> return values; async behavior already existed

**Source**: Feature spec constraints section

---

### Topic 6: Testing Strategy for Concurrent Queue Operations

**Decision**: Use `xUnit` with `Task.Run()` to simulate concurrent uploads; verify queue state before/after flush windows.

**Rationale**:
- Existing test framework is xUnit (used in RxDatasets.Tests)
- Use `TaskScheduler.Current` to force synchronous execution in tests OR `async Task` tests with `await Task.WhenAll()`
- Verify internal queue state by checking file timestamps or via internal test helpers
- Mock Google Cloud client or use in-memory storage for unit tests; integration tests use emulator

**Alternatives considered**:
- Manual threading: xUnit async support is cleaner - rejected
- Rely on production testing: Feature requires reliable shutdown; unit tests essential - rejected
- No concurrent tests: Race conditions would only appear in production - rejected

**Source**: xUnit async testing patterns documented in test project

---

### Topic 7: Storage of Last-Write Timestamps

**Decision**: Maintain `Dictionary<string, DateTimeOffset>` alongside write queues; update on successful flush, not on queue add.

**Rationale**:
- Timestamp tracks actual GCS write time (success point), not when write was requested (queue add time)
- This ensures 2-second window is relative to GCS confirmation, matching user intent
- Atomic update: timestamp + flush happen together or not at all
- Supports the feature logic: "we KNOW it was written less than 2 seconds ago" (emphasis on confirmed write)

**Alternatives considered**:
- Update on queue entry: Would restart 2-second window with each queue add - breaks feature intent - rejected
- Track first-queue-entry-time: Complex logic; doesn't match "when we KNOW it was written" - rejected
- Implicit tracking from file object metadata: Not reliable; GCS might have clock skew - rejected

**Source**: Feature spec user story 1, acceptance scenario 2

---

### Topic 8: Lock Granularity to Minimize Contention

**Decision**: Use per-file locking (via `ReaderWriterLockSlim` per key) rather than global lock; read-lock for checking timestamp, write-lock for flushing.

**Rationale**:
- Per-file locks allow concurrent operations on different files (DeltaInfo vs. ProcessedVersions)
- `ReaderWriterLockSlim` allows multiple readers (checking timestamp) without contention
- Only exclusive write-lock during flush operation (brief window)
- System can handle "100 or more queued writes across multiple files" (SC-005) without serialization

**Alternatives considered**:
- Single global `object` lock: Simple but serializes all file operations - rejected due to performance goal
- No explicit locking (rely on ConcurrentDictionary): Flush operation requires atomic dequeue+timestamp - rejected
- `SemaphoreSlim` per file: Lighter weight than ReaderWriterLockSlim but no reader benefit - acceptable but ReaderWriterLockSlim is proven pattern

**Source**: .NET concurrent programming best practices

---

## Implementation Approach Summary

### Queue Structure
- Per-file queue: `ConcurrentDictionary<objectKey, Queue<(byte[] bytes, long position)>>`
- Per-file metadata: `Dictionary<objectKey, (DateTimeOffset lastWrite, bool flushScheduled)>`
- Per-file locking: `Dictionary<objectKey, ReaderWriterLockSlim>`

### Write Path
1. Client calls `UploadBytes(objectKey, bytes)` or `UploadFileStream(objectKey, stream)`
2. Check last write timestamp for this file
3. If last write < 2 seconds ago: queue the write, schedule flush, return immediately
4. If last write >= 2 seconds ago: proceed with normal GCS upload (no queue needed)
5. Normal upload path handles rate limit retries as before

### Flush Path
1. Scheduled flush task waits for 2-second window
2. Dequeue all pending writes for that file
3. Execute dequeue in order using existing `UploadBytes()` logic
4. Update last-write timestamp on success
5. If multiple writes accumulated, only the last one is actually written (earlier ones are overwritten)
6. All errors logged; propagate if they occur

### Disposal Path
1. `Dispose()` called
2. Set flag to reject new writes
3. Wait for all in-flight flush tasks to complete (using `Task.WhenAll()`)
4. Flush any remaining queued writes synchronously
5. Dispose Google Cloud client
6. Dispose all `ReaderWriterLockSlim` instances

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|-----------|
| Race condition: write + dispose simultaneous | Low | High | Atomic state check + lock ensures serialize |
| Memory leak: abandoned TimerTask | Low | Medium | Explicit task tracking; dispose cancels pending |
| Clock skew: timestamps appear wrong | Very Low | Low | Use UtcNow (server clock); GCS is synchronized |
| Lost writes on crash before flush | Medium | High | Feature is improvement; not worse than current |
| Queue grows unbounded | Very Low | Medium | Flush removes all entries; only bounds are memory |

---

## Next Phase

All research questions answered. Proceed to Phase 1 (Design & Contracts) to finalize:
- Data-model.md with WriteQueueEntry and WriteQueueManager classes
- API contracts ensuring backward compatibility
- Quickstart.md with usage examples
