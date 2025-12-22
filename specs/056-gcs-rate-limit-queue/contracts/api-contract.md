# API Contracts: GCS Write Queue Feature

**Feature**: 056-gcs-rate-limit-queue  
**Date**: 2025-12-22  
**Phase**: 1 - Design & Contracts

---

## Overview

This feature introduces **zero breaking changes** to the public API. All queue management is internal implementation detail. The `IDiskStorage<T>` interface remains unchanged.

---

## Public API (No Changes)

All existing methods retain their signatures and semantics:

```csharp
namespace RxDatasets.StorageProvider.GoogleCloud
{
    public class GoogleCloudStorage<T> : IDiskStorage<T>, IAsyncDisposable
    {
        // ✓ UNCHANGED - Public methods
        public Task UploadBytes(string objectKey, byte[] bytes) { ... }
        public Task UploadFileStream(string objectKey, Stream stream) { ... }
        public Task DeleteFile(string objectKey) { ... }
        public Task DeleteFolder(string prefix) { ... }
        public Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey) { ... }
        public Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey, long position, int length) { ... }
        public Task DownloadWithCallBack(string objectKey, Action<Stream> callback) { ... }
        public Task<long> GetObjectContentLength(string objectKey) { ... }
        public Task<List<(string ObjectKey, DateTimeOffset DateTime)>> ListObjects(string prefix) { ... }
        
        // ✓ CHANGED TYPE - Existing property now supports IAsyncDisposable pattern
        public (DateTimeOffset ObtainedAt, Exception? Exception) LastResult { get; }

        // ✓ NEW - Async disposal (replaces synchronous Dispose)
        public async ValueTask DisposeAsync() { ... }
    }

    // ✓ UNCHANGED - Interface contract
    public interface IDiskStorage<T>
    {
        (DateTimeOffset ObtainedAt, Exception? Exception) LastResult { get; }
        Task UploadBytes(string objectKey, byte[] bytes);
        Task UploadFileStream(string objectKey, Stream stream);
        Task DeleteFile(string objectKey);
        Task DeleteFolder(string prefix);
        Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey);
        Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey, long position, int length);
        Task DownloadWithCallBack(string objectKey, Action<Stream> callback);
        Task<long> GetObjectContentLength(string objectKey);
        Task<List<(string ObjectKey, DateTimeOffset DateTime)>> ListObjects(string prefix);
    }
}
```

---

## Behavioral Changes (Observable)

### Return Time Improvement

**Old Behavior** (5.5+ second wait on rate limit):
```
Client calls UploadBytes()
  → Rate limit hit
  → Retry 1.1s later (attempt 2)
  → Retry 1.1s later (attempt 3)
  → ...up to 5 attempts total...
  → UploadBytes() returns (5.5+ seconds later)
```

**New Behavior** (immediate return with queueing):
```
Client calls UploadBytes()
  → Last write < 2 seconds ago
  → Write queued internally
  → UploadBytes() returns immediately (< 50ms)
  → Scheduled flush executes 2 seconds later
  → Writes submitted to GCS
```

### Error Semantics (Preserved)

If a write fails during queue flush, the error is:
- Logged with full context (file path, sequence number, etc.)
- Traced to original request metadata if available
- Handled same way as direct write failures would be

**Error handling is transparent**: Clients cannot distinguish between a direct write failure and a queued write flush failure, nor should they need to.

---

## Internal Contracts (Implementation Detail)

These types are internal but documented here for clarity:

### Internal Type: `QueuedWrite`

Represents a pending write operation.

```csharp
internal sealed class QueuedWrite
{
    public string ObjectKey { get; }           // GCS object path
    public byte[]? Bytes { get; }              // Bytes content (if bytes-based write)
    public Stream? Stream { get; }             // Stream content (if stream-based write)
    public bool IsStreamSource { get; }        // Discriminator
    public DateTimeOffset QueuedAt { get; }    // When queued
    public long SequenceNumber { get; }        // Ordering information
}
```

### Internal Type: `FileWriteQueueState`

Manages state for one GCS object's queue.

```csharp
internal sealed class FileWriteQueueState
{
    public string ObjectKey { get; }                        // Identifies which file
    public Queue<QueuedWrite> PendingWrites { get; }       // FIFO queue
    public DateTimeOffset? LastSuccessfulWrite { get; set; } // Last confirmed write time
    public bool FlushScheduled { get; set; }               // Is flush in progress?
    public Task? FlushTask { get; set; }                   // Handle to async flush
    public ReaderWriterLockSlim Lock { get; }             // Synchronization
    
    public bool IsFlushEligible()                          // >= 2 seconds since last write?
    public int GetMillisecondsUntilFlushEligible()        // How long to wait?
    public void Dispose()                                  // Cleanup
}
```

### Internal Type: `WriteQueueManager`

Orchestrates all per-file queues.

```csharp
internal sealed class WriteQueueManager
{
    public bool TryQueueWrite(string objectKey, byte[] bytes)     // Queue bytes or bypass
    public bool TryQueueWrite(string objectKey, Stream stream)    // Queue stream or bypass
    public void MarkSuccessfulWrite(string objectKey)            // Update timestamp after write
    public async Task FlushAndDisposeAsync()                      // Graceful shutdown
}
```

---

## Concurrency Contract

### Thread Safety Guarantees

1. **Multiple clients can call `UploadBytes()` concurrently**: Queue manager handles serialization
2. **Writes to different files don't block each other**: Per-file queueing enables parallelism
3. **Shutdown waits for all pending flushes**: No data loss on `DisposeAsync()`
4. **No race conditions on timestamp tracking**: `ReaderWriterLockSlim` prevents TOCTOU bugs

### Not Thread-Safe

- Calling `UploadBytes()` after `DisposeAsync()` throws `ObjectDisposedException`
- Creating multiple GoogleCloudStorage instances for same bucket is not recommended (separate connections)

---

## Error Handling Contract

### Errors Preserved

All errors that would have occurred without queueing still occur:

| Error Scenario | Old Behavior | New Behavior |
|---|---|---|
| Invalid object key | Throws immediately | Throws immediately |
| GCS permission denied | Throws after retries | Queued, throws on flush |
| Network timeout | Throws after retries | Queued, throws on flush |
| Bucket not found | Throws immediately (initialization) | Same (init unchanged) |
| Disposed storage provider | Throws `ObjectDisposedException` | Throws `ObjectDisposedException` |

### New Error Scenario

- **Queued write lost due to crash before flush**: This is mitigated by Kubernetes graceful shutdown signal; applications have ~30s to flush queues.

---

## Logging Contract

All queue operations are logged at appropriate levels:

| Operation | Level | Information |
|---|---|---|
| Write queued | DEBUG | object key, sequence number, time since last write |
| Flush started | DEBUG | object key, number of pending writes |
| Flush completed | DEBUG | object key, sequence number of flushed write |
| Flush failed | WARNING | object key, sequence number, error details |
| Shutdown flushing | INFO | count of in-flight flushes, count of remaining writes |

---

## Timing Contract

### Guaranteed Timing Behaviors

| Scenario | Guarantee |
|---|---|
| Direct write (no queue) | Executes immediately; existing 5x1.1s retry logic applies |
| Queued write | Returns to caller in < 50ms |
| Queue flush delay | Scheduled for 2.0 second window (±50ms due to scheduling) |
| Multiple queued writes | Flushed in FIFO order; only last write persists to GCS if multiple writes to same key |
| Shutdown flush | Completes before `DisposeAsync()` returns |

### Non-Guaranteed Timing Behaviors

- Exact GCS write time (depends on network, GCS processing)
- Flush duration (depends on file size, network)
- Queue growth rate (depends on write frequency, last-write timing)

---

## Compatibility Notes

### For Kubernetes Deployments

The graceful shutdown contract requires:
1. Pod receives SIGTERM signal
2. Application calls `DisposeAsync()` on storage provider
3. Queue manager flushes pending writes (typically < 2 seconds)
4. Google Cloud client disposed
5. Application terminates (before SIGKILL timeout, typically 30s)

This flow is safe and recommended for all production deployments.

### For Long-Running Applications

No changes required; queue management is transparent. Memory usage remains low (in-memory queue, bounded by write frequency and 2-second flush window).

### For Testing

- Unit tests can verify queue state via internal reflection if needed
- Mock Google Cloud client to test flush logic in isolation
- Integration tests can use GCS emulator (currently supported)
- No test changes required for existing tests

---

## Migration Guide

### For Existing Code

**Zero migration required.** All existing code continues to work unchanged.

Example: Existing code
```csharp
var storage = new GoogleCloudStorage<DatasetVersion>(logger, options);
await storage.UploadBytes("metadata/deltainfo", data);
// Still works exactly the same way; may now complete faster!
```

### For New Code

No new APIs to adopt. Code written today against `IDiskStorage<T>` interface automatically benefits from queueing optimization.

### Disposal Pattern Update (Recommended)

Old pattern:
```csharp
using (var storage = new GoogleCloudStorage<T>(logger, options))
{
    await storage.UploadBytes(...);
}
```

New pattern (more explicit):
```csharp
var storage = new GoogleCloudStorage<T>(logger, options);
try
{
    await storage.UploadBytes(...);
}
finally
{
    await storage.DisposeAsync(); // Async disposal with queue flushing
}
```

Or using `IAsyncDisposable`:
```csharp
await using var storage = new GoogleCloudStorage<T>(logger, options);
await storage.UploadBytes(...);
// Automatically calls DisposeAsync() on scope exit
```
