# Data Model & Design: GCS Write Queue

**Feature**: 056-gcs-rate-limit-queue  
**Date**: 2025-12-22  
**Phase**: 1 - Design & Contracts

---

## Overview

The data model extends GoogleCloudStorage<T> with internal queue management. No new public entities are exposed; all queue infrastructure is internal implementation detail.

---

## Entity: QueuedWrite

**Purpose**: Represents a single pending write operation waiting to be flushed to GCS.

**Scope**: Internal to GoogleCloudStorage<T>; used within write queue only.

```csharp
/// <summary>
/// Represents a write operation queued for deferred execution to GCS.
/// </summary>
internal sealed class QueuedWrite
{
    /// <summary>
    /// The object key in GCS where data should be written.
    /// </summary>
    public string ObjectKey { get; }

    /// <summary>
    /// Serialized bytes to write. Null if source is a stream.
    /// </summary>
    public byte[]? Bytes { get; }

    /// <summary>
    /// Stream to write. Null if source is bytes.
    /// </summary>
    public Stream? Stream { get; }

    /// <summary>
    /// Whether this write uses stream source (true) or bytes source (false).
    /// </summary>
    public bool IsStreamSource { get; }

    /// <summary>
    /// Timestamp when this write was queued.
    /// </summary>
    public DateTimeOffset QueuedAt { get; }

    /// <summary>
    /// Sequence number for ordering within queue (for debugging/logging).
    /// </summary>
    public long SequenceNumber { get; }

    /// Constructor for bytes-based write
    public QueuedWrite(string objectKey, byte[] bytes, long sequenceNumber)
    {
        ObjectKey = objectKey;
        Bytes = bytes;
        Stream = null;
        IsStreamSource = false;
        QueuedAt = DateTimeOffset.UtcNow;
        SequenceNumber = sequenceNumber;
    }

    /// Constructor for stream-based write
    public QueuedWrite(string objectKey, Stream stream, long sequenceNumber)
    {
        ObjectKey = objectKey;
        Bytes = null;
        Stream = stream;
        IsStreamSource = true;
        QueuedAt = DateTimeOffset.UtcNow;
        SequenceNumber = sequenceNumber;
    }
}
```

**Attributes**:
- `ObjectKey` (string): The GCS object path - required
- `Bytes` (byte[] | null): The byte content to write
- `Stream` (Stream | null): The stream content to write  
- `IsStreamSource` (bool): Discriminator for bytes vs. stream
- `QueuedAt` (DateTimeOffset): Wall-clock time of queue entry
- `SequenceNumber` (long): Monotonic counter for ordering/diagnostics

**Validation Rules**:
- Exactly one of `Bytes` or `Stream` must be non-null
- `ObjectKey` must not be empty
- `SequenceNumber` must be >= 0

---

## Entity: FileWriteQueueState

**Purpose**: Maintains per-file queue metadata and state.

**Scope**: Internal to GoogleCloudStorage<T>; manages one file's queue lifecycle.

```csharp
/// <summary>
/// Manages queue state and metadata for writes to a specific GCS object.
/// </summary>
internal sealed class FileWriteQueueState
{
    /// <summary>
    /// The GCS object key this queue manages.
    /// </summary>
    public string ObjectKey { get; }

    /// <summary>
    /// FIFO queue of pending writes for this file.
    /// </summary>
    public Queue<QueuedWrite> PendingWrites { get; }

    /// <summary>
    /// Timestamp of the last confirmed write to GCS for this file.
    /// </summary>
    public DateTimeOffset? LastSuccessfulWrite { get; set; }

    /// <summary>
    /// Whether a flush operation is currently scheduled/in-progress for this file.
    /// </summary>
    public bool FlushScheduled { get; set; }

    /// <summary>
    /// Task representing the in-flight flush operation, if any.
    /// </summary>
    public Task? FlushTask { get; set; }

    /// <summary>
    /// Lock for coordinating queue access and flush operations.
    /// </summary>
    public ReaderWriterLockSlim Lock { get; }

    /// <summary>
    /// Initializes queue state for an object key.
    /// </summary>
    public FileWriteQueueState(string objectKey)
    {
        ObjectKey = objectKey;
        PendingWrites = new Queue<QueuedWrite>();
        LastSuccessfulWrite = null;
        FlushScheduled = false;
        FlushTask = null;
        Lock = new ReaderWriterLockSlim();
    }

    /// <summary>
    /// Checks if this file's queue is eligible for immediate flush
    /// (2+ seconds since last confirmed write).
    /// </summary>
    public bool IsFlushEligible()
    {
        if (LastSuccessfulWrite == null)
            return true; // No prior writes, flush immediately

        var elapsed = DateTimeOffset.UtcNow - LastSuccessfulWrite.Value;
        return elapsed.TotalSeconds >= 2.0;
    }

    /// <summary>
    /// Calculates milliseconds to wait before flush is eligible.
    /// </summary>
    public int GetMillisecondsUntilFlushEligible()
    {
        if (LastSuccessfulWrite == null)
            return 0;

        var elapsed = DateTimeOffset.UtcNow - LastSuccessfulWrite.Value;
        var remaining = TimeSpan.FromSeconds(2.0) - elapsed;
        return Math.Max(0, (int)remaining.TotalMilliseconds);
    }

    /// Cleanup resources
    public void Dispose()
    {
        Lock?.Dispose();
    }
}
```

**Attributes**:
- `ObjectKey` (string): Identifies which file this queue manages
- `PendingWrites` (Queue<QueuedWrite>): FIFO collection of queued writes
- `LastSuccessfulWrite` (DateTimeOffset? | null): Wall-clock time of last confirmed GCS write
- `FlushScheduled` (bool): Flag indicating if flush is in-progress or scheduled
- `FlushTask` (Task? | null): Reference to the async flush task (for awaiting)
- `Lock` (ReaderWriterLockSlim): Synchronization primitive for queue updates

**State Transitions**:
```
[Empty] --enqueue--> [HasPendingWrites] --flush-scheduled--> [Flushing] --complete--> [Empty]
         \                                                                            /
          `-------- (last write >= 2s) ---------> [Eligible] --flush-now--->[Flushing]
```

---

## Entity: WriteQueueManager

**Purpose**: Orchestrates all per-file queues and coordinates flushing across files.

**Scope**: Internal to GoogleCloudStorage<T>; manages queue lifecycle for entire storage instance.

```csharp
/// <summary>
/// Manages write queues for all GCS objects within a storage instance.
/// </summary>
internal sealed class WriteQueueManager
{
    private readonly ILogger<GoogleCloudStorage<T>> _logger;
    private readonly Func<QueuedWrite, Task> _flushAction; // Delegate to perform actual writes
    private readonly ConcurrentDictionary<string, FileWriteQueueState> _queues;
    private long _sequenceNumber = 0;
    private volatile bool _isDisposing = false;
    private readonly Task[] _activeTasks; // Track all flush tasks for disposal

    /// <summary>
    /// Initialize the queue manager.
    /// </summary>
    /// <param name="logger">Logger instance for diagnostics.</param>
    /// <param name="flushAction">Async delegate to execute actual write.</param>
    public WriteQueueManager(
        ILogger<GoogleCloudStorage<T>> logger,
        Func<QueuedWrite, Task> flushAction)
    {
        _logger = logger;
        _flushAction = flushAction;
        _queues = new ConcurrentDictionary<string, FileWriteQueueState>();
        _activeTasks = new Task[1024]; // Pre-allocate for up to 1024 concurrent flushes
    }

    /// <summary>
    /// Attempt to queue a write, or execute immediately if queue not needed.
    /// </summary>
    /// <returns>
    /// True if write was queued (client should return immediately);
    /// False if queue was bypassed (normal write path should execute).
    /// </returns>
    public bool TryQueueWrite(string objectKey, byte[] bytes)
    {
        var queueState = _queues.GetOrAdd(objectKey, k => new FileWriteQueueState(k));

        queueState.Lock.EnterReadLock();
        try
        {
            if (!queueState.IsFlushEligible())
            {
                // Last write was < 2 seconds ago, queue this write
                var queuedWrite = new QueuedWrite(
                    objectKey,
                    bytes,
                    Interlocked.Increment(ref _sequenceNumber));

                queueState.PendingWrites.Enqueue(queuedWrite);
                _logger.LogDebug(
                    "Write queued for {ObjectKey} (#{Sequence}). " +
                    "Last write was {SecondsSince:F1}s ago",
                    objectKey,
                    queuedWrite.SequenceNumber,
                    (DateTimeOffset.UtcNow - queueState.LastSuccessfulWrite!.Value).TotalSeconds);

                ScheduleFlush(queueState);
                return true; // Write was queued
            }
        }
        finally
        {
            queueState.Lock.ExitReadLock();
        }

        return false; // Queue not needed, execute normal write
    }

    /// <summary>
    /// Similar to TryQueueWrite but for stream-based writes.
    /// </summary>
    public bool TryQueueWrite(string objectKey, Stream stream)
    {
        var queueState = _queues.GetOrAdd(objectKey, k => new FileWriteQueueState(k));

        queueState.Lock.EnterReadLock();
        try
        {
            if (!queueState.IsFlushEligible())
            {
                var queuedWrite = new QueuedWrite(
                    objectKey,
                    stream,
                    Interlocked.Increment(ref _sequenceNumber));

                queueState.PendingWrites.Enqueue(queuedWrite);
                _logger.LogDebug(
                    "Stream write queued for {ObjectKey} (#{Sequence})",
                    objectKey,
                    queuedWrite.SequenceNumber);

                ScheduleFlush(queueState);
                return true;
            }
        }
        finally
        {
            queueState.Lock.ExitReadLock();
        }

        return false;
    }

    /// <summary>
    /// Notify manager of a successful write. Updates timestamp.
    /// </summary>
    public void MarkSuccessfulWrite(string objectKey)
    {
        if (_queues.TryGetValue(objectKey, out var queueState))
        {
            queueState.Lock.EnterWriteLock();
            try
            {
                queueState.LastSuccessfulWrite = DateTimeOffset.UtcNow;
            }
            finally
            {
                queueState.Lock.ExitWriteLock();
            }
        }
    }

    /// <summary>
    /// Schedules a flush operation for the given queue state.
    /// </summary>
    private void ScheduleFlush(FileWriteQueueState queueState)
    {
        if (queueState.FlushScheduled)
            return; // Already scheduled

        queueState.FlushScheduled = true;
        var delayMs = queueState.GetMillisecondsUntilFlushEligible();

        queueState.FlushTask = Task.Delay(delayMs)
            .ContinueWith(_ => ExecuteFlush(queueState))
            .Unwrap();
    }

    /// <summary>
    /// Execute all pending writes for a file queue.
    /// </summary>
    private async Task ExecuteFlush(FileWriteQueueState queueState)
    {
        queueState.Lock.EnterWriteLock();
        try
        {
            while (queueState.PendingWrites.Count > 0)
            {
                var queuedWrite = queueState.PendingWrites.Dequeue();

                try
                {
                    await _flushAction(queuedWrite);
                    queueState.LastSuccessfulWrite = DateTimeOffset.UtcNow;
                    _logger.LogDebug(
                        "Flushed queued write for {ObjectKey} (#{Sequence})",
                        queueState.ObjectKey,
                        queuedWrite.SequenceNumber);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(
                        ex,
                        "Failed to flush queued write for {ObjectKey} (#{Sequence})",
                        queueState.ObjectKey,
                        queuedWrite.SequenceNumber);
                    throw; // Propagate to caller
                }
            }

            queueState.FlushScheduled = false;
            queueState.FlushTask = null;
        }
        finally
        {
            queueState.Lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Flush all pending queues and dispose. Called during storage provider disposal.
    /// </summary>
    public async Task FlushAndDisposeAsync()
    {
        _isDisposing = true;

        // Wait for all in-flight flush tasks
        var tasks = _queues.Values
            .Where(q => q.FlushTask != null)
            .Select(q => q.FlushTask!)
            .ToList();

        if (tasks.Count > 0)
        {
            _logger.LogInformation("Waiting for {Count} in-flight flush(es) before shutdown", tasks.Count);
            await Task.WhenAll(tasks);
        }

        // Flush any remaining queues
        foreach (var queueState in _queues.Values)
        {
            if (queueState.PendingWrites.Count > 0)
            {
                _logger.LogInformation(
                    "Flushing {Count} remaining writes for {ObjectKey} during shutdown",
                    queueState.PendingWrites.Count,
                    queueState.ObjectKey);

                await ExecuteFlush(queueState);
            }
        }

        // Dispose all locks
        foreach (var queueState in _queues.Values)
        {
            queueState.Dispose();
        }

        _queues.Clear();
    }
}
```

**Key Methods**:
- `TryQueueWrite(objectKey, bytes)`: Queue or bypass based on timing
- `MarkSuccessfulWrite(objectKey)`: Update last-write timestamp
- `ScheduleFlush(queueState)`: Arrange async flush after delay
- `ExecuteFlush(queueState)`: Perform actual dequeue and writes
- `FlushAndDisposeAsync()`: Graceful shutdown with queue flushing

---

## Integration with GoogleCloudStorage<T>

**Changes to existing class**:

```csharp
public class GoogleCloudStorage<T> : IDiskStorage<T>, IAsyncDisposable
{
    private readonly WriteQueueManager _writeQueueManager;
    private volatile bool _disposed = false;

    public GoogleCloudStorage(...)
    {
        // ... existing code ...
        _writeQueueManager = new WriteQueueManager(_logger, FlushQueuedWriteAsync);
    }

    public async Task UploadBytes(string objectKey, byte[] bytes)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(GoogleCloudStorage<T>));

        // Attempt to queue if within rate limit window
        if (_writeQueueManager.TryQueueWrite(objectKey, bytes))
        {
            return; // Write was queued, client gets immediate response
        }

        // Otherwise proceed with normal write (existing logic)
        // ... rate limit retry loop ...
        _writeQueueManager.MarkSuccessfulWrite(objectKey);
    }

    public async Task UploadFileStream(string objectKey, Stream stream)
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(GoogleCloudStorage<T>));

        // Same pattern for stream uploads
        if (_writeQueueManager.TryQueueWrite(objectKey, stream))
        {
            return;
        }

        // ... existing write logic ...
        _writeQueueManager.MarkSuccessfulWrite(objectKey);
    }

    // Helper for executing queued writes
    private async Task FlushQueuedWriteAsync(QueuedWrite queuedWrite)
    {
        if (queuedWrite.IsStreamSource)
        {
            await UploadFileStream(queuedWrite.ObjectKey, queuedWrite.Stream!);
        }
        else
        {
            await UploadBytes(queuedWrite.ObjectKey, queuedWrite.Bytes!);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        // Flush and dispose queues
        await _writeQueueManager.FlushAndDisposeAsync();

        // Dispose Google Cloud client
        _storageClient?.Dispose();

        GC.SuppressFinalize(this);
    }
}
```

---

## Key Design Decisions

1. **Per-File Queues**: Each GCS object has its own independent queue, allowing concurrent operations on different files without serialization.

2. **Last-Write Tracking**: Timestamp updated on successful write completion, not queue entry, ensuring 2-second window is relative to confirmed GCS writes.

3. **Transparent Queueing**: Clients don't see queue; it's an internal optimization that preserves error semantics and ordering guarantees.

4. **Graceful Shutdown**: All pending writes flushed before client disposal; no data loss on shutdown.

5. **Thread Safety**: Minimal locking via `ReaderWriterLockSlim` - multiple readers can check timestamps simultaneously; exclusive lock only during flush.

---

## Validation Rules

### For QueuedWrite
- Either `Bytes` or `Stream` must be populated (not both, not neither)
- `ObjectKey` must not be null or empty string
- `SequenceNumber` must be positive

### For FileWriteQueueState  
- `IsFlushEligible()` returns true if no prior writes OR >= 2 seconds elapsed
- `GetMillisecondsUntilFlushEligible()` returns 0 if eligible, else milliseconds to wait
- Lock must be held when accessing `PendingWrites` or `LastSuccessfulWrite`

### For WriteQueueManager
- `TryQueueWrite()` can only be called from non-disposed state
- `FlushAndDisposeAsync()` must complete before storage provider disposal finalizes
- All timestamps use `DateTimeOffset.UtcNow` (UTC only, no local time)
