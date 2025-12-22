using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace RxDatasets.StorageProvider.GoogleCloud
{
    /// <summary>
    /// Manages write queuing for Google Cloud Storage to handle the 1-write-per-second rate limit
    /// on individual objects. Maintains per-file queues with automatic flushing after 2-second windows.
    /// 
    /// Thread-safe. Uses ReaderWriterLockSlim per file for efficient concurrent access patterns.
    /// </summary>
    internal sealed class WriteQueueManager
    {
        private readonly ILogger _logger;
        private readonly Func<QueuedWrite, Task> _flushAction;
        private readonly ConcurrentDictionary<string, FileWriteQueueState> _queues = new();
        private long _sequenceNumber;
        private volatile bool _isDisposing;

        /// <summary>
        /// Creates a new write queue manager.
        /// </summary>
        /// <param name="logger">Logger for queue operations.</param>
        /// <param name="flushAction">Async delegate called to flush a single queued write.</param>
        public WriteQueueManager(ILogger logger, Func<QueuedWrite, Task> flushAction)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _flushAction = flushAction ?? throw new ArgumentNullException(nameof(flushAction));
        }

        /// <summary>
        /// Attempts to queue a write. Returns true if queued, false if write should proceed normally.
        /// </summary>
        public bool TryQueueWrite(string objectKey, byte[] bytes)
        {
            if (_isDisposing)
                return false;

            var queue = _queues.GetOrAdd(objectKey, key => new FileWriteQueueState(key));

            queue.Lock.EnterReadLock();
            try
            {
                var now = DateTimeOffset.UtcNow;

                // Check if queue is eligible to be flushed (last write > 2 seconds ago)
                if (queue.LastSuccessfulWrite != DateTimeOffset.MinValue &&
                    now.Subtract(queue.LastSuccessfulWrite).TotalSeconds >= 2.0)
                {
                    // Not queuing - write now
                    _logger.LogDebug("Write eligible for {ObjectKey}. Last successful write was {SecondsSince}s ago",
                        objectKey, now.Subtract(queue.LastSuccessfulWrite).TotalSeconds);
                    return false;
                }

                // Will queue this write
            }
            finally
            {
                queue.Lock.ExitReadLock();
            }

            // Acquire write lock to queue the write
            queue.Lock.EnterWriteLock();
            try
            {
                var sequenceNum = Interlocked.Increment(ref _sequenceNumber);
                var queuedWrite = new QueuedWrite(objectKey, bytes, DateTimeOffset.UtcNow, sequenceNum);
                queue.PendingWrites.Enqueue(queuedWrite);

                var now = DateTimeOffset.UtcNow;
                var secondsSinceLastWrite = queue.LastSuccessfulWrite == DateTimeOffset.MinValue
                    ? 0.0
                    : now.Subtract(queue.LastSuccessfulWrite).TotalSeconds;

                _logger.LogDebug(
                    "Write queued for {ObjectKey} (Sequence: {SequenceNumber}). Last successful write was {SecondsSince:F2}s ago",
                    objectKey, sequenceNum, secondsSinceLastWrite);

                ScheduleFlush(queue, objectKey);
                return true;
            }
            finally
            {
                queue.Lock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Attempts to queue a write from a stream. Returns true if queued, false if write should proceed normally.
        /// </summary>
        public bool TryQueueWrite(string objectKey, Stream stream)
        {
            if (_isDisposing)
                return false;

            var queue = _queues.GetOrAdd(objectKey, key => new FileWriteQueueState(key));

            queue.Lock.EnterReadLock();
            try
            {
                var now = DateTimeOffset.UtcNow;

                // Check if queue is eligible to be flushed (last write > 2 seconds ago)
                if (queue.LastSuccessfulWrite != DateTimeOffset.MinValue &&
                    now.Subtract(queue.LastSuccessfulWrite).TotalSeconds >= 2.0)
                {
                    // Not queuing - write now
                    _logger.LogDebug("Stream write eligible for {ObjectKey}. Last successful write was {SecondsSince}s ago",
                        objectKey, now.Subtract(queue.LastSuccessfulWrite).TotalSeconds);
                    return false;
                }

                // Will queue this write
            }
            finally
            {
                queue.Lock.ExitReadLock();
            }

            // Acquire write lock to queue the write
            queue.Lock.EnterWriteLock();
            try
            {
                var sequenceNum = Interlocked.Increment(ref _sequenceNumber);
                var queuedWrite = new QueuedWrite(objectKey, stream, DateTimeOffset.UtcNow, sequenceNum);
                queue.PendingWrites.Enqueue(queuedWrite);

                var now = DateTimeOffset.UtcNow;
                var secondsSinceLastWrite = queue.LastSuccessfulWrite == DateTimeOffset.MinValue
                    ? 0.0
                    : now.Subtract(queue.LastSuccessfulWrite).TotalSeconds;

                _logger.LogDebug(
                    "Stream write queued for {ObjectKey} (Sequence: {SequenceNumber}). Last successful write was {SecondsSince:F2}s ago",
                    objectKey, sequenceNum, secondsSinceLastWrite);

                ScheduleFlush(queue, objectKey);
                return true;
            }
            finally
            {
                queue.Lock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Marks a write as successful, updating the timestamp for the object's queue.
        /// </summary>
        public void MarkSuccessfulWrite(string objectKey)
        {
            if (!_queues.TryGetValue(objectKey, out var queue))
                return;

            queue.Lock.EnterWriteLock();
            try
            {
                queue.LastSuccessfulWrite = DateTimeOffset.UtcNow;
            }
            finally
            {
                queue.Lock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Schedules a flush task if one isn't already scheduled for this queue.
        /// Must be called within a write-lock context on the queue.
        /// </summary>
        private void ScheduleFlush(FileWriteQueueState queue, string objectKey)
        {
            if (queue.FlushScheduled)
                return;

            queue.FlushScheduled = true;

            var now = DateTimeOffset.UtcNow;
            var delayMs = queue.GetMillisecondsUntilFlushEligible(now);

            if (delayMs < 0)
                delayMs = 0;

            _logger.LogDebug("Flush scheduled for {ObjectKey}, delay: {DelayMs}ms", objectKey, delayMs);

            var flushTask = Task.Delay(delayMs).ContinueWith(async _ =>
            {
                await ExecuteFlush(queue, objectKey);
            }).Unwrap();

            queue.FlushTask = flushTask;
        }

        /// <summary>
        /// Executes a flush for a specific queue, writing all pending writes in order.
        /// </summary>
        private async Task ExecuteFlush(FileWriteQueueState queue, string objectKey)
        {
            queue.Lock.EnterWriteLock();
            try
            {
                // Dequeue all pending writes
                while (queue.PendingWrites.Count > 0)
                {
                    var queuedWrite = queue.PendingWrites.Dequeue();

                    try
                    {
                        // Execute the flush action (this will call GoogleCloudStorage's actual write logic)
                        await _flushAction(queuedWrite);

                        _logger.LogDebug("Flushed queued write for {ObjectKey} (Sequence: {SequenceNumber})",
                            objectKey, queuedWrite.SequenceNumber);

                        // Mark as successful
                        queue.LastSuccessfulWrite = DateTimeOffset.UtcNow;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex,
                            "Failed to flush queued write for {ObjectKey} (Sequence: {SequenceNumber}): {ExceptionMessage}",
                            objectKey, queuedWrite.SequenceNumber, ex.Message);
                        throw;
                    }
                }

                // Reset flush state
                queue.FlushScheduled = false;
                queue.FlushTask = null;
            }
            finally
            {
                queue.Lock.ExitWriteLock();
            }
        }

        /// <summary>
        /// Flushes all pending writes and disposes the manager.
        /// Must be called during shutdown to ensure no data loss.
        /// </summary>
        public async Task FlushAndDisposeAsync()
        {
            _isDisposing = true;

            // Collect all in-flight flush tasks
            var flushTasks = new List<Task>();
            foreach (var queue in _queues.Values)
            {
                queue.Lock.EnterReadLock();
                try
                {
                    if (queue.FlushTask != null)
                    {
                        flushTasks.Add(queue.FlushTask);
                    }
                }
                finally
                {
                    queue.Lock.ExitReadLock();
                }
            }

            // Wait for all in-flight flushes
            if (flushTasks.Count > 0)
            {
                _logger.LogInformation(
                    "Waiting for {Count} in-flight flush(es) before shutdown",
                    flushTasks.Count);

                try
                {
                    await Task.WhenAll(flushTasks);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "One or more in-flight flush tasks failed during shutdown");
                }
            }

            // Flush all remaining pending writes from each queue
            foreach (var kvp in _queues)
            {
                var objectKey = kvp.Key;
                var queue = kvp.Value;

                queue.Lock.EnterReadLock();
                try
                {
                    if (queue.PendingWrites.Count > 0)
                    {
                        _logger.LogInformation(
                            "Flushing {Count} remaining writes for {ObjectKey} during shutdown",
                            queue.PendingWrites.Count, objectKey);
                    }
                }
                finally
                {
                    queue.Lock.ExitReadLock();
                }

                // Execute flush (acquires write lock internally)
                try
                {
                    await ExecuteFlush(queue, objectKey);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to flush remaining writes for {ObjectKey} during shutdown", objectKey);
                }
            }

            // Clean up all locks
            foreach (var queue in _queues.Values)
            {
                queue.Dispose();
            }

            _queues.Clear();
        }
    }
}
