using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using RxDatasets.BackgroundServices;

namespace RxDatasets.StorageProvider.GoogleCloud
{
    /// <summary>
    /// Manages write queuing for Google Cloud Storage to handle the 1-write-per-second rate limit
    /// on individual objects. Maintains per-file queues with automatic flushing after 2-second windows.
    /// 
    /// Thread-safe. Uses SemaphoreSlim per file for efficient concurrent access patterns.
    /// </summary>
    internal sealed class WriteQueueManager
    {
        private readonly ILogger _logger;
        private readonly Func<QueuedWrite, Task> _flushAction;
        private readonly IClock _clock;
        private readonly ConcurrentDictionary<string, FileWriteQueueState> _queues = new();
        private long _sequenceNumber;
        private volatile bool _isDisposing;

        /// <summary>
        /// Creates a new write queue manager.
        /// </summary>
        /// <param name="logger">Logger for queue operations.</param>
        /// <param name="flushAction">Async delegate called to flush a single queued write.</param>
        /// <param name="clock">Clock for time-based decisions (uses IClock for testability).</param>
        public WriteQueueManager(ILogger logger, Func<QueuedWrite, Task> flushAction, IClock clock)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _flushAction = flushAction ?? throw new ArgumentNullException(nameof(flushAction));
            _clock = clock ?? throw new ArgumentNullException(nameof(clock));
        }

        /// <summary>
        /// Attempts to queue a write. Returns true if queued, false if write should proceed normally.
        /// </summary>
        public async Task<bool> TryQueueWriteAsync(string objectKey, byte[] bytes)
        {
            if (_isDisposing)
                return false;

            var queue = _queues.GetOrAdd(objectKey, key => new FileWriteQueueState(key));

            await queue.Lock.WaitAsync();
            try
            {
                var lastWrite = queue.LastSuccessfulWrite;
                if (lastWrite == DateTimeOffset.MinValue)
                {
                    return false; // No prior writes to compare against
                }

                var now = _clock.UtcNow;
                var secondsSinceLastWrite = (now - lastWrite).TotalSeconds;

                if (secondsSinceLastWrite >= 2.0)
                {
                    _logger.LogDebug("Bypassing queue for {ObjectKey}; last write was {SecondsSince:F2}s ago",
                        objectKey, secondsSinceLastWrite);
                    return false;
                }

                var sequenceNum = Interlocked.Increment(ref _sequenceNumber);
                var queuedWrite = new QueuedWrite(objectKey, bytes, now, sequenceNum);
                queue.PendingWrite = queuedWrite;

                _logger.LogDebug(
                    "Write queued for {ObjectKey} (Sequence: {SequenceNumber}). Last successful write was {SecondsSince:F2}s ago",
                    objectKey, sequenceNum, secondsSinceLastWrite);

                ScheduleFlush(queue, objectKey);
                return true;
            }
            finally
            {
                queue.Lock.Release();
            }
        }

        /// <summary>
        /// Attempts to queue a write from a stream. Returns true if queued, false if write should proceed normally.
        /// </summary>
        public Task<bool> TryQueueWriteAsync(string objectKey, Stream stream)
        {
            _logger.LogDebug("Stream writes bypass the queue for {ObjectKey}", objectKey);
            return Task.FromResult(false);
        }

        /// <summary>
        /// Marks a write as successful, updating the timestamp for the object's queue.
        /// </summary>
        public async Task MarkSuccessfulWriteAsync(string objectKey)
        {
            if (!_queues.TryGetValue(objectKey, out var queue))
                return;

            await queue.Lock.WaitAsync();
            try
            {
                queue.LastSuccessfulWrite = _clock.UtcNow;
            }
            finally
            {
                queue.Lock.Release();
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

            var now = _clock.UtcNow;
            var delayMs = queue.GetMillisecondsUntilFlushEligible(now);

            if (delayMs < 0)
                delayMs = 0;

            _logger.LogDebug("Flush scheduled for {ObjectKey}, delay: {DelayMs}ms", objectKey, delayMs);

            var delay = TimeSpan.FromMilliseconds(delayMs);
            queue.FlushTask = Task.Run(() => ExecuteFlushAfterDelay(queue, objectKey, delay));
        }

        private Task ExecuteFlushAfterDelay(FileWriteQueueState queue, string objectKey, TimeSpan delay)
        {
            return PerformFlushAsync();

            async Task PerformFlushAsync()
            {
                if (delay > TimeSpan.Zero)
                {
                    await _clock.Delay(delay);
                }
                else
                {
                    // Ensure flush happens outside the scheduling lock when no delay is required.
                    await Task.Yield();
                }

                await ExecuteFlush(queue, objectKey);
            }
        }

        /// <summary>
        /// Executes a flush for a specific queue, writing the pending write if any.
        /// </summary>
        private async Task ExecuteFlush(FileWriteQueueState queue, string objectKey)
        {
            await queue.Lock.WaitAsync();
            try
            {
                // Flush the single pending write (if any)
                if (queue.PendingWrite != null)
                {
                    var queuedWrite = queue.PendingWrite;
                    queue.PendingWrite = null;

                    try
                    {
                        // Execute the flush action (this will call GoogleCloudStorage's actual write logic)
                        await _flushAction(queuedWrite);

                        _logger.LogDebug("Flushed queued write for {ObjectKey} (Sequence: {SequenceNumber})",
                            objectKey, queuedWrite.SequenceNumber);

                        // Mark as successful
                        queue.LastSuccessfulWrite = _clock.UtcNow;
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
                queue.Lock.Release();
            }
        }

        /// <summary>
        /// Flushes all pending and in-flight writes without disposing the manager.
        /// The queue remains usable after this call.
        /// </summary>
        public async Task FlushAsync()
        {
            // Collect all in-flight flush tasks
            var flushTasks = new List<Task>();
            foreach (var queue in _queues.Values)
            {
                await queue.Lock.WaitAsync();
                try
                {
                    if (queue.FlushTask != null)
                    {
                        flushTasks.Add(queue.FlushTask);
                    }
                }
                finally
                {
                    queue.Lock.Release();
                }
            }

            // Wait for all in-flight flushes
            if (flushTasks.Count > 0)
            {
                _logger.LogInformation(
                    "Waiting for {Count} in-flight flush(es)",
                    flushTasks.Count);

                try
                {
                    await Task.WhenAll(flushTasks);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "One or more in-flight flush tasks failed");
                }
            }

            // Flush all remaining pending writes from each queue in parallel
            var finalFlushTasks = new List<Task>();
            foreach (var kvp in _queues)
            {
                var objectKey = kvp.Key;
                var queue = kvp.Value;

                finalFlushTasks.Add(Task.Run(async () =>
                {
                    bool hadPendingWrite = false;
                    await queue.Lock.WaitAsync();
                    try
                    {
                        if (queue.PendingWrite != null)
                        {
                            hadPendingWrite = true;
                            _logger.LogInformation(
                                "Flushing remaining write for {ObjectKey}",
                                objectKey);
                        }
                    }
                    finally
                    {
                        queue.Lock.Release();
                    }

                    // Execute flush (acquires write lock internally)
                    try
                    {
                        await ExecuteFlush(queue, objectKey);
                        if (hadPendingWrite)
                        {
                            _logger.LogInformation("Successfully flushed remaining write for {ObjectKey}", objectKey);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Failed to flush remaining writes for {ObjectKey}", objectKey);
                    }
                }));
            }

            await Task.WhenAll(finalFlushTasks);
        }

        /// <summary>
        /// Flushes all pending writes and disposes the manager.
        /// Must be called during shutdown to ensure no data loss.
        /// </summary>
        public async Task FlushAndDisposeAsync()
        {
            _isDisposing = true;

            await FlushAsync();

            // Clean up all locks
            foreach (var queue in _queues.Values)
            {
                queue.Dispose();
            }

            _queues.Clear();
        }
    }
}
