using System;
using System.Threading;
using System.Threading.Tasks;

namespace RxDatasets.StorageProvider.GoogleCloud
{
    /// <summary>
    /// Manages the queued writes for a single GCS object.
    /// Each object (e.g., delta.pb, ProcessedVersions.pb) has its own queue and timing state.
    /// </summary>
    internal sealed class FileWriteQueueState : IDisposable
    {
        private readonly ReaderWriterLockSlim _lock = new();

        /// <summary>
        /// The GCS object key this queue is managing.
        /// </summary>
        public string ObjectKey { get; }

        /// <summary>
        /// The single pending write waiting to be flushed (latest write replaces previous).
        /// Accessed under write lock.
        /// </summary>
        public QueuedWrite? PendingWrite { get; set; }

        /// <summary>
        /// The timestamp of the last successful write to this object (UTC).
        /// Updated when a write completes successfully, checked to determine if new writes should be queued.
        /// Accessed under read lock for reads, write lock for updates.
        /// </summary>
        public DateTimeOffset LastSuccessfulWrite { get; set; } = DateTimeOffset.MinValue;

        /// <summary>
        /// Whether a flush task is currently scheduled for this object.
        /// Prevents scheduling multiple flush tasks.
        /// </summary>
        public bool FlushScheduled { get; set; }

        /// <summary>
        /// The currently scheduled flush task (if any).
        /// Stored so DisposeAsync can await it.
        /// </summary>
        public Task? FlushTask { get; set; }

        /// <summary>
        /// Reader-writer lock for synchronizing access to queue state.
        /// Allows multiple readers for LastSuccessfulWrite checks, exclusive writer for updates.
        /// </summary>
        public ReaderWriterLockSlim Lock => _lock;

        public FileWriteQueueState(string objectKey)
        {
            ObjectKey = objectKey ?? throw new ArgumentNullException(nameof(objectKey));
        }

        /// <summary>
        /// Determines if this queue is eligible for flushing based on time elapsed since last successful write.
        /// </summary>
        public bool IsFlushEligible(DateTimeOffset now)
        {
            // If never written, not eligible
            if (LastSuccessfulWrite == DateTimeOffset.MinValue)
                return false;

            // Eligible if 2+ seconds have passed since last successful write
            var elapsed = now - LastSuccessfulWrite;
            return elapsed.TotalSeconds >= 2.0;
        }

        /// <summary>
        /// Gets milliseconds until this queue becomes flush-eligible (0 if already eligible or no writes yet).
        /// </summary>
        public int GetMillisecondsUntilFlushEligible(DateTimeOffset now)
        {
            if (LastSuccessfulWrite == DateTimeOffset.MinValue)
                return 0;

            var elapsed = now - LastSuccessfulWrite;
            var needed = TimeSpan.FromSeconds(2.0);

            if (elapsed >= needed)
                return 0; // Already eligible

            var remaining = needed - elapsed;
            return (int)Math.Ceiling(remaining.TotalMilliseconds);
        }

        public void Dispose()
        {
            _lock?.Dispose();
        }
    }
}
