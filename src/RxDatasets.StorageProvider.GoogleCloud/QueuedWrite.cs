using System;

namespace RxDatasets.StorageProvider.GoogleCloud
{
    /// <summary>
    /// Represents a single write operation queued for deferred execution.
    /// Used internally by WriteQueueManager to track pending writes to GCS objects.
    /// </summary>
    internal sealed class QueuedWrite
    {
        /// <summary>
        /// The GCS object key being written to.
        /// </summary>
        public string ObjectKey { get; }

        /// <summary>
        /// The bytes to write (null if writing from stream).
        /// </summary>
        public byte[]? Bytes { get; }

        /// <summary>
        /// The stream to write (null if writing from bytes).
        /// </summary>
        public Stream? Stream { get; }

        /// <summary>
        /// True if this write comes from a stream source; false if from bytes.
        /// </summary>
        public bool IsStreamSource { get; }

        /// <summary>
        /// When this write was queued (UTC).
        /// </summary>
        public DateTimeOffset QueuedAt { get; }

        /// <summary>
        /// Sequential number assigned when queued (for logging and tracing).
        /// </summary>
        public long SequenceNumber { get; }

        /// <summary>
        /// Creates a queued write for bytes-based content.
        /// </summary>
        public QueuedWrite(string objectKey, byte[] bytes, DateTimeOffset queuedAt, long sequenceNumber)
        {
            ObjectKey = objectKey ?? throw new ArgumentNullException(nameof(objectKey));
            Bytes = bytes ?? throw new ArgumentNullException(nameof(bytes));
            Stream = null;
            IsStreamSource = false;
            QueuedAt = queuedAt;
            SequenceNumber = sequenceNumber;
        }

        /// <summary>
        /// Creates a queued write for stream-based content.
        /// </summary>
        public QueuedWrite(string objectKey, Stream stream, DateTimeOffset queuedAt, long sequenceNumber)
        {
            ObjectKey = objectKey ?? throw new ArgumentNullException(nameof(objectKey));
            Stream = stream ?? throw new ArgumentNullException(nameof(stream));
            Bytes = null;
            IsStreamSource = true;
            QueuedAt = queuedAt;
            SequenceNumber = sequenceNumber;
        }
    }
}
