using System.ComponentModel.DataAnnotations;

namespace RxDatasets.StorageProvider.GoogleCloud;

public class GoogleCloudConfig
{
    [Required]
    public string BucketName { get; set; } = "";
    public string? ProjectId { get; set; }
    public string? JsonCredentials { get; set; }
    public string? EmulatorHost { get; set; }
    public bool EnableWriteQueue { get; set; } = true;

    // Bigtable (only used with WithGoogleBigtableStorage); ProjectId is the Bigtable project too. The table must exist,
    // with the column families "meta" and "payload" (max 1 version each).
    public string? BigtableInstanceId { get; set; }
    public string? BigtableTableId { get; set; }
    public string? BigtableEmulatorHost { get; set; }

    /// <summary>
    /// Where a delta's or snapshot's payloads go: up to this size in its Bigtable row, above it in the bucket (the row
    /// points at the object). The index is measured after compression, the data before: data in the bucket is never
    /// compressed, so one entity can be read from it by range.
    /// <para>
    /// The trade-off: a row is read whole, so reading one entity of a version whose data is in the row reads all of that
    /// data (the reader keeps it, see <see cref="InlineDataCacheMB"/>), where the bucket reads just the entity's range but
    /// with higher latency. A small threshold keeps the small deltas of frequent writes in Bigtable, and makes reads of
    /// single entities from large ones (history, reporting catch-up, BatchGet over many versions) cheap. At most 50 MB:
    /// a Bigtable cell holds at most 100 MB.
    /// </para>
    /// </summary>
    [Range(0L, 50L * 1024 * 1024)]
    public long MaxInlinePayloadBytes { get; set; } = 1024 * 1024;

    /// <summary>
    /// Memory for the data payloads read from rows, so the other entities of the same version are served without
    /// reading the row again. With <c>WithCache</c> on, that cache may hold the same bytes too.
    /// </summary>
    [Range(0, 64 * 1024)]
    public int InlineDataCacheMB { get; set; } = 64;

    /// <summary>
    /// Brotli-compresses the payloads stored in rows, and indexes stored in the bucket (not data in the bucket).
    /// </summary>
    public bool CompressPayloads { get; set; }
}
