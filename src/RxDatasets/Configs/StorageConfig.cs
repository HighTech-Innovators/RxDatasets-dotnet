using System.ComponentModel.DataAnnotations;

namespace RxDatasets.Configs;

public class StorageConfig
{
    [RegularExpression(@"^$|^[a-zA-Z0-9\-_/]+/$", ErrorMessage = "The field must be either an empty string or be a sensible alphanumeric name and end with a '/'.")]
    public string CustomRootFolder { get; set; } = "";

    [RegularExpression(@"^$|^[a-zA-Z0-9\-_\.]+$", ErrorMessage = "The field must be either an empty string or be a sensible alphanumeric name.")]
    public string DtoName { get; set; } = "";

    public UploadVerificationMode UploadVerificationMode { get; set; } = UploadVerificationMode.DoNotVerify;
    public bool LogStorageAccess { get; set; } = false;

    /// <summary>
    /// Brotli compression level for delta indices (0 = no compression, 1 = fastest, 11 = smallest).
    /// </summary>
    public int DeltaIndexCompressionLevel { get; set; } = 1;

    /// <summary>
    /// Brotli compression level for snapshot indices (0 = no compression, 1 = fastest, 11 = smallest).
    /// Snapshots are written in the background so higher compression is free.
    /// </summary>
    public int SnapshotIndexCompressionLevel { get; set; } = 11;

    /// <summary>
    /// Brotli compression level for delta data (0 = no compression, 1 = fastest, 11 = smallest).
    /// </summary>
    public int DeltaDataCompressionLevel { get; set; } = 1;

    /// <summary>
    /// Brotli compression level for snapshot data (0 = no compression, 1 = fastest, 11 = smallest).
    /// Snapshots are written in the background so higher compression is free.
    /// </summary>
    public int SnapshotDataCompressionLevel { get; set; } = 11;
}

public enum UploadVerificationMode
{
    VerifyAll,
    VerifyLargeDataOnly,
    DoNotVerify
}
