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
    /// Compression for delta indices. Default: None.
    /// </summary>
    public CompressionSetting DeltaIndexCompression { get; set; } = new();

    /// <summary>
    /// Compression for snapshot indices. Default: None.
    /// </summary>
    public CompressionSetting SnapshotIndexCompression { get; set; } = new();

    /// <summary>
    /// Compression for delta data. Default: None.
    /// </summary>
    public CompressionSetting DeltaDataCompression { get; set; } = new();

    /// <summary>
    /// Compression for snapshot data. Default: None.
    /// </summary>
    public CompressionSetting SnapshotDataCompression { get; set; } = new();
}

public enum UploadVerificationMode
{
    VerifyAll,
    VerifyLargeDataOnly,
    DoNotVerify
}

public enum CompressionAlgorithm
{
    None,
    Brotli,
    Lz4
}

public class CompressionSetting
{
    /// <summary>
    /// Compression algorithm: None, Brotli, or Lz4.
    /// </summary>
    public CompressionAlgorithm Algorithm { get; set; } = CompressionAlgorithm.None;

    /// <summary>
    /// For Brotli: 1 (fastest) to 11 (smallest). For LZ4: 0 (fastest) to 12 (max).
    /// </summary>
    public int Level { get; set; } = 1;
}
