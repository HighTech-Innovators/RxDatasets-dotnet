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

    // Bigtable settings
    public string? BigtableProjectId { get; set; }
    public string? BigtableInstanceId { get; set; }
    public string? BigtableTableId { get; set; }
    public long SizeThresholdBytes { get; set; } = 1024 * 1024; // 1MB default
}
