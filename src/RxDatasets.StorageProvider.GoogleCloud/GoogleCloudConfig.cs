using System.ComponentModel.DataAnnotations;

namespace RxDatasets.StorageProvider.GoogleCloud;

public class GoogleCloudConfig
{
    [Required]
    public string BucketName { get; set; } = "";
    public string? ProjectId { get; set; }
    public string? JsonCredentials { get; set; }
}
