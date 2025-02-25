using System.ComponentModel.DataAnnotations;

namespace RxDatasets.StorageProvider.S3;

[RegionOrEndpointRequired]
public class S3Config
{
    [Required]
    public string AccessKey { get; set; } = "";

    [Required]
    public string SecretKey { get; set; } = "";
    public string ServiceUrl { get; set; } = "";

    [Required]
    public string BucketName { get; set; } = "";

    public string Region { get; set; } = "";
}
