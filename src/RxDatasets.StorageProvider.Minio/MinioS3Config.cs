using System.ComponentModel.DataAnnotations;

namespace RxDatasets.StorageProvider.Minio;

[RegionOrEndpointRequiredAttribute]
public class MinioS3Config
{
    [Required]
    public string AccessKey { get; set; } = "";

    [Required]
    public string SecretKey { get; set; } = "";

    public string ServiceURL { get; set; } = "";

    public string AwsRegion { get; set; } = "";

    [Required]
    public string BucketName { get; set; } = "";
}
