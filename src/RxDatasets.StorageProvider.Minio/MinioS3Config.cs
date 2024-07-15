using System.ComponentModel.DataAnnotations;

namespace RxDatasets.StorageProvider.Minio;

public class MinioS3Config
{
    [Required]
    public string AccessKey { get; set; } = "";

    [Required]
    public string SecretKey { get; set; } = "";

    [Required]
    public string ServiceURL { get; set; } = "";

    public string AwsRegion { get; set; } = "";

    [Required]
    public string BucketName { get; set; } = "";
}