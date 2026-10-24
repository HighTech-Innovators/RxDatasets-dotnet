using System.ComponentModel.DataAnnotations;
using Amazon.Runtime;

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

    /// <summary>
    /// When the SDK adds integrity checksums (CRC32 etc.) to upload requests.
    /// AWS SDK v4 defaults to WHEN_SUPPORTED, which S3-compatible providers (Scaleway, Minio, Ceph, ...)
    /// may reject with 400 Bad Request. Defaults to WHEN_REQUIRED, the pre-2025 SDK behaviour.
    /// </summary>
    public RequestChecksumCalculation RequestChecksumCalculation { get; set; } = RequestChecksumCalculation.WHEN_REQUIRED;

    /// <summary>
    /// When the SDK validates checksums returned on downloads. Defaults to WHEN_REQUIRED, because some
    /// S3-compatible providers return the full-object checksum on ranged GETs, which fails validation.
    /// </summary>
    public ResponseChecksumValidation ResponseChecksumValidation { get; set; } = ResponseChecksumValidation.WHEN_REQUIRED;
}
