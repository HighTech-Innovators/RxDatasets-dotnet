using System.ComponentModel.DataAnnotations;

namespace RxDatasets.StorageProvider.Minio;

[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
public class RegionOrEndpointRequiredAttribute : ValidationAttribute
{
    protected override ValidationResult IsValid(object? value, ValidationContext validationContext)
    {
        var config = (MinioS3Config)validationContext.ObjectInstance;

        var isAwsRegionEmpty = string.IsNullOrEmpty(config.AwsRegion);
        var isServiceUrlEmpty = string.IsNullOrEmpty(config.ServiceURL);

        return (isAwsRegionEmpty, isServiceUrlEmpty) switch
        {
            (true, true) => new ValidationResult("Either AwsRegion or ServiceUrl must be filled."),
            (false, false) => new ValidationResult("Either AwsRegion or ServiceUrl must be filled, but not both."),
            _ => ValidationResult.Success!
        };
    }
}