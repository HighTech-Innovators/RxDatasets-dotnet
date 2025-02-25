using System.ComponentModel.DataAnnotations;

namespace RxDatasets.StorageProvider.S3;

[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
public class RegionOrEndpointRequiredAttribute : ValidationAttribute
{
    protected override ValidationResult IsValid(object? value, ValidationContext validationContext)
    {
        var config = (S3Config)validationContext.ObjectInstance;

        var isRegionEmpty = string.IsNullOrEmpty(config.Region);
        var isServiceUrlEmpty = string.IsNullOrEmpty(config.ServiceUrl);

        return (isRegionEmpty, isServiceUrlEmpty) switch
        {
            (true, true) => new ValidationResult("Either Region or ServiceUrl must be filled."),
            (false, false) => new ValidationResult("Either Region or ServiceUrl must be filled, but not both."),
            _ => ValidationResult.Success!
        };
    }
}
