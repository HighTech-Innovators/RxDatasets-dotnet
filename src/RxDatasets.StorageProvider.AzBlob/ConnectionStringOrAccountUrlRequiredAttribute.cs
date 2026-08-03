using System.ComponentModel.DataAnnotations;

namespace RxDatasets.StorageProvider.AzBlob;

[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
public class ConnectionStringOrAccountUrlRequiredAttribute : ValidationAttribute
{
    protected override ValidationResult IsValid(object? value, ValidationContext validationContext)
    {
        var config = (AzStorageConfig)validationContext.ObjectInstance;

        var isConnectionStringEmpty = string.IsNullOrEmpty(config.ConnectionString);
        var isAccountUrlEmpty = string.IsNullOrEmpty(config.AccountUrl);

        return (isConnectionStringEmpty, isAccountUrlEmpty) switch
        {
            (true, true) => new ValidationResult("Either ConnectionString or AccountUrl must be filled."),
            (false, false) => new ValidationResult("Either ConnectionString or AccountUrl must be filled, but not both."),
            _ => ValidationResult.Success!
        };
    }
}
