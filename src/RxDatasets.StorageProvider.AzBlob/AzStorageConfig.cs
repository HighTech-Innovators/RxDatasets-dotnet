using System.ComponentModel.DataAnnotations;

namespace RxDatasets.StorageProvider.AzBlob;

[ConnectionStringOrAccountUrlRequired]
public class AzStorageConfig
{
    public string? ConnectionString { get; set; }

    /// <summary>
    /// The blob service endpoint (e.g. https://{account}.blob.core.windows.net) to use with
    /// Azure AD / managed identity authentication via <see cref="Azure.Identity.DefaultAzureCredential"/>.
    /// </summary>
    public string? AccountUrl { get; set; }

    /// <summary>
    /// Client ID of a user-assigned managed identity. Leave null to use the system-assigned
    /// managed identity or the default <see cref="Azure.Identity.DefaultAzureCredential"/> chain.
    /// </summary>
    public string? ManagedIdentityClientId { get; set; }

    [Required]
    public string ContainerName { get; set; } = "";
}
