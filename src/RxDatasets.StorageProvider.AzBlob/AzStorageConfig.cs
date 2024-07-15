using System.ComponentModel.DataAnnotations;

namespace RxDatasets.StorageProvider.AzBlob;

public class AzStorageConfig
{
    [Required]
    public string ConnectionString { get; set; } = "";
}