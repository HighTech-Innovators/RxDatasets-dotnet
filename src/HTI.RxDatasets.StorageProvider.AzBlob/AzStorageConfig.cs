using System.ComponentModel.DataAnnotations;

namespace HTI.RxDatasets.StorageProvider.AzBlob;

public class AzStorageConfig
{
    [Required]
    public string ConnectionString { get; set; } = "";
}