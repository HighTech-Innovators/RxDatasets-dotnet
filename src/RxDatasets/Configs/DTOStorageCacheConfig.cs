using System.ComponentModel.DataAnnotations;

namespace RxDatasets.Configs;

public class DtoStorageCacheConfig
{
    [Range(0, 2048)]
    public int CacheSizeMB { get; set; } = 100; 
}