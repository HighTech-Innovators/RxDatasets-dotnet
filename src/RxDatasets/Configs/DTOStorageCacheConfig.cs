using System.ComponentModel.DataAnnotations;

namespace RxDatasets.Configs;

public class DtoStorageCacheConfig
{
    [Range(0, 65536)]
    public int CacheSizeMB { get; set; } = 100;
}
