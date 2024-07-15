using System.ComponentModel.DataAnnotations;

namespace RxDatasets.Models.Config;

public class DtoStorageCacheConfig
{
    [Range(0, 2048)]
    public int CacheSizeMB { get; set; } = 100;
    public int CacheDurationSec { get; set; } = 60;
}