using System.ComponentModel.DataAnnotations;

namespace RxDatasets.Abstractions;

public class DtoConfig
{
    [Required]
    public string BucketName { get; set; } = "";
}