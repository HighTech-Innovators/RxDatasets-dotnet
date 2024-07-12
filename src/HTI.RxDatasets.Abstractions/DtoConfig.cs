using System.ComponentModel.DataAnnotations;

namespace HTI.RxDatasets.Abstractions;

public class DtoConfig
{
    [Required]
    public string BucketName { get; set; } = "";
}