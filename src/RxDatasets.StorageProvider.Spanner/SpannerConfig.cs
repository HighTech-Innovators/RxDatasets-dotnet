using System.ComponentModel.DataAnnotations;

namespace RxDatasets.StorageProvider.Spanner;

public class SpannerConfig
{
    /// <summary>
    /// e.g. <c>Data Source=projects/{project}/instances/{instance}/databases/{database}</c>; add
    /// <c>;EmulatorDetection=EmulatorOnly</c> to use the emulator from <c>SPANNER_EMULATOR_HOST</c>.
    /// </summary>
    [Required]
    public string ConnectionString { get; set; } = "";

    [RegularExpression("^[A-Za-z][A-Za-z0-9_]*$")]
    public string TablePrefix { get; set; } = "RxDatasets";

    /// <summary>
    /// False (default): all datasets share one set of tables, keyed by the dataset's storage folder first.
    /// True: each dataset gets its own tables, named after the prefix and the storage folder. Same columns either way.
    /// </summary>
    public bool TablePerDataset { get; set; }
}
