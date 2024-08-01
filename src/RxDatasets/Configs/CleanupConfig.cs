using System.ComponentModel.DataAnnotations;

namespace RxDatasets.Configs;

public class CleanupConfig
{
    private const int DefaultMinimumAgeToLiveInMinutes = 120;
    private const int DefaultMinimumSnapshotsToKeep = 2;

    [Range(DefaultMinimumAgeToLiveInMinutes, int.MaxValue,
        ErrorMessage = "The minimum age to live must be at least 120 minutes.")]
    public int MinimumAgeToLiveInMinutes { get; set; } = DefaultMinimumAgeToLiveInMinutes;

    [Range(DefaultMinimumSnapshotsToKeep, int.MaxValue, ErrorMessage = "At least 2 snapshots must be kept.")]
    public int MinimumSnapshotsToKeep { get; set; } = DefaultMinimumSnapshotsToKeep;
    public bool KeepAllDeltas { get; set; } = false;
    public bool IsEnabled { get; set; } = true;


    public virtual TimeSpan GetMinimumAgeToLiveInMinutes()
    {
        return TimeSpan.FromMinutes(MinimumAgeToLiveInMinutes);
    }
}
