using System.ComponentModel.DataAnnotations;

namespace RxDatasets.Configs;

public class SnapshotCreationConfig
{
    private const int IntervalHours = 24;

    [Range(0, int.MaxValue,
        ErrorMessage = "Range of hours was exceeded")]
    public int PeriodInHours { get; set; } = IntervalHours;

    public virtual TimeSpan PeriodIntervalAsTimespan => TimeSpan.FromHours(PeriodInHours);

    public virtual ulong SnapshotInterval { get; set; } = 100;

    public int StartupDelaySeconds { get; set; } = 0;
    public virtual TimeSpan StartupDelay => TimeSpan.FromSeconds(StartupDelaySeconds);
}
