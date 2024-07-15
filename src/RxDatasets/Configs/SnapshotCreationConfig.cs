using System.ComponentModel.DataAnnotations;

namespace RxDatasets.Configs;

public class SnapshotCreationConfig
{
    private const int IntervalHours = 24;

    [Range(0, int.MaxValue,
        ErrorMessage = "Range of hours was exceeded")]
    public int PeriodInHours { get; set; } = IntervalHours;

    public TimeSpan PeriodIntervalAsTimespan => TimeSpan.FromHours(PeriodInHours);

    public ulong SnapshotInterval { get; set; } = 100;
}
