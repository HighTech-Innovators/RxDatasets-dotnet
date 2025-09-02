namespace RxDatasets.BackgroundServices;

public class DistributedLockOptions
{
    public TimeSpan RenewalInterval { get; set; } = TimeSpan.FromMinutes(5);
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan RenewalThreshold { get; set; } = TimeSpan.FromSeconds(30);
}
