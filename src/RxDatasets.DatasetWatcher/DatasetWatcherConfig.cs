namespace RxDatasets.DatasetWatcher;

public class DatasetWatcherConfig
{
    public int CheckingIntervalInSeconds { get; set; } = 1;

    public TimeSpan GetCheckingInterval() => TimeSpan.FromSeconds(CheckingIntervalInSeconds);
}
