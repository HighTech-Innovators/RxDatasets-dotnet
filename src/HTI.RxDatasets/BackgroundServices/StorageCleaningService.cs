using System.Reactive.Concurrency;
using HTI.RxDatasets.Abstractions;
using HTI.RxDatasets.DatasetUpdateNotifications;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace HTI.RxDatasets.BackgroundServices
{
    public class StorageCleaningService<T> : IHostedService
    {
        private IDisposable? _disposableSubscription;
        private readonly List<DateTime> _listOfSnapshotsDatesScheduledForDeleting = [];
        private readonly IOptionsMonitor<CleanupConfig> _configMonitor;
        private readonly IDatasetUpdateNotifier<T> datasetUpdateNotifier;
        private readonly IStorageProvider<T> storageProvider;
        private readonly IScheduler scheduler;
        private readonly ILogger<StorageCleaningService<T>> _logger;

        public StorageCleaningService(
            IDatasetUpdateNotifier<T> datasetUpdateNotifier,
            IStorageProvider<T> storageProvider,
            IScheduler scheduler,
            IOptionsMonitor<CleanupConfig> configMonitor,
            ILogger<StorageCleaningService<T>> logger)
        {
            this.datasetUpdateNotifier = datasetUpdateNotifier;
            this.storageProvider = storageProvider;
            this.scheduler = scheduler;
            _configMonitor = configMonitor;
            _logger = logger;
        }


        public async Task CleanStorageAsync(ulong versionOfOldestToKeepSnapshot)
        {
            await storageProvider.DeleteOlderThan(versionOfOldestToKeepSnapshot);
        }

        public async Task SearchForOutdatedData()
        {
            try
            {
                var (_, _, allSnapshots) = await storageProvider.ListVersions();
                var LastProcessedSnapshotVersion = await storageProvider.GetLowestProcessedVersion();
                if (allSnapshots.Count == 0) return;

                var minimumSnapshotsToKeep = _configMonitor.CurrentValue.MinimumSnapshotsToKeep;
                var minimumAgeToLiveInMinutes = _configMonitor.CurrentValue.GetMinimumAgeToLiveInMinutes();

                var cutoffTime = scheduler.Now.UtcDateTime - minimumAgeToLiveInMinutes;

                var orderedAllSnapshots = allSnapshots.OrderByDescending(s => s.version).ToList();

                // Skip the newest groups we want to keep and select the rest
                var toDeleteSnapshots = orderedAllSnapshots
                    .Skip(minimumSnapshotsToKeep) // Skip the newest groups that we want to keep
                    .Where(s => s.DateTime.UtcDateTime <= cutoffTime)
                    .ToList();

                var snapshotsThatHasToWait = orderedAllSnapshots
                    .Skip(minimumSnapshotsToKeep)
                    .Where(s => s.DateTime.UtcDateTime > cutoffTime &&
                                !_listOfSnapshotsDatesScheduledForDeleting.Any(tbd => tbd == s.DateTime))
                    .ToList();

                var versionOfOldestToKeepSnapshot = orderedAllSnapshots.Except(toDeleteSnapshots).Last().version;
                var versionOfProcessedToKeepSnapshot = LastProcessedSnapshotVersion < versionOfOldestToKeepSnapshot ? LastProcessedSnapshotVersion : versionOfOldestToKeepSnapshot;

                if (snapshotsThatHasToWait.Count != 0)
                {
                    ScheduleItemsToBeDeletedAt(snapshotsThatHasToWait.Last().DateTime.UtcDateTime);
                }

                if (toDeleteSnapshots.Count == 0) return;
                await CleanStorageAsync(versionOfProcessedToKeepSnapshot);
            }

            catch (Exception)
            {
                scheduler.ScheduleAsync(TimeSpan.FromSeconds(15), (_, _) => SearchForOutdatedData());
            }
        }

        private void ScheduleItemsToBeDeletedAt(DateTime earliestSnapshotTime)
        {
            if(_listOfSnapshotsDatesScheduledForDeleting.Any(s=>s == earliestSnapshotTime)) return;
            
            var currentTime = scheduler.Now.UtcDateTime;

            // Calculate when you want the cleaning to occur
            var scheduleTime = earliestSnapshotTime + _configMonitor.CurrentValue.GetMinimumAgeToLiveInMinutes();

            // Calculate the delay as a TimeSpan
            var delay = scheduleTime - currentTime;
            if (delay > TimeSpan.Zero)
            {
                scheduler.ScheduleAsync(delay, (_, _) => SearchForOutdatedData());
            }
            _listOfSnapshotsDatesScheduledForDeleting.Add(earliestSnapshotTime);
        }


        public Task StartAsync(CancellationToken cancellationToken)
        {
            _disposableSubscription = datasetUpdateNotifier.Subscribe(ASnapshotHasBeenCreated);
            _ = SearchForOutdatedData();
            _logger.LogInformation(typeof(T).Name + " cleaning schedule started");
            return Task.CompletedTask;
        }

        private async Task ASnapshotHasBeenCreated(DatasetUpdateNotification datasetUpdateNotification)
        {
            if (datasetUpdateNotification.SnapshotVersion != datasetUpdateNotification.Version) return;
            await SearchForOutdatedData();
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _disposableSubscription?.Dispose();
            return Task.CompletedTask;
        }
    }
}