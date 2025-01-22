using System.Reactive.Concurrency;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Notifications;

namespace RxDatasets.BackgroundServices
{
    public class StorageCleaningService<T>(
        INotificationsHub<T> notificationsHub,
        IStorageProvider<T> storageProvider,
        IScheduler scheduler,
        IOptions<CleanupConfig> config,
        ILogger<StorageCleaningService<T>> logger)
        : IHostedService
    {
        private IDisposable? _disposableSnapshotNotificationSubscription;
        private readonly List<DateTime> _listOfSnapshotsDatesScheduledForDeleting = [];
        private IDisposable? _disposableVersionProcessedNotificationSubscription;

        public async Task SearchForOutdatedData()
        {
            try
            {
                var (_, _, allSnapshots) = await storageProvider.ListVersions();
                if (allSnapshots.Count == 0) return;

                var minimumSnapshotsToKeep = config.Value.MinimumSnapshotsToKeep;
                var minimumAgeToLiveInMinutes = config.Value.GetMinimumAgeToLiveInMinutes();

                var cutoffTime = scheduler.Now.UtcDateTime - minimumAgeToLiveInMinutes;

                var orderedAllSnapshots = allSnapshots.OrderByDescending(s => s.Version).ToList();

                // Skip the newest groups we want to keep and select the rest
                var toDeleteSnapshots = orderedAllSnapshots
                    .Skip(minimumSnapshotsToKeep) // Skip the newest groups that we want to keep
                    .Where(s => s.DateTime?.UtcDateTime <= cutoffTime)
                    .ToList();

                var snapshotsThatHasToWait = orderedAllSnapshots
                    .Skip(minimumSnapshotsToKeep)
                    .Where(s => s.DateTime?.UtcDateTime > cutoffTime &&
                                !_listOfSnapshotsDatesScheduledForDeleting.Any(tbd => tbd == s.DateTime))
                    .ToList();

                var versionOfOldestToKeepSnapshot = orderedAllSnapshots.Except(toDeleteSnapshots).Last().Version;
                var lowestProcessedVersion = await storageProvider.GetLowestProcessedVersion();

                if (snapshotsThatHasToWait.Count != 0)
                {
                    ScheduleItemsToBeDeletedAt(snapshotsThatHasToWait.Last().DateTime?.UtcDateTime);
                }

                if (toDeleteSnapshots.Count == 0) return;
                await storageProvider.DeleteOlderThan(versionOfOldestToKeepSnapshot, config.Value.KeepAllDeltas);
            }

            catch (Exception ex)
            {
                logger.LogWarning($"Failed to clean storage: {ex}");
                scheduler.ScheduleAsync(TimeSpan.FromSeconds(30), (_, _) => SearchForOutdatedData());
            }
        }

        private void ScheduleItemsToBeDeletedAt(DateTime? earliestSnapshotTime)
        {
            if(_listOfSnapshotsDatesScheduledForDeleting.Any(s=>s <= earliestSnapshotTime)) return;
            
            var currentTime = scheduler.Now.UtcDateTime;

            // Calculate when you want the cleaning to occur
            var scheduleTime = earliestSnapshotTime + config.Value.GetMinimumAgeToLiveInMinutes();

            // Calculate the delay as a TimeSpan
            var delay = scheduleTime - currentTime;
            if (delay > TimeSpan.Zero)
            {
                scheduler.ScheduleAsync((TimeSpan)delay, (_, _) => SearchForOutdatedData());
            }
            _listOfSnapshotsDatesScheduledForDeleting.Add(earliestSnapshotTime.Value);
        }


        public Task StartAsync(CancellationToken cancellationToken)
        {
            if (!config.Value.IsEnabled)
                return Task.CompletedTask;

            _disposableSnapshotNotificationSubscription = notificationsHub
                .Subscribe<SnapshotCreatedNotification>( (_) => SearchForOutdatedData());

            if (!config.Value.KeepAllDeltas)
            {
                _disposableVersionProcessedNotificationSubscription = notificationsHub
                    .Subscribe<VersionProcessedNotification>((_) => SearchForOutdatedData());
            }

            _ = SearchForOutdatedData();
            logger.LogInformation(typeof(T).Name + " cleaning schedule started");
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _disposableSnapshotNotificationSubscription?.Dispose();
            _disposableVersionProcessedNotificationSubscription?.Dispose();
            return Task.CompletedTask;
        }
    }
}