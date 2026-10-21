using Microsoft.Extensions.Logging.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Notifications;

namespace RxDatasets.Tests;

/// <summary>
/// With a synchronous notifications hub, a write's <see cref="DatasetUpdatedNotification"/> returns only once the
/// consumer has handled the deltas; that is what lets a writer, and a test, rely on the consumer having seen it.
/// </summary>
public class DatasetUpdateConsumingServiceTests
{
    private static readonly TimeSpan Soon = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task Notify_ShouldReturnOnlyAfterARunThatSawTheDelta_WhenARunWasAlreadyInProgress()
    {
        // Arrange — a consumer whose first run is in progress, and has read the dataset before the delta came
        var consumer = new GatedConsumer();
        using var hub = new NotificationsHubSync<object>();
        var service = new DatasetUpdateConsumingService<object, GatedConsumer>(
            NullLogger<DatasetUpdateConsumingService<object, GatedConsumer>>.Instance, hub, consumer);
        await hub.Notify(new StorageProviderReadinessNotifcation(ServiceStatus.Ready));
        await service.StartAsync(CancellationToken.None);
        await consumer.FirstRunStarted.WaitAsync(Soon);

        // Act — a delta is stored and notified while that run is still going, and then the run finishes.
        // NotificationsHubSync.Notify blocks until the handlers are done, so it runs on its own task; the delay
        // lets the notification arrive before the first run is released.
        consumer.LatestVersion = 1;
        Task notified = Task.Run(() => hub.Notify(new DatasetUpdatedNotification(1)));
        await Task.Delay(200);
        consumer.ReleaseFirstRun();
        await notified.WaitAsync(Soon);

        // Assert
        consumer.VersionsHandled.Should().Contain(1UL, "the notification returned, so a run must have handled version 1");

        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Notify_ShouldReturn_WhenTheServiceStopsWhileItWaitsForARun()
    {
        // Arrange — a consumer whose first run never finishes on its own
        var consumer = new GatedConsumer();
        using var hub = new NotificationsHubSync<object>();
        var service = new DatasetUpdateConsumingService<object, GatedConsumer>(
            NullLogger<DatasetUpdateConsumingService<object, GatedConsumer>>.Instance, hub, consumer);
        await hub.Notify(new StorageProviderReadinessNotifcation(ServiceStatus.Ready));
        await service.StartAsync(CancellationToken.None);
        await consumer.FirstRunStarted.WaitAsync(Soon);

        // A notification that waits for a run, which will not come before the service stops.
        // NotificationsHubSync.Notify blocks until the handlers are done, so it runs on its own task.
        consumer.LatestVersion = 1;
        Task notified = Task.Run(() => hub.Notify(new DatasetUpdatedNotification(1)));
        await Task.Delay(200);
        notified.IsCompleted.Should().BeFalse("the notification waits for a run that saw the delta");

        // Act
        await service.StopAsync(CancellationToken.None).WaitAsync(Soon);

        // Assert — the notification returns, without an exception for the writer that sent it
        Func<Task> waitForNotification = () => notified.WaitAsync(Soon);
        await waitForNotification.Should().NotThrowAsync();
    }

    [Fact]
    public async Task StopAsync_ShouldNotThrow_AfterDispose()
    {
        // Arrange — the host can stop a service after it has disposed it
        var consumer = new GatedConsumer();
        consumer.ReleaseFirstRun();
        using var hub = new NotificationsHubSync<object>();
        var service = new DatasetUpdateConsumingService<object, GatedConsumer>(
            NullLogger<DatasetUpdateConsumingService<object, GatedConsumer>>.Instance, hub, consumer);
        await hub.Notify(new StorageProviderReadinessNotifcation(ServiceStatus.Ready));
        await service.StartAsync(CancellationToken.None);
        service.Dispose();

        // Act
        Func<Task> stop = () => service.StopAsync(CancellationToken.None).WaitAsync(Soon);

        // Assert
        await stop.Should().NotThrowAsync();
    }

    private sealed class GatedConsumer : IDatasetUpdateConsumer<object>
    {
        private readonly TaskCompletionSource _firstRunStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _releaseFirstRun = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _runs;
        private long _latestVersion;

        public ulong LatestVersion
        {
            get => (ulong)Interlocked.Read(ref _latestVersion);
            set => Interlocked.Exchange(ref _latestVersion, (long)value);
        }

        /// <summary>The version each completed run read when it started.</summary>
        public List<ulong> VersionsHandled { get; } = [];

        public Task FirstRunStarted => _firstRunStarted.Task;

        public string ConsumerIdentifier => "gated";

        public void ReleaseFirstRun() => _releaseFirstRun.TrySetResult();

        public async Task HandleNewDeltas(CancellationToken stoppingToken)
        {
            var version = LatestVersion;

            if (Interlocked.Increment(ref _runs) == 1)
            {
                _firstRunStarted.TrySetResult();
                await _releaseFirstRun.Task.WaitAsync(stoppingToken);
            }
            else
            {
                // Handling takes a while, as it does for a real consumer
                await Task.Delay(200, stoppingToken);
            }

            lock (VersionsHandled)
            {
                VersionsHandled.Add(version);
            }
        }
    }
}
