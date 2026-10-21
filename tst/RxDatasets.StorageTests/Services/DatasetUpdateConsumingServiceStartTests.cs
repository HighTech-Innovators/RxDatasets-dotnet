using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Notifications;

namespace RxDatasets.StorageTests.Services;

/// <summary>
/// Since .NET 10, <c>BackgroundService</c> runs <c>ExecuteAsync</c> on the thread pool after <c>StartAsync</c>
/// returns. These tests live in this project because it runs on Microsoft.AspNetCore.App, like the servers do, and so
/// gets that behaviour; RxDatasets.Tests gets the 9.x hosting package, where <c>ExecuteAsync</c> still runs inside
/// <c>StartAsync</c> until its first await.
/// </summary>
public class DatasetUpdateConsumingServiceStartTests
{
    private static readonly TimeSpan Soon = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task Notify_RightAfterStart_ShouldReturnOnlyAfterARunThatSawTheDelta()
    {
        // Arrange — a started service whose loop is busy in a first run, which keeps its thread until released
        var consumer = new BlockingFirstRunConsumer();
        using var hub = new NotificationsHubSync<object>();
        using var service = new DatasetUpdateConsumingService<object, BlockingFirstRunConsumer>(
            NullLogger<DatasetUpdateConsumingService<object, BlockingFirstRunConsumer>>.Instance, hub, consumer);
        await hub.Notify(new StorageProviderReadinessNotifcation(ServiceStatus.Ready));
        await service.StartAsync(CancellationToken.None);
        await consumer.FirstRunStarted.WaitAsync(Soon);

        // Act — a delta is stored and notified; NotificationsHubSync.Notify blocks until its handlers are done, so it
        // runs on its own task, and the delay lets it arrive before the first run is released
        consumer.LatestVersion = 1;
        Task notified = Task.Run(() => hub.Notify(new DatasetUpdatedNotification(1)));
        await Task.Delay(200);
        consumer.ReleaseFirstRun();
        await notified.WaitAsync(Soon);

        // Assert
        consumer.VersionsHandled.Should().Contain(1UL,
            "the notification returned, so the service was subscribed and a run must have handled version 1");

        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task StartAsync_ShouldReturnOnlyAfterARunHandledADeltaNotifiedBeforeTheStart()
    {
        // Arrange — a delta notified before the service starts, which the synchronous hub replays when it subscribes
        var consumer = new SlowConsumer { LatestVersion = 1 };
        using var hub = new NotificationsHubSync<object>();
        using var service = new DatasetUpdateConsumingService<object, SlowConsumer>(
            NullLogger<DatasetUpdateConsumingService<object, SlowConsumer>>.Instance, hub, consumer);
        await hub.Notify(new StorageProviderReadinessNotifcation(ServiceStatus.Ready));
        await hub.Notify(new DatasetUpdatedNotification(1));

        // Act — on its own task, so a start that deadlocks on the replayed notification fails the test rather than
        // hanging it: subscribing before the loop runs would wait for a run that never comes
        Func<Task> start = () => Task.Run(() => service.StartAsync(CancellationToken.None)).WaitAsync(Soon);

        // Assert
        await start.Should().NotThrowAsync("the start must not deadlock on the replayed notification");
        consumer.VersionsHandled.Should().Contain(1UL,
            "the replayed notification waits for a run, so StartAsync returns once version 1 is handled");

        await service.StopAsync(CancellationToken.None);
    }

    /// <summary>Every run takes a while, so a start that does not wait for its run returns before it is done.</summary>
    private sealed class SlowConsumer : IDatasetUpdateConsumer<object>
    {
        private long _latestVersion;

        public ulong LatestVersion
        {
            get => (ulong)Interlocked.Read(ref _latestVersion);
            init => _latestVersion = (long)value;
        }

        /// <summary>The version each completed run read when it started.</summary>
        public List<ulong> VersionsHandled { get; } = [];

        public string ConsumerIdentifier => "slow";

        public async Task HandleNewDeltas(CancellationToken stoppingToken)
        {
            var version = LatestVersion;
            await Task.Delay(200, stoppingToken);
            lock (VersionsHandled)
            {
                VersionsHandled.Add(version);
            }
        }
    }

    private sealed class BlockingFirstRunConsumer : IDatasetUpdateConsumer<object>
    {
        private readonly TaskCompletionSource _firstRunStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly ManualResetEventSlim _releaseFirstRun = new();
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

        public string ConsumerIdentifier => "blocking-first-run";

        public void ReleaseFirstRun() => _releaseFirstRun.Set();

        public async Task HandleNewDeltas(CancellationToken stoppingToken)
        {
            var version = LatestVersion;

            if (Interlocked.Increment(ref _runs) == 1)
            {
                _firstRunStarted.TrySetResult();
                // Blocks the thread, not just the run: ExecuteAsync can't get past this first run until it is
                // released. Bounded, so the test fails rather than hangs where ExecuteAsync runs inside StartAsync.
                _releaseFirstRun.Wait(Soon, stoppingToken);
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
