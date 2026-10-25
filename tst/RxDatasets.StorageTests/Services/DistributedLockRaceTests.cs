using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Notifications;
using RxDatasets.StorageProvider.LocalDisk;
using RxDatasets.StorageTests.Common;
using RxDatasets.TestImplementations.InMemoryProvider;

namespace RxDatasets.StorageTests.Services;

/// <summary>
/// Several lock services on one storage, driven by a shared virtual clock. Each instance can be paused
/// (think GC pause or network stall) so its lock expires and another instance takes it over.
/// </summary>
public sealed class DistributedLockRaceTests : IAsyncLifetime
{
    private static readonly IOptions<StorageConfig> StorageConfig = Options.Create(new StorageConfig { DtoName = "RaceTest" });

    private static readonly IOptions<DistributedLockOptions> LockOptions = Options.Create(new DistributedLockOptions
    {
        RenewalInterval = TimeSpan.FromSeconds(20),
        RenewalThreshold = TimeSpan.FromSeconds(5),
        PollingInterval = TimeSpan.FromSeconds(1),
        HandoverTimeout = TimeSpan.FromSeconds(3),
    });

    private readonly ManualClock _clock = new();
    private readonly LowLevelInMemoryStorage<object> _inMemory = new(NullLogger<LowLevelInMemoryStorage<object>>.Instance);
    private readonly string _rootFolder = Path.Combine(Path.GetTempPath(), $"rxdatasets-lock-race-{Guid.NewGuid():N}");
    private readonly List<Node> _nodes = [];

    [Fact]
    public Task InMemory_Conditional_Operations_Follow_The_Contract() => ConditionalStorageContract.VerifyAsync(_inMemory);

    [Theory]
    [InlineData("in-memory")]
    [InlineData("local-disk")]
    public async Task At_Most_One_Running_Instance_Is_Active_At_Any_Time(string storageKind)
    {
        var random = new Random(79);
        var nodes = Enumerable.Range(0, 5).Select(_ => AddNode(storageKind)).ToList();

        // All start at the same time: a real race on the initial acquisition
        await Task.WhenAll(nodes.Select(n => n.Service.StartAsync(CancellationToken.None)));
        WaitUntilParked(nodes);
        AssertAtMostOneActive(nodes);

        for (var step = 0; step < 400; step++)
        {
            // Stall the holder now and then, and resume stalled instances at random moments: some resume
            // before their lock expires, some after another instance took it over
            if (random.Next(12) == 0)
                nodes.FirstOrDefault(n => n.Service.HasLock && !n.Clock.Paused)?.Clock.Pause();
            if (random.Next(10) == 0)
                nodes[random.Next(nodes.Count)].Clock.Resume();

            _clock.Advance(TimeSpan.FromSeconds(1));
            WaitUntilParked(nodes);
            AssertAtMostOneActive(nodes);
        }

        foreach (var node in nodes)
            node.Clock.Resume();
        for (var step = 0; step < 30; step++)
        {
            _clock.Advance(TimeSpan.FromSeconds(1));
            WaitUntilParked(nodes);
            AssertAtMostOneActive(nodes);
        }

        nodes.Count(n => n.Service.HasLock).Should().Be(1, "someone holds the lock once everybody runs again");
        // Lost is only notified once a stalled holder resumes and finds its lock taken over
        nodes.Sum(n => n.Acquired).Should().BeGreaterThanOrEqualTo(10, "the schedule must actually move the lock around");
        nodes.Sum(n => n.Lost).Should().BeGreaterThanOrEqualTo(5, "the schedule must actually take locks over");
    }

    [Theory]
    [InlineData("in-memory", false)]
    [InlineData("in-memory", true)]
    [InlineData("local-disk", false)]
    [InlineData("local-disk", true)]
    public async Task Taken_Over_Instance_Cannot_Renew_Or_Release_The_New_Owners_Lock(
        string storageKind, bool stopInsteadOfResume)
    {
        var a = AddNode(storageKind);
        await a.Service.StartAsync(CancellationToken.None);
        WaitUntilParked([a]);
        a.Service.HasLock.Should().BeTrue();

        // A stalls; B starts and takes over once A's lock has expired
        a.Clock.Pause();
        var b = AddNode(storageKind);
        await b.Service.StartAsync(CancellationToken.None);
        for (var step = 0; step < 60 && !b.Service.HasLock; step++)
        {
            _clock.Advance(TimeSpan.FromSeconds(1));
            WaitUntilParked([a, b]);
        }

        b.Service.HasLock.Should().BeTrue();
        a.Service.HasLock.Should().BeTrue("A is stalled and has not noticed the takeover yet");
        var lockOfB = await ReadLockFile(storageKind);

        if (stopInsteadOfResume)
        {
            // Stopping releases the lock: must not touch B's
            await a.Service.StopAsync(CancellationToken.None);
        }
        else
        {
            // Resuming renews the lock (it is overdue): must fail and report the loss
            a.Clock.Resume();
            WaitUntilParked([a, b]);
            a.Lost.Should().Be(1);
        }

        a.Service.HasLock.Should().BeFalse();
        (await ReadLockFile(storageKind)).Should().Equal(lockOfB);
        b.Service.HasLock.Should().BeTrue();
    }

    private Node AddNode(string storageKind)
    {
        IDiskStorage<object> storage = storageKind == "in-memory"
            ? _inMemory
            // A storage instance per node, as if each were its own process
            : new LocalDiskStorage<object>(NullLogger<LocalDiskStorage<object>>.Instance,
                Options.Create(new LocalDiskStorageConfig { RootFolder = _rootFolder }));
        var hub = new NotificationsHubSync<object>();
        var clock = new NodeClock(_clock);
        var service = new DistributedLockService<object>(storage, StorageConfig,
            NullLogger<DistributedLockService<object>>.Instance, hub, LockOptions, clock);
        var node = new Node(service, clock);
        hub.Subscribe<DistributedLockNotification>(n =>
        {
            if (n.LockStatus == DistirbutedLockStatus.Acquried)
                node.Acquired++;
            else
                node.Lost++;
            return Task.CompletedTask;
        });
        _nodes.Add(node);
        return node;
    }

    private async Task<byte[]> ReadLockFile(string storageKind)
    {
        IDiskStorage<object> storage = storageKind == "in-memory"
            ? _inMemory
            : new LocalDiskStorage<object>(NullLogger<LocalDiskStorage<object>>.Instance,
                Options.Create(new LocalDiskStorageConfig { RootFolder = _rootFolder }));
        var lockFile = DatasetHelper.GetObjectNamePrefix<object>(StorageConfig.Value) + "lock-file";
        return (await storage.DownloadBytes(lockFile)).ToArray();
    }

    private static void AssertAtMostOneActive(List<Node> nodes) =>
        nodes.Count(n => !n.Clock.Paused && n.Service.HasLock).Should().BeLessThanOrEqualTo(1);

    /// <summary>
    /// Waits until every service loop is back in its polling delay, so the next step starts from a quiet state.
    /// </summary>
    private static void WaitUntilParked(IReadOnlyCollection<Node> nodes) =>
        SpinWait.SpinUntil(() => nodes.All(n => n.Clock.Parked), TimeSpan.FromSeconds(10))
            .Should().BeTrue("all lock service loops should settle");

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var node in _nodes)
        {
            node.Clock.Resume();
            await node.Service.StopAsync(CancellationToken.None);
            node.Service.Dispose();
        }
        if (Directory.Exists(_rootFolder))
            Directory.Delete(_rootFolder, true);
    }

    private sealed class Node(DistributedLockService<object> service, NodeClock clock)
    {
        public DistributedLockService<object> Service { get; } = service;
        public NodeClock Clock { get; } = clock;
        public int Acquired { get; set; }
        public int Lost { get; set; }
    }

    /// <summary>
    /// Thread-safe virtual time (TestScheduler is not, and LocalDisk continues on thread-pool threads).
    /// Due timers fire synchronously from <see cref="Advance"/>.
    /// </summary>
    private sealed class ManualClock
    {
        private readonly Lock _sync = new();
        private readonly List<(DateTimeOffset Due, Action Fire)> _timers = [];
        private DateTimeOffset _now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

        public DateTimeOffset UtcNow
        {
            get
            {
                lock (_sync)
                    return _now;
            }
        }

        public void Schedule(TimeSpan dueTime, Action fire)
        {
            lock (_sync)
                _timers.Add((_now + dueTime, fire));
        }

        public void Advance(TimeSpan by)
        {
            List<Action> due;
            lock (_sync)
            {
                _now += by;
                due = _timers.Where(t => t.Due <= _now).Select(t => t.Fire).ToList();
                _timers.RemoveAll(t => t.Due <= _now);
            }
            foreach (var fire in due)
                fire();
        }
    }

    /// <summary>
    /// One instance's view of the shared clock. While paused, its polling delay does not end.
    /// <see cref="Parked"/> turns false synchronously when the delay ends, before the service loop continues.
    /// </summary>
    private sealed class NodeClock(ManualClock clock) : IClock
    {
        private readonly Lock _sync = new();
        private TaskCompletionSource? _wake;
        private bool _due;

        public bool Parked
        {
            get
            {
                lock (_sync)
                    return _wake is { Task.IsCompleted: false };
            }
        }

        public bool Paused { get; private set; }
        public DateTimeOffset UtcNow => clock.UtcNow;

        public Task Delay(TimeSpan dueTime, CancellationToken cancellationToken = default)
        {
            var wake = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            lock (_sync)
            {
                _wake = wake;
                _due = false;
            }
            clock.Schedule(dueTime, () =>
            {
                lock (_sync)
                {
                    _due = true;
                    if (!Paused)
                        wake.TrySetResult();
                }
            });
            cancellationToken.Register(() => wake.TrySetCanceled(cancellationToken));
            return wake.Task;
        }

        public void Pause()
        {
            lock (_sync)
                Paused = true;
        }

        public void Resume()
        {
            lock (_sync)
            {
                Paused = false;
                if (_due)
                    _wake?.TrySetResult();
            }
        }
    }
}
