using FluentAssertions;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Notifications;
using RxDatasets.TestImplementations.InMemoryProvider;

namespace RxDatasets.StorageTests.Services;

/// <summary>
/// Two instances of a server with two datasets each, under one shared lock per instance.
/// </summary>
public sealed class SharedDistributedLockTests : IAsyncLifetime
{
    private static readonly IOptions<StorageConfig> StorageConfig = Options.Create(new StorageConfig());

    private static readonly IOptions<DistributedLockOptions> LockOptions = Options.Create(new DistributedLockOptions
    {
        PollingInterval = TimeSpan.FromMilliseconds(50),
        HandoverTimeout = TimeSpan.FromMilliseconds(200),
        RenewalInterval = TimeSpan.FromSeconds(30),
        RenewalThreshold = TimeSpan.FromSeconds(5),
    });

    // The bucket the shared locks of both instances run on; the datasets' own storages are only flushed
    private readonly LowLevelInMemoryStorage<SharedDistributedLock> _bucket =
        new(NullLogger<LowLevelInMemoryStorage<SharedDistributedLock>>.Instance);
    private readonly List<Instance> _instances = [];

    [Fact]
    public async Task A_Handover_Moves_Every_Dataset_At_Once()
    {
        var a = await StartInstance();
        WaitUntil(() => a.AllHold(DistirbutedLockStatus.Acquried));

        var b = await StartInstance(); // asks A for the lock
        using var sampler = new CancellationTokenSource();
        var bothHeld = Task.Run(() =>
        {
            while (!sampler.IsCancellationRequested)
            {
                if (a.Lock.HasLock && b.Lock.HasLock)
                    return true;
                Thread.Yield();
            }
            return false;
        });
        WaitUntil(() => b.AllHold(DistirbutedLockStatus.Acquried));
        await sampler.CancelAsync();

        (await bothHeld).Should().BeFalse("only one instance holds the shared lock at a time");
        WaitUntil(() => a.AllHold(DistirbutedLockStatus.Lost)); // every dataset of A gives up writing
        a.Members.Should().OnlyContain(m => !m.HasLock);

        // B stops and releases; A takes over again with all its datasets
        await b.StopAsync();
        WaitUntil(() => a.AllHold(DistirbutedLockStatus.Acquried));
        a.Members.Should().OnlyContain(m => m.HasLock);
    }

    [Fact]
    public async Task The_Lock_Runs_On_Its_Own_Storage_Not_On_A_Datasets()
    {
        var a = await StartInstance();
        WaitUntil(() => a.AllHold(DistirbutedLockStatus.Acquried));

        // The dataset that joined first leaves, and its storage with it (a host that disposes its container)
        await a.Datasets[0].Member.StopAsync(CancellationToken.None);
        a.Storages[0].Invocations.Clear();

        var b = await StartInstance();
        WaitUntil(() => b.AllHold(DistirbutedLockStatus.Acquried));
        (await _bucket.DownloadBytes("SharedLock/lock-file")).IsEmpty.Should().BeFalse();
        a.Storages[0].Invocations.Should().BeEmpty("the lock never touches a dataset's storage");
    }

    [Fact]
    public async Task A_Dataset_That_Joins_Later_Inherits_The_Current_State()
    {
        var a = await StartInstance();
        WaitUntil(() => a.AllHold(DistirbutedLockStatus.Acquried));

        var (late, lastStatus, _) = Member<int>(a.Lock);
        await late.StartAsync(CancellationToken.None);

        WaitUntil(() => lastStatus() == DistirbutedLockStatus.Acquried);
        late.HasLock.Should().BeTrue();
        await late.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task A_Dataset_That_Joins_After_The_Lock_Stopped_Is_Not_Told_It_Holds_It()
    {
        var a = await StartInstance();
        WaitUntil(() => a.AllHold(DistirbutedLockStatus.Acquried));
        await a.StopAsync();

        var (late, lastStatus, _) = Member<int>(a.Lock);
        await late.StartAsync(CancellationToken.None);
        await Task.Delay(200); // the hub would replay at once

        lastStatus().Should().BeNull();
        late.HasLock.Should().BeFalse();
    }

    [Fact]
    public async Task Stopping_The_Lock_First_Releases_It_Only_After_The_Last_Dataset_Flushed_And_Left()
    {
        var a = await StartInstance();
        WaitUntil(() => a.AllHold(DistirbutedLockStatus.Acquried));
        var heldWhileFlushing = false;
        var storage = new Mock<IDiskStorage<int>>(MockBehavior.Strict);
        storage.Setup(s => s.FlushAsync()).Returns(() =>
        {
            heldWhileFlushing = a.Lock.HasLock;
            return Task.CompletedTask;
        });
        var last = new SharedDistributedLockMember<int>(a.Lock, storage.Object, new NotificationsHub<int>(),
            NullLogger<SharedDistributedLockMember<int>>.Instance);
        await last.StartAsync(CancellationToken.None);

        // As a host that stops its services concurrently, or registered the lock after the datasets
        var stopping = a.Lock.StopAsync(CancellationToken.None);
        foreach (var (member, _) in a.Datasets)
            await member.StopAsync(CancellationToken.None);
        await Task.Delay(200);
        stopping.IsCompleted.Should().BeFalse("a dataset is still writing");
        a.Lock.HasLock.Should().BeTrue();

        await last.StopAsync(CancellationToken.None);
        await stopping.WaitAsync(TimeSpan.FromSeconds(10));
        heldWhileFlushing.Should().BeTrue("a dataset flushes before the lock is released");
        a.Lock.HasLock.Should().BeFalse();
    }

    [Fact]
    public async Task A_Dataset_Whose_Flush_Fails_Still_Leaves()
    {
        var a = await StartInstance();
        var storage = new Mock<IDiskStorage<int>>(MockBehavior.Strict);
        storage.Setup(s => s.FlushAsync()).ThrowsAsync(new InvalidOperationException("storage gone"));
        var failing = new SharedDistributedLockMember<int>(a.Lock, storage.Object, new NotificationsHub<int>(),
            NullLogger<SharedDistributedLockMember<int>>.Instance);
        await failing.StartAsync(CancellationToken.None);

        await failing.Invoking(m => m.StopAsync(CancellationToken.None)).Should().NotThrowAsync();
        await a.StopAsync().WaitAsync(TimeSpan.FromSeconds(10)); // the lock no longer waits for it
    }

    [Fact]
    public async Task Stopping_The_Lock_Releases_It_When_The_Stop_Times_Out()
    {
        var a = await StartInstance();
        WaitUntil(() => a.AllHold(DistirbutedLockStatus.Acquried));

        using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        await a.Lock.StopAsync(timeout.Token).WaitAsync(TimeSpan.FromSeconds(10)); // its datasets never stop

        a.Lock.HasLock.Should().BeFalse();
    }

    [Fact]
    public void A_Dataset_Cannot_Have_Both_Its_Own_And_The_Shared_Lock()
    {
        var build = () => new ServiceCollection().AddDtoServer<StringValue>()
            .WithDistributedLock().WithSharedDistributedLock().Build();

        build.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public async Task The_Registrations_Resolve_And_Hold_The_Lock()
    {
        var clock = new CountingClock();
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddLogging();
        services.AddSingleton<IClock>(clock);
        services.AddSharedDistributedLock<LowLevelInMemoryStorage<SharedDistributedLock>>("MyServerLock");
        services.AddSingleton<IDiskStorage<StringValue>, LowLevelInMemoryStorage<StringValue>>();
        services.AddDtoServer<StringValue>().WithSharedDistributedLock().Build("readiness");
        await using var provider = services.BuildServiceProvider();

        var hostedServices = provider.GetServices<IHostedService>().ToList();
        var sharedLock = provider.GetRequiredService<SharedDistributedLock>();
        var datasetLock = provider.GetRequiredService<IDistributedLock<StringValue>>();
        datasetLock.Should().BeOfType<SharedDistributedLockMember<StringValue>>();
        hostedServices.Should().Contain(sharedLock).And.Contain((IHostedService)datasetLock);
        provider.GetRequiredService<IDiskStorage<SharedDistributedLock>>().Should()
            .BeSameAs(provider.GetRequiredService<IConditionalDiskStorage<SharedDistributedLock>>(),
                "a provider's health check watches the lock's storage through it");

        await sharedLock.StartAsync(CancellationToken.None);
        await ((IHostedService)datasetLock).StartAsync(CancellationToken.None);
        WaitUntil(() => datasetLock.HasLock);
        var health = await provider.GetRequiredService<HealthCheckService>()
            .CheckHealthAsync(r => r.Name == "DistributedLockHealthCheck-StringValue");
        health.Entries.Should().ContainSingle().Which.Value.Status.Should().Be(HealthStatus.Healthy);
        clock.Calls.Should().BePositive("the lock uses the IClock of the service collection");
        (await provider.GetRequiredService<IConditionalDiskStorage<SharedDistributedLock>>()
            .DownloadBytes("MyServerLock/lock-file")).IsEmpty.Should().BeFalse();

        await ((IHostedService)datasetLock).StopAsync(CancellationToken.None);
        await sharedLock.StopAsync(CancellationToken.None);
    }

    private async Task<Instance> StartInstance()
    {
        var sharedLock = new SharedDistributedLock(_bucket, StorageConfig, "SharedLock", NullLoggerFactory.Instance,
            LockOptions);
        var instance = new Instance(sharedLock, Member<object>(sharedLock), Member<string>(sharedLock));
        _instances.Add(instance);
        await sharedLock.StartAsync(CancellationToken.None);
        foreach (var (member, _) in instance.Datasets)
            await member.StartAsync(CancellationToken.None);
        return instance;
    }

    // A dataset's storage that only allows a flush: anything else throws
    private static (SharedDistributedLockMember<T> Member, Func<DistirbutedLockStatus?> LastStatus, Mock Storage)
        Member<T>(SharedDistributedLock sharedLock)
    {
        var storage = new Mock<IDiskStorage<T>>(MockBehavior.Strict);
        storage.Setup(s => s.FlushAsync()).Returns(Task.CompletedTask);
        var hub = new NotificationsHub<T>();
        DistirbutedLockStatus? last = null;
        hub.Subscribe<DistributedLockNotification>(n =>
        {
            last = n.LockStatus;
            return Task.CompletedTask;
        });
        return (new SharedDistributedLockMember<T>(sharedLock, storage.Object, hub,
            NullLogger<SharedDistributedLockMember<T>>.Instance), () => last, storage);
    }

    private static void WaitUntil(Func<bool> condition) =>
        SpinWait.SpinUntil(condition, TimeSpan.FromSeconds(10)).Should().BeTrue();

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var instance in _instances)
        {
            await instance.StopAsync();
            instance.Lock.Dispose();
        }
    }

    private sealed class CountingClock : IClock
    {
        private int _calls;
        public int Calls => _calls;

        public DateTimeOffset UtcNow
        {
            get
            {
                Interlocked.Increment(ref _calls);
                return DateTimeOffset.UtcNow;
            }
        }

        public Task Delay(TimeSpan dueTime, CancellationToken cancellationToken = default) =>
            Task.Delay(dueTime, cancellationToken);
    }

    private sealed class Instance
    {
        public Instance(SharedDistributedLock sharedLock,
            params (IDistributedLock Member, Func<DistirbutedLockStatus?> LastStatus, Mock Storage)[] datasets)
        {
            Lock = sharedLock;
            Datasets = datasets.Select(d => ((IHostedService)d.Member, d.LastStatus)).ToList();
            Members = datasets.Select(d => d.Member).ToList();
            Storages = datasets.Select(d => d.Storage).ToList();
        }

        public SharedDistributedLock Lock { get; }
        public List<(IHostedService Member, Func<DistirbutedLockStatus?> LastStatus)> Datasets { get; }
        public List<IDistributedLock> Members { get; }
        public List<Mock> Storages { get; }

        public bool AllHold(DistirbutedLockStatus status) => Datasets.All(d => d.LastStatus() == status);

        public async Task StopAsync()
        {
            // The datasets first, then the shared lock, which releases it (it would wait for them otherwise)
            foreach (var (member, _) in Datasets)
                await member.StopAsync(CancellationToken.None);
            await Lock.StopAsync(CancellationToken.None);
        }
    }
}
