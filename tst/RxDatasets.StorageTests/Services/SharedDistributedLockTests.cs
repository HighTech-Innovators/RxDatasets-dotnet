using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
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

    // One bucket: the lock file lives on the storage of the dataset that joins first
    private readonly LowLevelInMemoryStorage<object> _bucket = new(NullLogger<LowLevelInMemoryStorage<object>>.Instance);
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
                if (a.Lock.HasLock && b.Lock.HasLock)
                    return true;
            return false;
        });
        WaitUntil(() => b.AllHold(DistirbutedLockStatus.Acquried));
        await sampler.CancelAsync();

        (await bothHeld).Should().BeFalse("only one instance holds the shared lock at a time");
        a.AllHold(DistirbutedLockStatus.Lost).Should().BeTrue("every dataset of A gives up writing");
        a.Members.Should().OnlyContain(m => !m.HasLock);

        // B stops and releases; A takes over again with all its datasets
        await b.StopAsync();
        WaitUntil(() => a.AllHold(DistirbutedLockStatus.Acquried));
        a.Members.Should().OnlyContain(m => m.HasLock);
    }

    [Fact]
    public async Task A_Dataset_That_Joins_Later_Inherits_The_Current_State()
    {
        var a = await StartInstance();
        WaitUntil(() => a.AllHold(DistirbutedLockStatus.Acquried));

        var hub = new NotificationsHub<int>();
        DistirbutedLockStatus? received = null;
        hub.Subscribe<DistributedLockNotification>(n =>
        {
            received = n.LockStatus;
            return Task.CompletedTask;
        });
        var late = new SharedDistributedLockMember<int>(a.Lock,
            new LowLevelInMemoryStorage<int>(NullLogger<LowLevelInMemoryStorage<int>>.Instance), StorageConfig, hub);
        await late.StartAsync(CancellationToken.None);

        WaitUntil(() => received == DistirbutedLockStatus.Acquried);
        late.HasLock.Should().BeTrue();
        await late.StopAsync(CancellationToken.None);
    }

    [Fact]
    public void A_Dataset_Cannot_Have_Both_Its_Own_And_The_Shared_Lock()
    {
        var build = () => new ServiceCollection().AddDtoServer<Google.Protobuf.WellKnownTypes.StringValue>()
            .WithDistributedLock().WithSharedDistributedLock().Build();

        build.Should().Throw<InvalidOperationException>();
    }

    private async Task<Instance> StartInstance()
    {
        var sharedLock = new SharedDistributedLock("SharedLock", NullLoggerFactory.Instance, LockOptions);
        var instance = new Instance(sharedLock,
            Member<object>(sharedLock, _bucket),
            Member<string>(sharedLock, new LowLevelInMemoryStorage<string>(NullLogger<LowLevelInMemoryStorage<string>>.Instance)));
        _instances.Add(instance);
        await sharedLock.StartAsync(CancellationToken.None);
        foreach (var (member, _) in instance.Datasets)
            await member.StartAsync(CancellationToken.None);
        return instance;
    }

    private static (SharedDistributedLockMember<T> Member, Func<DistirbutedLockStatus?> LastStatus) Member<T>(
        SharedDistributedLock sharedLock, IDiskStorage<T> storage)
    {
        var hub = new NotificationsHub<T>();
        DistirbutedLockStatus? last = null;
        hub.Subscribe<DistributedLockNotification>(n =>
        {
            last = n.LockStatus;
            return Task.CompletedTask;
        });
        return (new SharedDistributedLockMember<T>(sharedLock, storage, StorageConfig, hub), () => last);
    }

    private static void WaitUntil(Func<bool> condition) =>
        SpinWait.SpinUntil(condition, TimeSpan.FromSeconds(10)).Should().BeTrue();

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var instance in _instances)
            await instance.StopAsync();
    }

    private sealed class Instance
    {
        public Instance(SharedDistributedLock sharedLock,
            params (IDistributedLock Member, Func<DistirbutedLockStatus?> LastStatus)[] datasets)
        {
            Lock = sharedLock;
            Datasets = datasets.Select(d => ((Microsoft.Extensions.Hosting.IHostedService)d.Member, d.LastStatus)).ToList();
            Members = datasets.Select(d => d.Member).ToList();
        }

        public SharedDistributedLock Lock { get; }
        public List<(Microsoft.Extensions.Hosting.IHostedService Member, Func<DistirbutedLockStatus?> LastStatus)> Datasets { get; }
        public List<IDistributedLock> Members { get; }

        public bool AllHold(DistirbutedLockStatus status) => Datasets.All(d => d.LastStatus() == status);

        public async Task StopAsync()
        {
            // As a host does: the datasets first, then the shared lock, which releases it
            foreach (var (member, _) in Datasets)
                await member.StopAsync(CancellationToken.None);
            await Lock.StopAsync(CancellationToken.None);
        }
    }
}
