using System.Reactive.Concurrency;
using System.Runtime.CompilerServices;
using DotNet.Testcontainers;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Reactive.Testing;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Notifications;
using RxDatasets.StorageProvider.LocalDisk;
using RxDatasets.StorageProviders;
using RxDatasets.TestImplementations.InMemoryProvider;

namespace RxDatasets.StorageTests.Services;

public class StorageCleanTaskTests
{
    private readonly IOptions<SnapshotCreationConfig> _snapshotsOptions = Options.Create(new SnapshotCreationConfig
    {
        SnapshotInterval = 5,
    });

    private IOptions<CleanupConfig> _storageCleanupConfig = Options.Create(new CleanupConfig() { KeepAllDeltas = false });


    private const ulong UNIT_KB = 1024;

    private readonly ILoggerFactory logFactory = LoggerFactory.Create(builder =>
      {
          builder
              .SetMinimumLevel(LogLevel.Trace)              // be as chatty as you want
              .AddSimpleConsole(o =>                        // prettier console output
              {
                  o.SingleLine = true;
                  o.TimestampFormat = "HH:mm:ss.fff ";
                  o.IncludeScopes = true;
              });
      });

    private async Task<(IStorageProvider<object>, DatasetReader<object>, DatasetWriter<object>)> SetupDataset(TestScheduler ts)
    {
        INotificationsHub<object> notificationsHub = new NotificationsHubSync<object>();
        IStorageProvider<object> inMemoryStorage = new InMemoryStorageProvider<object>(logFactory.CreateLogger<InMemoryStorageProvider<object>>(), notificationsHub);

        var writerLogger = new Mock<ILogger<DatasetWriter<object>>>();
        var storageCleanerLogger = logFactory.CreateLogger<StorageCleaningService<object>>();
        var snapshotCreationPlugInLogger = logFactory.CreateLogger<SnapshotCreationPlugin<object>>();
        var snapshotCreationServiceLogger = logFactory.CreateLogger<DatasetUpdateConsumingService<object, SnapshotCreationPlugin<object>>>();

        var datasetReader = new DatasetReader<object>(Mock.Of<ILogger<DatasetReader<object>>>(), inMemoryStorage, notificationsHub);
        await datasetReader.StartAsync(CancellationToken.None);
        var datasetWriter = new DatasetWriter<object>(notificationsHub, writerLogger.Object, inMemoryStorage, Options.Create(new WriterConfig { }), false);

        var storageCleaningTask = new StorageCleaningService<object>(notificationsHub, inMemoryStorage, ts, _storageCleanupConfig, storageCleanerLogger);
        await storageCleaningTask.StartAsync(CancellationToken.None);

        var snapshotCreationPlugin = new SnapshotCreationPlugin<object>(snapshotCreationPlugInLogger, inMemoryStorage, _snapshotsOptions, ts);
        var snapshotCreationTask = new DatasetUpdateConsumingService<object, SnapshotCreationPlugin<object>>(snapshotCreationServiceLogger, notificationsHub, snapshotCreationPlugin);
        await snapshotCreationTask.StartAsync(CancellationToken.None);

        await notificationsHub.Notify(new StorageProviderReadinessNotifcation(ServiceStatus.Ready));

        return (inMemoryStorage, datasetReader, datasetWriter);
    }


    private async Task<(IDiskStorage<object>, IStorageProvider<object>, DatasetReader<object>, DatasetWriter<object>, IOptions<LocalDiskStorageConfig>)> SetupLocalDiskDataset(TestScheduler ts, [CallerMemberName] string memberName = "")
    {
        var config = Options.Create(new LocalDiskStorageConfig());
        config.Value.RootFolder = Path.Combine(DatasetHelper.GetTempFileDirectory(), $"UnitTestsData_CleaningService/{memberName}");

        if (Directory.Exists(config.Value.RootFolder))
        {
            Directory.Delete(config.Value.RootFolder, recursive: true);
        }

        IDiskStorage<object> localDiskStorage = new LocalDiskStorage<object>(logFactory.CreateLogger<LocalDiskStorage<object>>(), config);
        INotificationsHub<object> notificationsHub = new NotificationsHub<object>();
        var storageConfig = Options.Create(new StorageConfig());
        IStorageReader<object> readOnlyStorage = new ObjectStorageReader<object>(logFactory.CreateLogger<ObjectStorageReader<object>>(), storageConfig, localDiskStorage);

        IStorageProvider<object> storageProvider = new ObjectStorageProvider<object>(logFactory.CreateLogger<ObjectStorageProvider<object>>(), storageConfig, localDiskStorage, notificationsHub, readOnlyStorage, false);
        await ((ObjectStorageProvider<object>)storageProvider).StartAsync(CancellationToken.None);
        var writerLogger = new Mock<ILogger<DatasetWriter<object>>>();


        var datasetReader = new DatasetReader<object>(Mock.Of<ILogger<DatasetReader<object>>>(), storageProvider, notificationsHub);
        await datasetReader.StartAsync(CancellationToken.None);
        var datasetWriter = new DatasetWriter<object>(notificationsHub, writerLogger.Object, storageProvider, Options.Create(new WriterConfig { }), false);

        var storageCleanerLogger = logFactory.CreateLogger<StorageCleaningService<object>>();
        var storageCleaningTask = new StorageCleaningService<object>(notificationsHub, storageProvider, ts, _storageCleanupConfig, storageCleanerLogger);
        await storageCleaningTask.StartAsync(CancellationToken.None);

        var snapshotCreationPlugInLogger = logFactory.CreateLogger<SnapshotCreationPlugin<object>>();
        var snapshotCreationServiceLogger = logFactory.CreateLogger<DatasetUpdateConsumingService<object, SnapshotCreationPlugin<object>>>();
        var snapshotCreationPlugin = new SnapshotCreationPlugin<object>(snapshotCreationPlugInLogger, storageProvider, _snapshotsOptions, ts);
        var snapshotCreationTask = new DatasetUpdateConsumingService<object, SnapshotCreationPlugin<object>>(snapshotCreationServiceLogger, notificationsHub, snapshotCreationPlugin);
        await snapshotCreationTask.StartAsync(CancellationToken.None);
        await Task.Delay(TimeSpan.FromSeconds(2));

        return (localDiskStorage, storageProvider, datasetReader, datasetWriter, config);
    }

    [Fact]
    public async Task Should_Delete_Old_Snapshots_OnLocalStorage()
    {
        var cleanupConfigMock = new Mock<CleanupConfig>();
        cleanupConfigMock.Setup(c => c.GetMinimumAgeToLiveInMinutes()).Returns(TimeSpan.FromMinutes(0));
        cleanupConfigMock.Object.KeepAllDeltas = false;

        _storageCleanupConfig = Options.Create(cleanupConfigMock.Object);
        var scheduler = new TestScheduler();
        var (localStorage, storageProvider, _, datasetWriter, config) = await SetupLocalDiskDataset(scheduler);

        scheduler.AdvanceTo(DateTime.UtcNow.Ticks);

        scheduler.AdvanceBy(TimeSpan.FromSeconds(5).Ticks);
        var randomBytes = GenerateRandomData(UNIT_KB, 1);
        await datasetWriter?.Put("Same_DTO", randomBytes)!; //creates 1st snapshot

        await WaitForANumberOfSnapshotsToBeCreated(storageProvider, 1);
        ulong N = 5;
        await CreateNumberOf1Deltas(datasetWriter, 5 + (int)N); //creates 6th snapshot

        var snapshots = await WaitForANumberOfSnapshotsToBeCreated(storageProvider, 2);

        ulong firstSnapshot = snapshots[^2].Version;
        ulong secondSnapshot = snapshots[^1].Version;

        await CreateNumberOf1Deltas(datasetWriter, 5); //creates 6th snapshot
        await storageProvider.StoreProcessedVersion("unittest", 10 + N);
        await Task.Delay(TimeSpan.FromSeconds(0.2));

        //act
        await CreateNumberOf1Deltas(datasetWriter, 5); //creates 6th snapshot
        await storageProvider.StoreProcessedVersion("unittest", 15 + N);
        await Task.Delay(TimeSpan.FromSeconds(0.2));


        //assert
        (var oldestDelta, var newestDelta, snapshots) = await storageProvider.ListVersions();
        const int expectedRemainingSnapshots = 2;
        var expectedRemainingDeltas = newestDelta - oldestDelta + 1;
        var listObjects = (await localStorage.ListObjects("Object/" + FolderNames.SnapshotsFolderNameWithSlash)).GroupBy(s => SharedMethods.ExtractNumber(s.ObjectKey)).Select(g => g.Key);
        var listDeltaObjects = (await localStorage.ListObjects("Object/" + FolderNames.DeltasFolderNameWithSlash)).GroupBy(s => SharedMethods.ExtractNumber(s.ObjectKey)).Select(g => g.Key);

        snapshots.Count.Should().Be(expectedRemainingSnapshots);
        listObjects.Count().Should().Be(expectedRemainingSnapshots);
        listDeltaObjects.Count().Should().Be((int)expectedRemainingDeltas);
        snapshots[0].Version.Should().NotBe(firstSnapshot);
        snapshots[1].Version.Should().NotBe(secondSnapshot);
        oldestDelta.Should().Be(snapshots[0].Version + 1, $"Because if earliest snapshot is 11, then the first delta after it is 12");

        if (Directory.Exists(config.Value.RootFolder))
        {
            Directory.Delete(config.Value.RootFolder, recursive: true);
        }
    }

    private static async Task<List<SnapshotType>> WaitForANumberOfSnapshotsToBeCreated(IStorageProvider<object> storageProvider, int howManyShouldBeThere)
    {
        int counter = 0;
        while (true)
        {
            var (_, _, snapshots) = await storageProvider.ListVersions();
            if (snapshots.Count >= howManyShouldBeThere)
                return snapshots;
            await Task.Delay(TimeSpan.FromSeconds(0.8));
            counter++;
            if (counter == 50)
            {
                Assert.Fail($"Could not get the needed number of snapshots {howManyShouldBeThere} but we got {snapshots.Count}");
            }
        }
    }

    private static byte[] GenerateRandomData(ulong length = UNIT_KB, int seed = 0)
    {
        var randomData = new byte[length];
        var random = new Random(seed);

        random.NextBytes(randomData);
        return randomData;
    }

    [Fact]
    public async Task Should_Delete_Old_Snapshots()
    {
        var cleanupConfigMock = new Mock<CleanupConfig>();
        cleanupConfigMock.Setup(c => c.GetMinimumAgeToLiveInMinutes()).Returns(TimeSpan.FromMinutes(0));
        cleanupConfigMock.Object.KeepAllDeltas = false;
        _storageCleanupConfig = Options.Create(cleanupConfigMock.Object);
        var scheduler = new TestScheduler();
        var (storageProvider, _, datasetWriter) = await SetupDataset(scheduler);

        scheduler.AdvanceTo(DateTime.UtcNow.Ticks);

        scheduler.AdvanceBy(TimeSpan.FromSeconds(5).Ticks);

        await Create6Deltas(datasetWriter);
        await Task.Delay(TimeSpan.FromSeconds(.1));

        var (oldestDelta, _, snapshots) = await storageProvider.ListVersions();
        var firstSnapshot = snapshots[0].Version;

        await Create6Deltas(datasetWriter);
        await Task.Delay(TimeSpan.FromSeconds(.1));

        var secondSnapshot = snapshots[1].Version;

        await storageProvider.StoreProcessedVersion("unittest", 10);
        await Task.Delay(TimeSpan.FromSeconds(.1));

        //act
        await Create6Deltas(datasetWriter);
        await storageProvider.StoreProcessedVersion("unittest", 15);
        await Task.Delay(TimeSpan.FromSeconds(.1));


        //assert
        (oldestDelta, _, snapshots) = await storageProvider.ListVersions();
        const int expectedRemainingSnapshots = 2;

        snapshots.Count.Should().Be(expectedRemainingSnapshots);
        snapshots[0].Version.Should().NotBe(firstSnapshot);
        snapshots[1].Version.Should().NotBe(secondSnapshot);
        oldestDelta.Should().BeCloseTo(snapshots[0].Version, 5, "we are not sure because of async when the snapshot will be exactly created");
    }


    [Fact]
    public async Task Should_Delete_Old_Snapshots_After_Some_Time()
    {
        var cleanupConfigMock = new Mock<CleanupConfig>
        {
            Object =
            {
                KeepAllDeltas = false
            }
        };

        const int minimumTimeToLiveInMinutes = 5;
        var scheduler = new TestScheduler();
        cleanupConfigMock.Setup(c => c.GetMinimumAgeToLiveInMinutes()).Returns(TimeSpan.FromMinutes(minimumTimeToLiveInMinutes));
        _storageCleanupConfig = Options.Create(cleanupConfigMock.Object);

        var (storageProvider, _, datasetWriter) = await SetupDataset(scheduler);
        await storageProvider.StoreProcessedVersion("unittest", 0);

        scheduler.AdvanceTo(DateTime.UtcNow.Ticks);
        const int fourSnapshots = 4;
        var deltasInOneSnapshot = (int)_snapshotsOptions.Value.SnapshotInterval;
        var howManyDeltasToCreate = deltasInOneSnapshot * fourSnapshots; //4 to make sure that there are enough deltas, but also, avoid triggering the cleanup, more than once for testing purposes
        scheduler.Schedule(TimeSpan.FromMinutes(3), () => Create6Deltas(datasetWriter).Wait());
        scheduler.Schedule(TimeSpan.FromMinutes(6), () => Create6Deltas(datasetWriter).Wait());
        await storageProvider.StoreProcessedVersion("unittest", 10);
        await Task.Delay(TimeSpan.FromSeconds(.1));

        //create 3 snapshots as a setup with 18 deltas
        await Create6Deltas(datasetWriter);
        await Task.Delay(TimeSpan.FromSeconds(.2));

        await Create6Deltas(datasetWriter);
        await Task.Delay(TimeSpan.FromSeconds(.2));

        await Create6Deltas(datasetWriter);
        await storageProvider.StoreProcessedVersion("unittest", 25);
        await Task.Delay(TimeSpan.FromSeconds(.2));

        //assert nothing is deleted even tho the threshold for snapshots is met at minimum 2 snapshots
        var (oldestDelta, newestDelta, snapshots) = await storageProvider.ListVersions();

        snapshots.Count.Should().Be(fourSnapshots);
        oldestDelta.Should().Be(snapshots[0].Version);


        // at 3 minutes we still have not deleted anything, however, we added one more snapshot (i.e 6 deltas)
        scheduler.AdvanceBy(TimeSpan.FromMinutes(4).Ticks);
        await Task.Delay(TimeSpan.FromSeconds(.1));

        (oldestDelta, _, snapshots) = await storageProvider.ListVersions();
        int fiveSnapshots = 5;
        snapshots.Count.Should().Be(fiveSnapshots);
        oldestDelta.Should().Be(snapshots[0].Version);

        //act
        scheduler.AdvanceBy(TimeSpan.FromMinutes(6).Ticks);
        await Task.Delay(TimeSpan.FromSeconds(.1));


        //assert
        const int expectedRemainingSnapshots = 2;
        (oldestDelta, newestDelta, snapshots) = await storageProvider.ListVersions();
        snapshots.Count.Should().Be(expectedRemainingSnapshots);
        oldestDelta.Should().BeCloseTo(snapshots[0].Version, 5, "we are not sure because of async when the snapshot will be exactly created");
    }


    [Fact]
    public async Task Should_Not_Delete_Old_Snapshots_When_Time_Exceeds_But_Number_Of_Snapshots_Doesnt()
    {
        var cleanupConfigMock = new Mock<CleanupConfig>();
        cleanupConfigMock.Setup(c => c.GetMinimumAgeToLiveInMinutes()).Returns(TimeSpan.FromMinutes(1));
        _storageCleanupConfig = Options.Create(cleanupConfigMock.Object);
        _storageCleanupConfig.Value.MinimumSnapshotsToKeep = 5;
        var scheduler = new TestScheduler();
        var (storageProvider, _, datasetWriter) = await SetupDataset(scheduler);


        scheduler.AdvanceTo(DateTime.UtcNow.Ticks);

        scheduler.AdvanceBy(TimeSpan.FromMinutes(3).Ticks);

        //act
        await Create6Deltas(datasetWriter);
        await Task.Delay(TimeSpan.FromSeconds(.1));

        await Create6Deltas(datasetWriter);
        await Task.Delay(TimeSpan.FromSeconds(.1));

        await Create6Deltas(datasetWriter);
        await Task.Delay(TimeSpan.FromSeconds(.1));
        await Create6Deltas(datasetWriter);
        await Task.Delay(TimeSpan.FromSeconds(.1));
        await storageProvider.StoreProcessedVersion("unittest", 21);

        //assert
        var (oldestDelta, newestDelta, snapshots) = await storageProvider.ListVersions();
        const int expectedRemainingSnapshots = 5;

        snapshots.Count.Should().Be(expectedRemainingSnapshots);
        oldestDelta.Should().Be(1);
        newestDelta.Should().Be(24);
    }

    [Fact]
    public async Task Should_Not_Delete_Old_Snapshots_When_Number_Of_Snapshots_Exceeds_But_Minimum_Time_Doesnt()
    {
        var cleanupConfigMock = new Mock<CleanupConfig>();
        cleanupConfigMock.Setup(c => c.GetMinimumAgeToLiveInMinutes()).Returns(TimeSpan.FromMinutes(1));
        _storageCleanupConfig = Options.Create(cleanupConfigMock.Object);
        var scheduler = new TestScheduler();
        var (storageProvider, _, datasetWriter) = await SetupDataset(scheduler);

        scheduler.AdvanceTo(DateTime.UtcNow.Ticks);

        scheduler.AdvanceBy(TimeSpan.FromSeconds(5).Ticks);

        await Create6Deltas(datasetWriter);
        await Task.Delay(TimeSpan.FromSeconds(.1));
        await Create6Deltas(datasetWriter);
        await Task.Delay(TimeSpan.FromSeconds(.1));

        //act
        await Create6Deltas(datasetWriter);
        await Task.Delay(TimeSpan.FromSeconds(.1));
        await Create6Deltas(datasetWriter);
        await Task.Delay(TimeSpan.FromSeconds(.1));
        await Create6Deltas(datasetWriter);
        await Task.Delay(TimeSpan.FromSeconds(.1));

        //assert
        var (oldestDelta, newestDelta, snapshots) = await storageProvider.ListVersions();
        const int expectedRemainingSnapshots = 6;

        snapshots.Count.Should().Be(expectedRemainingSnapshots);
        _storageCleanupConfig.Value.MinimumSnapshotsToKeep.Should().BeLessThan(expectedRemainingSnapshots);
        oldestDelta.Should().Be(1);
        newestDelta.Should().Be(30);
    }


    [Fact]
    public async Task Should_Not_Delete_Old_Snapshots_After_Some_Time_If_Minimum_Processed_Version_Not_Met()
    {
        var cleanupConfigMock = new Mock<CleanupConfig>
        {
            Object =
            {
                KeepAllDeltas = false
            }
        };

        const int minimumTimeToLiveInMinutes = 5;
        var scheduler = new TestScheduler();
        cleanupConfigMock.Setup(c => c.GetMinimumAgeToLiveInMinutes()).Returns(TimeSpan.FromMinutes(minimumTimeToLiveInMinutes));
        _storageCleanupConfig = Options.Create(cleanupConfigMock.Object);

        var (storageProvider, _, datasetWriter) = await SetupDataset(scheduler);

        scheduler.AdvanceTo(DateTime.UtcNow.Ticks);
        const int threeSnapshots = 3;
        var deltasInOneSnapshot = (int)_snapshotsOptions.Value.SnapshotInterval;
        var howManyDeltasToCreate = deltasInOneSnapshot * threeSnapshots; //3
        scheduler.Schedule(TimeSpan.FromMinutes(3), () => Create6Deltas(datasetWriter).Wait());
        scheduler.Schedule(TimeSpan.FromMinutes(6), () => Create6Deltas(datasetWriter).Wait());
        await Task.Delay(TimeSpan.FromSeconds(.1));

        //create 3 snapshots as a setup with 18 deltas
        await Create6Deltas(datasetWriter);
        await Task.Delay(TimeSpan.FromSeconds(.2));

        await Create6Deltas(datasetWriter);
        await Task.Delay(TimeSpan.FromSeconds(.2));

        //assert nothing is deleted even tho the threshold for snapshots is met at minimum 2 snapshots
        var (oldestDelta, newestDelta, snapshots) = await storageProvider.ListVersions();

        snapshots.Count.Should().Be(threeSnapshots);
        oldestDelta.Should().Be(snapshots[0].Version);


        // at 3 minutes we still have not deleted anything, however, we added one more snapshot (i.e 6 deltas)
        scheduler.AdvanceBy(TimeSpan.FromMinutes(4).Ticks);
        await Task.Delay(TimeSpan.FromSeconds(.1));

        (oldestDelta, _, snapshots) = await storageProvider.ListVersions();
        int fourSnapshots = 4;
        snapshots.Count.Should().Be(fourSnapshots);
        oldestDelta.Should().Be(snapshots[0].Version);

        //act
        scheduler.AdvanceBy(TimeSpan.FromMinutes(5).Ticks); //create snapshot 5 here after time interval passes
        await Task.Delay(TimeSpan.FromSeconds(.1));

        //assert
        (oldestDelta, newestDelta, snapshots) = await storageProvider.ListVersions();
        const int expectedRemainingSnapshots = 5; // 1, 6, 11, 16, 21 -> something like this

        snapshots.Count.Should().Be(expectedRemainingSnapshots);
        oldestDelta.Should().Be(1);
        newestDelta.Should().Be(24);
    }

    private static async Task Create6Deltas(DatasetWriter<object>? datasetWriter)
    {
        await CreateNumberOf1Deltas(datasetWriter, 6);
    }

    private static async Task CreateNumberOf1Deltas(DatasetWriter<object>? datasetWriter, int numberOfDeltasNeeded)
    {
        for (var i = 0; i < numberOfDeltasNeeded; i++)
        {
            if (i % 2 == 0)
            {
                var randomBytes = GenerateRandomData(UNIT_KB, i);

                await datasetWriter?.Put("Same_DTO", randomBytes)!;
            }
            else
            {

                var randomBytes2 = GenerateRandomData(UNIT_KB, 1);
                await datasetWriter?.Put("Same_DTO", randomBytes2)!;
            }
        }
    }
}
