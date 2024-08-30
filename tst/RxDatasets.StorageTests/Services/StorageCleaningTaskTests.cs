using System.Reactive.Concurrency;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Reactive.Testing;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.Configs;
using RxDatasets.DatasetUpdateNotifications;
using RxDatasets.StorageProviders;

namespace RxDatasets.StorageTests.Services;

public class StorageCleanTaskTests 
{
    private readonly IOptions<StorageConfig> _dtoConfigOptions = Options.Create(new StorageConfig());
    private readonly IOptions<SnapshotCreationConfig> _snapshotsOptions = Options.Create(new SnapshotCreationConfig
    {
        SnapshotInterval = 5,
    });

    private IOptions<CleanupConfig> _storageCleanupConfig = Options.Create(new CleanupConfig());
    private readonly DatasetUpdateNotifierSync<object> _datasetUpdateNotifier = new();
    private const ulong UNIT_KB = 1024;
    private const ulong UNIT_MB = 1024 * UNIT_KB;


    private async Task<(IStorageProvider<object>, DatasetReader<object>, DatasetWriter<object>)> SetupDataset(TestScheduler ts)
    {
        IStorageProvider<object> inMemoryStorage = new InMemoryStorageProvider<object>(new Mock<ILogger<InMemoryStorageProvider<object>>>().Object, _datasetUpdateNotifier);
        
        var readerLogger = new Mock<ILogger<DatasetReader<object>>>();
        var writerLogger = new Mock<ILogger<DatasetWriter<object>>>();
        var storageCleanerLogger = new Mock<ILogger<StorageCleaningService<object>>>();
        var snapshotCreationPlugInLogger = new Mock<ILogger<SnapshotCreationPlugin<object>>>();
        var snapshotCreationServiceLogger = new Mock<ILogger<DatasetUpdateConsumingService<object, SnapshotCreationPlugin<object>>>>();

        var datasetReader = new DatasetReader<object>(_datasetUpdateNotifier, _dtoConfigOptions, readerLogger.Object, inMemoryStorage, false);
        var datasetWriter = new DatasetWriter<object>(_datasetUpdateNotifier, _dtoConfigOptions, writerLogger.Object, inMemoryStorage, false);
        
        var storageCleaningTask = new StorageCleaningService<object>(_datasetUpdateNotifier, inMemoryStorage, ts, _storageCleanupConfig, storageCleanerLogger.Object);
        await storageCleaningTask.StartAsync(CancellationToken.None);

        var snapshotCreationPlugin = new SnapshotCreationPlugin<object>(snapshotCreationPlugInLogger.Object, inMemoryStorage, _snapshotsOptions, ts);
        var snapshotCreationTask = new DatasetUpdateConsumingService<object, SnapshotCreationPlugin<object>> (snapshotCreationServiceLogger.Object, _datasetUpdateNotifier, snapshotCreationPlugin);
        await snapshotCreationTask.StartAsync(CancellationToken.None);

        await datasetReader.StartAsync(CancellationToken.None);
        await datasetWriter.StartAsync(CancellationToken.None);

        return (inMemoryStorage, datasetReader, datasetWriter);
    }

    private static byte[] GenerateRandomData(ulong length = UNIT_MB, int seed = 0)
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
        _storageCleanupConfig = Options.Create(cleanupConfigMock.Object);
        var scheduler = new TestScheduler();
        var (storageProvider, _, datasetWriter) = await SetupDataset(scheduler);

        
        scheduler.AdvanceTo(DateTime.UtcNow.Ticks);

        scheduler.AdvanceBy(TimeSpan.FromSeconds(5).Ticks);

        await Create6Deltas(datasetWriter);
        await Task.Delay(TimeSpan.FromSeconds(3));

        var (oldestDelta, newestDelta, snapshots) = await storageProvider.ListVersions();
        var firstSnapshot = snapshots[0].version;

        await Create6Deltas(datasetWriter);
        await Task.Delay(TimeSpan.FromSeconds(3));

        var secondSnapshot = snapshots[1].version;

        await storageProvider.StoreProcessedVersion("unittest", 10);
        await Task.Delay(TimeSpan.FromSeconds(3));

        //act
        await Create6Deltas(datasetWriter);
        await storageProvider.StoreProcessedVersion("unittest", 15);
        await Task.Delay(TimeSpan.FromSeconds(3));


        //assert
        (oldestDelta, _, snapshots) = await storageProvider.ListVersions();
        const int expectedRemainingSnapshots = 2;

        snapshots.Count.Should().Be(expectedRemainingSnapshots);
        snapshots[0].version.Should().NotBe(firstSnapshot);
        snapshots[1].version.Should().NotBe(secondSnapshot);
        oldestDelta.Should().BeCloseTo(snapshots[0].version, 5, "we are not sure because of async when the snapshot will be exactly created");
    }


    [Fact]
    public async Task Should_Delete_Old_Snapshots_After_Some_Time()
    {
        var cleanupConfigMock = new Mock<CleanupConfig>();
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
        await Task.Delay(TimeSpan.FromSeconds(1));

        //create 3 snapshots as a setup with 18 deltas
        await Create6Deltas(datasetWriter);
        await Task.Delay(TimeSpan.FromSeconds(2));

        await Create6Deltas(datasetWriter);
        await Task.Delay(TimeSpan.FromSeconds(2));

        await Create6Deltas(datasetWriter);
        await storageProvider.StoreProcessedVersion("unittest", 25);
        await Task.Delay(TimeSpan.FromSeconds(2));

        //assert nothing is deleted even tho the threshold for snapshots is met at minimum 2 snapshots
        var (oldestDelta, newestDelta, snapshots) = await storageProvider.ListVersions();

        snapshots.Count.Should().Be(fourSnapshots);
        oldestDelta.Should().Be(snapshots[0].version);


        // at 3 minutes we still have not deleted anything, however, we added one more snapshot (i.e 6 deltas)
        scheduler.AdvanceBy(TimeSpan.FromMinutes(4).Ticks);
        await Task.Delay(TimeSpan.FromSeconds(1));

        (oldestDelta, _, snapshots) = await storageProvider.ListVersions();
        int fiveSnapshots = 5;
        snapshots.Count.Should().Be(fiveSnapshots);
        oldestDelta.Should().Be(snapshots[0].version);

        //act
        scheduler.AdvanceBy(TimeSpan.FromMinutes(6).Ticks);
        await Task.Delay(TimeSpan.FromSeconds(1));


        //assert
        const int expectedRemainingSnapshots = 2;
        (oldestDelta, newestDelta, snapshots) = await storageProvider.ListVersions();
        snapshots.Count.Should().Be(expectedRemainingSnapshots);
        oldestDelta.Should().BeCloseTo(snapshots[0].version, 5, "we are not sure because of async when the snapshot will be exactly created");
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
        await Task.Delay(TimeSpan.FromSeconds(1));

        await Create6Deltas(datasetWriter);
        await Task.Delay(TimeSpan.FromSeconds(1));

        await Create6Deltas(datasetWriter);
        await Task.Delay(TimeSpan.FromSeconds(1));
        await Create6Deltas(datasetWriter);
        await Task.Delay(TimeSpan.FromSeconds(1));

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
        await Task.Delay(TimeSpan.FromSeconds(1));
        await Create6Deltas(datasetWriter);
        await Task.Delay(TimeSpan.FromSeconds(1));

        //act
        await Create6Deltas(datasetWriter);
        await Task.Delay(TimeSpan.FromSeconds(1));
        await Create6Deltas(datasetWriter);
        await Task.Delay(TimeSpan.FromSeconds(1));
        await Create6Deltas(datasetWriter);
        await Task.Delay(TimeSpan.FromSeconds(1));

        //assert
        var (oldestDelta, newestDelta, snapshots) = await storageProvider.ListVersions();
        const int expectedRemainingSnapshots = 6;

        snapshots.Count.Should().Be(expectedRemainingSnapshots);
        _storageCleanupConfig.Value.MinimumSnapshotsToKeep.Should().BeLessThan(expectedRemainingSnapshots);
        oldestDelta.Should().Be(1);
        newestDelta.Should().Be(30);
    }

    private async Task Create6Deltas(DatasetWriter<object>? s3DatasetWriter)
    {
        for (var i = 0; i < 6; i++)
        {
            if (i % 2 == 0)
            {
                var randomBytes = GenerateRandomData(UNIT_MB, i);

                await s3DatasetWriter?.Put("Same_DTO", randomBytes)!;
            }
            else
            {

                var randomBytes2 = GenerateRandomData(UNIT_MB, 1);
                await s3DatasetWriter?.Put("Same_DTO", randomBytes2)!;
            }
        }
    }
}