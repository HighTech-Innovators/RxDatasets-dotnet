using System.Reactive.Concurrency;
using System.Runtime.CompilerServices;
using FluentAssertions;
using HTI.RxDatasets.Abstractions;
using HTI.RxDatasets.BackgroundServices;
using HTI.RxDatasets.DatasetUpdateNotifications;
using HTI.RxDatasets.StorageProviders;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Reactive.Testing;
using Moq;

namespace HTI.RxDatasets.Tests.Services;

public class StorageCleanTaskTests 
{
    private readonly IOptions<DtoConfig> _dtoConfigOptions;
    private readonly IOptions<SnapshotCreationConfig> _snapshotsOptions;
    private IOptions<CleanupConfig> _storageCleanupConfig;
    private readonly DatasetUpdateNotifierSync<object> _datasetUpdateNotifier;
    private const ulong UNIT_KB = 1024;
    private const ulong UNIT_MB = 1024 * UNIT_KB;


    public StorageCleanTaskTests()
    {
        _storageCleanupConfig = Options.Create(new CleanupConfig());
        _datasetUpdateNotifier = new DatasetUpdateNotifierSync<object>();
        _snapshotsOptions = Options.Create<SnapshotCreationConfig>(new()
        {
            SnapshotInterval = 5
        });
        _dtoConfigOptions = Options.Create<DtoConfig>(new());
    }

    private async Task<(IStorageProvider<object>, DatasetReader<object>, DatasetWriter<object>)> SetupDataset(TestScheduler ts,
        [CallerMemberName] string memberName = "")
    {
        _dtoConfigOptions.Value.BucketName = new string(memberName.ToLower().Replace("_", "").TakeLast(63).ToArray());

        IStorageProvider<object> inMemoryStorage = new InMemoryStorageProvider<object>(new Mock<ILogger<InMemoryStorageProvider<object>>>().Object, _datasetUpdateNotifier);
        
        var readerLogger = new Mock<ILogger<DatasetReader<object>>>();
        var writerLogger = new Mock<ILogger<DatasetWriter<object>>>();
        var storageCleanerLogger = new Mock<ILogger<StorageCleaningService<object>>>();
        var SnapshotCreationTaskLogger = new Mock<ILogger<SnapshotCreationService<object>>>();
        
        var configMonitor = new Mock<IOptionsMonitor<CleanupConfig>>();
        var configMonitorSnapshotCreationMock = new Mock<IOptionsMonitor<SnapshotCreationConfig>>();
        configMonitor.SetupGet((c) => c.CurrentValue).Returns(_storageCleanupConfig.Value);
        
        var snapshotCreationConfig = _snapshotsOptions;
        configMonitorSnapshotCreationMock.SetupGet((c) => c.CurrentValue).Returns(snapshotCreationConfig.Value);


        var datasetReader = new DatasetReader<object>(_datasetUpdateNotifier, _dtoConfigOptions, readerLogger.Object, inMemoryStorage, false);
        var datasetWriter = new DatasetWriter<object>(_datasetUpdateNotifier, _dtoConfigOptions, writerLogger.Object, inMemoryStorage, false);
        
        var storageCleaningTask = new StorageCleaningService<object>(_datasetUpdateNotifier, inMemoryStorage, ts, configMonitor.Object, storageCleanerLogger.Object);
        await storageCleaningTask.StartAsync(CancellationToken.None);

        var snapshotCreationTask = new SnapshotCreationService<object>(SnapshotCreationTaskLogger.Object, inMemoryStorage, _datasetUpdateNotifier, configMonitorSnapshotCreationMock.Object, _dtoConfigOptions);
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

        await CreateASnapshot(datasetWriter);
        await CreateASnapshot(datasetWriter);
        await storageProvider.StoreProcessedVersion("unittest", 10);

        //act
        await CreateASnapshot(datasetWriter);
        await storageProvider.StoreProcessedVersion("unittest", 15);


        //assert
        var (oldestDelta, newestDelta, snapshots) = await storageProvider.ListVersions();
        const int expectedRemainingSnapshots = 2;

        snapshots.Count.Should().Be(expectedRemainingSnapshots);
        oldestDelta.Should().Be(11); 
        newestDelta.Should().Be(15);
    }


    [Fact]
    public async Task Should_Delete_Old_Snapshots_After_Some_Time()
    {
        var cleanupConfigMock = new Mock<CleanupConfig>();
        const int minimumTimeToLive = 5;
        var scheduler = new TestScheduler();
        cleanupConfigMock.Setup(c => c.GetMinimumAgeToLiveInMinutes()).Returns(TimeSpan.FromMinutes(minimumTimeToLive));
        _storageCleanupConfig = Options.Create(cleanupConfigMock.Object);
        
        var (storageProvider, _, DatasetWriter) = await SetupDataset(scheduler);
        await storageProvider.StoreProcessedVersion("unittest", 0);

        scheduler.AdvanceTo(DateTime.UtcNow.Ticks);
        const int threeSnapshots = 3;
        var deltasInOneSnapshot = (int)_snapshotsOptions.Value.SnapshotInterval;
        var howManyDeltasToCreate = deltasInOneSnapshot * threeSnapshots; //3 to make sure that there are enough deltas, but also, avoid triggering the cleanup, more than once for testing purposes
        scheduler.Schedule(TimeSpan.FromMinutes(3), () => CreateASnapshot(DatasetWriter).Wait());
        scheduler.Schedule(TimeSpan.FromMinutes(6), () => CreateASnapshot(DatasetWriter).Wait());
        await storageProvider.StoreProcessedVersion("unittest", 10);

        //create 3 snapshots as a setup with 15 deltas
        await CreateASnapshot(DatasetWriter);
        await CreateASnapshot(DatasetWriter);
        await CreateASnapshot(DatasetWriter);
        await storageProvider.StoreProcessedVersion("unittest", 25);

        //assert nothing is deleted even tho the threshold for snapshots is met at minimum 2 snapshots
        var (oldestDelta, newestDelta, snapshots) = await storageProvider.ListVersions();

        snapshots.Count.Should().Be(threeSnapshots);
        oldestDelta.Should().Be(1);
        newestDelta.Should().Be(15);


        // at 3 minutes we still have not deleted anything, however, we added one more snapshot (i.e 5 deltas)
        scheduler.AdvanceBy(TimeSpan.FromMinutes(4).Ticks);
        (oldestDelta, newestDelta, snapshots) = await storageProvider.ListVersions();
        const int fourSnapshots = 4;
        snapshots.Count.Should().Be(fourSnapshots);
        oldestDelta.Should().Be(1);
        newestDelta.Should().Be(20);

        //act
        scheduler.AdvanceBy(TimeSpan.FromMinutes(6).Ticks);


        //assert
        const int expectedRemainingSnapshots = 2;
        (oldestDelta, newestDelta, snapshots) = await storageProvider.ListVersions();
        snapshots.Count.Should().Be(expectedRemainingSnapshots);
        oldestDelta.Should().Be(21);
        newestDelta.Should().Be(25);
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
        await CreateASnapshot(datasetWriter);
        await CreateASnapshot(datasetWriter);
        await CreateASnapshot(datasetWriter);
        await CreateASnapshot(datasetWriter);

        //assert
        var (oldestDelta, newestDelta, snapshots) = await storageProvider.ListVersions();
        const int expectedRemainingSnapshots = 4;

        snapshots.Count.Should().Be(expectedRemainingSnapshots);
        oldestDelta.Should().Be(1);
        newestDelta.Should().Be(20);
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

        await CreateASnapshot(datasetWriter);
        await CreateASnapshot(datasetWriter);

        //act
        await CreateASnapshot(datasetWriter);
        await CreateASnapshot(datasetWriter);
        await CreateASnapshot(datasetWriter);

        //assert
        var (oldestDelta, newestDelta, snapshots) = await storageProvider.ListVersions();
        const int expectedRemainingSnapshots = 5;

        snapshots.Count.Should().Be(expectedRemainingSnapshots);
        _storageCleanupConfig.Value.MinimumSnapshotsToKeep.Should().BeLessThan(expectedRemainingSnapshots);
        oldestDelta.Should().Be(1);
        newestDelta.Should().Be(25);
    }

    private async Task CreateASnapshot(DatasetWriter<object>? s3DatasetWriter)
    {
        for (var i = 0; i < 5; i++)
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