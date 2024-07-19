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

public class SnapshotCreationServiceTests
{
    private readonly IOptions<StorageConfig> _dtoConfigOptions = Options.Create(new StorageConfig());
    private readonly IOptions<SnapshotCreationConfig> _snapshotsOptions = Options.Create(new SnapshotCreationConfig
    {
        SnapshotInterval = 5
    });
    private IOptions<SnapshotCreationConfig> _snapshotCreationConfig = Options.Create(new SnapshotCreationConfig());
    private readonly DatasetUpdateNotifierSync<object> _datasetUpdateNotifier = new();
    private const ulong UNIT_KB = 1024;
    private const ulong UNIT_MB = 1024 * UNIT_KB;


    private async Task<(IStorageProvider<object>, DatasetReader<object>, DatasetWriter<object>, SnapshotCreationService<object>)> SetupDataset(TestScheduler ts)
    {
        IStorageProvider<object> inMemoryStorage = new InMemoryStorageProvider<object>(new Mock<ILogger<InMemoryStorageProvider<object>>>().Object, _datasetUpdateNotifier);
        
        var readerLogger = new Mock<ILogger<DatasetReader<object>>>();
        var writerLogger = new Mock<ILogger<DatasetWriter<object>>>();
        var snapshotCreationTaskLogger = new Mock<ILogger<SnapshotCreationService<object>>>();
        



        var datasetReader = new DatasetReader<object>(_datasetUpdateNotifier, _dtoConfigOptions, readerLogger.Object, inMemoryStorage, false);
        var datasetWriter = new DatasetWriter<object>(_datasetUpdateNotifier, _dtoConfigOptions, writerLogger.Object, inMemoryStorage, false);

        var snapshotCreationService = new SnapshotCreationService<object>(snapshotCreationTaskLogger.Object, inMemoryStorage, _datasetUpdateNotifier, _snapshotCreationConfig, ts);
        await snapshotCreationService.StartAsync(CancellationToken.None);

        await datasetReader.StartAsync(CancellationToken.None);
        await datasetWriter.StartAsync(CancellationToken.None);

        return (inMemoryStorage, datasetReader, datasetWriter, snapshotCreationService);
    }

    private static byte[] GenerateRandomData(ulong length = UNIT_MB, int seed = 0)
    {
        var randomData = new byte[length];
        var random = new Random(seed);

        random.NextBytes(randomData);
        return randomData;
    }

    [Fact]
    public async Task Should_Create_A_Snapshot_After_One_Hour()
    {
        
        _snapshotCreationConfig = Options.Create(new SnapshotCreationConfig()
        {
            PeriodInHours = 1,
            SnapshotInterval = 10
        });
        var utcNowTicks = DateTime.UtcNow.Ticks;

        var scheduler = new TestScheduler();
        scheduler.AdvanceTo(utcNowTicks);

        var (storageProvider, _, datasetWriter, _) = await SetupDataset(scheduler);

        const ulong howManyDeltasToCreate = 4ul;
        await CreateNumberDeltas(howManyDeltasToCreate, datasetWriter);   

        var (_, newestDelta, snapshots) = await storageProvider.ListVersions();
        newestDelta.Should().Be(howManyDeltasToCreate);
        snapshots.Count.Should().Be(1);
        scheduler.AdvanceBy(TimeSpan.FromHours(2).Ticks); //this should trigger a call to create a snapshot for up to delta number 4, currently nothing happens

        (var oldestDelta, newestDelta, snapshots) = await storageProvider.ListVersions();
        snapshots.Count.Should().Be(2);
        snapshots.First().version.Should().Be(oldestDelta);
        snapshots.Last().version.Should().Be(newestDelta);
        await storageProvider.StoreProcessedVersion("unittest", howManyDeltasToCreate);
    }


    [Fact]
    public void Should_ReturnTrue_When_DeltaIsGreaterThanOrEqualInterval()
    {
        // Arrange
        ulong lastDelta = 10;
        ulong lastSnapshotVersion = 5;
        ulong interval = 5;

        // Act
        bool result = SnapshotCreationService<object>.IsDeltaGapConditionMet(lastDelta, lastSnapshotVersion, interval);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void Should_ReturnFalse_When_DeltaIsLessThanInterval()
    {
        // Arrange
        ulong lastDelta = 10;
        ulong lastSnapshotVersion = 5;
        ulong interval = 6;

        // Act
        bool result = SnapshotCreationService<object>.IsDeltaGapConditionMet(lastDelta, lastSnapshotVersion, interval);

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public void Should_ReturnTrue_When_DeltaIsEqualToInterval()
    {
        // Arrange
        ulong lastDelta = 10;
        ulong lastSnapshotVersion = 0;
        ulong interval = 10;

        // Act
        bool result = SnapshotCreationService<object>.IsDeltaGapConditionMet(lastDelta, lastSnapshotVersion, interval);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void Should_ReturnDefault_When_SnapshotTimeIsNull()
    {
        // Arrange
        var scheduler = new TestScheduler();
        scheduler.AdvanceTo(DateTime.UtcNow.Ticks);
        TimeSpan maxInterval = TimeSpan.FromMinutes(10);

        // Act
        TimeSpan result = SnapshotCreationService<object>.CalculateTimeUntilNextSnapshot(scheduler, null, maxInterval);

        // Assert
        result.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void Should_ReturnFullInterval_When_SnapshotTimeJustOccurred()
    {
        // Arrange
        var scheduler = new TestScheduler();
        scheduler.AdvanceTo(DateTime.UtcNow.Ticks);
        DateTimeOffset snapshotTime = scheduler.Now;
        TimeSpan maxInterval = TimeSpan.FromMinutes(10);

        // Act
        TimeSpan result = SnapshotCreationService<object>.CalculateTimeUntilNextSnapshot(scheduler, snapshotTime, maxInterval);

        // Assert
        result.Should().Be(maxInterval);
    }

    [Fact]
    public void Should_ReturnCorrectTimeUntilNextSnapshot_When_SomeTimeHasPassed()
    {
        // Arrange
        var scheduler = new TestScheduler();
        scheduler.AdvanceTo(DateTime.UtcNow.Ticks);
        DateTimeOffset snapshotTime = scheduler.Now.AddMinutes(-5);
        TimeSpan maxInterval = TimeSpan.FromMinutes(10);

        // Act
        TimeSpan result = SnapshotCreationService<object>.CalculateTimeUntilNextSnapshot(scheduler, snapshotTime, maxInterval);

        // Assert
        result.Should().Be(TimeSpan.FromMinutes(5));
    }

    [Fact]
    public void Should_ReturnZero_When_MaxIntervalHasPassed()
    {
        // Arrange
        var scheduler = new TestScheduler();
        scheduler.AdvanceTo(DateTime.UtcNow.Ticks);
        DateTimeOffset snapshotTime = scheduler.Now.AddMinutes(-10);
        TimeSpan maxInterval = TimeSpan.FromMinutes(10);

        // Act
        TimeSpan result = SnapshotCreationService<object>.CalculateTimeUntilNextSnapshot(scheduler, snapshotTime, maxInterval);

        // Assert
        result.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void Should_ReturnNegativeTimeSpan_When_MoreThanMaxIntervalHasPassed()
    {
        // Arrange
        var scheduler = new TestScheduler();
        scheduler.AdvanceTo(DateTime.UtcNow.Ticks);
        DateTimeOffset snapshotTime = scheduler.Now.AddMinutes(-15);
        TimeSpan maxInterval = TimeSpan.FromMinutes(10);

        // Act
        TimeSpan result = SnapshotCreationService<object>.CalculateTimeUntilNextSnapshot(scheduler, snapshotTime, maxInterval);

        // Assert
        result.Should().Be(TimeSpan.FromMinutes(-5));
    }


private static async Task CreateNumberDeltas(ulong howManyDeltasToCreate, DatasetWriter<object>? s3DatasetWriter)
    {
        for (var i = 0ul; i < howManyDeltasToCreate; i++)
        {
            if (i % 2 == 0)
            {
                var randomBytes = GenerateRandomData(UNIT_MB, (int)i);

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

