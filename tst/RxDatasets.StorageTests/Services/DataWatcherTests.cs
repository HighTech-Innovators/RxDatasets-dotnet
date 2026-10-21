using FluentAssertions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.DatasetWatcher;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProviders;
using RxDatasets.StorageTests.SpecTests.UsingInMemoryTests;
using RxDatasets.TestImplementations.InMemoryProvider;

namespace RxDatasets.StorageTests.Services;

public class DataWatcherTests : IAsyncLifetime
{
    // The watcher polls until the observer cancels it; if the version it waits for never comes, this fails the test
    // instead of polling forever.
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(30);

    private IStorageProvider<object> _storageProvider = null!;
    private IHostedService? _snapshotCreationService;

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        if (_snapshotCreationService != null)
            await _snapshotCreationService.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task WhenThereIsASubscriber_ShouldCallOnVersionStart()
    {
        // Arrange
        var (datasetWriter, datasetObserver, datasetWatcher) = await SetupWatcher();
        const string testAdd = "Test";
        await datasetWriter.Put(testAdd, [1], null, null);
        await AssertSnapshot(1);

        using CancellationTokenSource cts = new(TestTimeout);
        datasetObserver.Setup((observer => observer.OnDatasetVersionEnd(1))).Callback(() => cts.Cancel());

        // Act
        var task = datasetWatcher.SubscribeAllData(datasetObserver.Object, cts.Token);
        await task;


        // Assert
        datasetObserver.Verify(o => o.OnDatasetVersionStart(1), Times.Once);
        datasetObserver.Verify(o => o.OnDatasetVersionEnd(1), Times.Once);
    }



    [Fact]
    public async Task WhenThereIsADisposedSubscriber_ShouldNotCallOnVersionStart()
    {
        // Arrange
        var (datasetWriter, datasetObserver, datasetWatcher) = await SetupWatcher();

        const string testAdd = "Test";
        await datasetWriter.Put(testAdd, [1], null, null);
        await AssertSnapshot(1);
        using CancellationTokenSource cts = new(TestTimeout);
        datasetObserver.Setup((observer => observer.OnDatasetVersionEnd(1))).Callback(() => cts.Cancel());


        // Act
        var task = datasetWatcher.SubscribeAllData(datasetObserver.Object, cts.Token);
        await task;


        // Assert
        datasetObserver.Verify(o => o.OnDatasetVersionStart(1), Times.Once);
        datasetObserver.Verify(o => o.OnDatasetVersionEnd(1), Times.Once);

        await datasetWriter.Put(testAdd, [], null, null);
        await Task.Delay(10);

        datasetObserver.Verify(o => o.OnDatasetVersionStart(2), Times.Never);
        datasetObserver.Verify(o => o.OnDatasetVersionEnd(2), Times.Never);
    }


    [Fact]
    public async Task WhenThereIsASubscriber_ShouldCallSubMethods()
    {
        // Arrange
        var (datasetWriter, datasetObserver, datasetWatcher) = await SetupWatcher();

        // The snapshot service snapshots right after the first delta, and then every 5 deltas: at versions 1 and 6.
        // The watcher starts from the snapshot at 6 and reports the deltas after it, up to version 7, which ends it.
        const string testAdd = "Test";
        await datasetWriter.Put(testAdd, [1], null, null);
        await AssertSnapshot(1);

        const string testAddUpdateDeleted = "Test2";
        await datasetWriter.Put(testAddUpdateDeleted, [1], null, null);
        await datasetWriter.Put(testAddUpdateDeleted, [2], null, null);
        await datasetWriter.Delete(testAddUpdateDeleted, null, null);

        var testAddUpdate3Times = "Test3";
        await datasetWriter.Put(testAddUpdate3Times, [1], null, null);
        await datasetWriter.Put(testAddUpdate3Times, [2], null, null);
        await AssertSnapshot(6);
        await datasetWriter.Put(testAddUpdate3Times, [3], null, null);
        await datasetWriter.Put(testAddUpdate3Times, [4], null, null);
        using CancellationTokenSource cts = new(TestTimeout);
        datasetObserver.Setup((observer => observer.OnDatasetVersionEnd(7))).Callback(() => cts.Cancel());

        // act
        var task = datasetWatcher.SubscribeAllData(datasetObserver.Object, cts.Token);
        await task;

        // assert
        datasetObserver.Verify(o => o.OnDatasetVersionStart(6), Times.Once);
        datasetObserver.Verify(o => o.OnDatasetVersionStart(7), Times.Once);

        datasetObserver.Verify(o => o.OnCreate(testAdd, It.IsAny<ReadOnlyMemory<byte>>()), Times.Once);
        datasetObserver.Verify(o => o.OnDelete(testAddUpdateDeleted), Times.Once);
        datasetObserver.Verify(o => o.OnChange(testAddUpdate3Times, It.IsAny<ReadOnlyMemory<byte>>()), Times.Exactly(3));

        datasetObserver.Verify(o => o.OnDatasetVersionEnd(6), Times.Once);
        datasetObserver.Verify(o => o.OnDatasetVersionEnd(7), Times.Once);
    }

    private async Task AssertSnapshot(ulong version)
    {
        // With the synchronous notifications hub a write returns only after the snapshot service has handled it
        var (_, _, snapshots) = await _storageProvider.ListVersions();
        snapshots.Select(s => s.Version).Should().Contain(version,
            $"snapshots [{string.Join(", ", snapshots.Select(s => s.Version))}]");
    }

    private async Task<(DatasetWriter<object> datasetWriter, Mock<IDatasetObserver<object>> datasetObserver, DatasetWatcher<object> datasetWatcher)> SetupWatcher()
    {
        var logger = Mock.Of<ILogger<DatasetWatcher<object>>>();
        var lowLevelStorage = new LowLevelInMemoryStorage<object>(Mock.Of<ILogger<LowLevelInMemoryStorage<object>>>());
        var notificationsHub = new NotificationsHubSync<object>();
        var storageConfig = Options.Create(new StorageConfig());

        IStorageReader<object> readOnlyStorage = new ObjectStorageReader<object>(Mock.Of<ILogger<ObjectStorageReader<object>>>(), storageConfig, lowLevelStorage);
        IStorageProvider<object> storageProvider = new ObjectStorageProvider<object>(Mock.Of<ILogger<ObjectStorageProvider<object>>>(), storageConfig, lowLevelStorage, notificationsHub, readOnlyStorage, false);
        await ((ObjectStorageProvider<object>)storageProvider).StartAsync(CancellationToken.None);
        var scs = UsingLowLevelInMemoryStorage.CreateSnapshotCreationService(storageProvider, notificationsHub);
        await scs.StartAsync(CancellationToken.None);
        _storageProvider = storageProvider;
        _snapshotCreationService = scs;

        var datasetWriter = new DatasetWriter<object>(notificationsHub, Mock.Of<ILogger<DatasetWriter<object>>>(), storageProvider, Options.Create(new WriterConfig()), false);
        var datasetObserver = new Mock<IDatasetObserver<object>>();

        var datasetWatcher = new DatasetWatcher<object>(logger, readOnlyStorage, Options.Create(new DatasetWatcherConfig
        {
            CheckingIntervalInSeconds = 0
        }));
        return (datasetWriter, datasetObserver, datasetWatcher);
    }
}