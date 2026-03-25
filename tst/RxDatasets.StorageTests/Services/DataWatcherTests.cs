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

public class DataWatcherTests
{
    [Fact]
    public async Task WhenThereIsASubscriber_ShouldCallOnVersionStart()
    {
        // Arrange
        var (datasetWriter, datasetObserver, datasetWatcher, _) = await SetupWatcher();
        const string testAdd = "Test";
        await datasetWriter.Put(testAdd, [1]);

        CancellationTokenSource cts = new();
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
        var (datasetWriter, datasetObserver, datasetWatcher, _) = await SetupWatcher();

        const string testAdd = "Test";
        await datasetWriter.Put(testAdd, [1]);
        CancellationTokenSource cts = new();
        datasetObserver.Setup((observer => observer.OnDatasetVersionEnd(1))).Callback(() => cts.Cancel());


        // Act
        var task = datasetWatcher.SubscribeAllData(datasetObserver.Object, cts.Token);
        await task;


        // Assert
        datasetObserver.Verify(o => o.OnDatasetVersionStart(1), Times.Once);
        datasetObserver.Verify(o => o.OnDatasetVersionEnd(1), Times.Once);

        await datasetWriter.Put(testAdd, []);
        await Task.Delay(10);

        datasetObserver.Verify(o => o.OnDatasetVersionStart(2), Times.Never);
        datasetObserver.Verify(o => o.OnDatasetVersionEnd(2), Times.Never);
    }


    [Fact]
    public async Task WhenThereIsASubscriber_ShouldCallSubMethods()
    {
        // Arrange
        var (datasetWriter, datasetObserver, datasetWatcher, storageProvider) = await SetupWatcher();

        const string testAdd = "Test";
        await datasetWriter.Put(testAdd, [1]);           // v1

        const string testAddUpdateDeleted = "Test2";
        await datasetWriter.Put(testAddUpdateDeleted, [1]); // v2
        await datasetWriter.Put(testAddUpdateDeleted, [2]); // v3
        await datasetWriter.Delete(testAddUpdateDeleted);    // v4

        var testAddUpdate3Times = "Test3";
        await datasetWriter.Put(testAddUpdate3Times, [1]);   // v5 — triggers snapshot

        // Wait for the snapshot at delta 5 to complete before continuing writes
        // (SnapshotInterval=5). This ensures the watcher sees a deterministic snapshot version.
        for (var i = 0; i < 50; i++)
        {
            var (_, _, s) = await storageProvider.ListVersions();
            if (s.Count >= 1) break;
            await Task.Delay(100);
        }

        await datasetWriter.Put(testAddUpdate3Times, [2]);   // v6
        await datasetWriter.Put(testAddUpdate3Times, [3]);   // v7
        await datasetWriter.Put(testAddUpdate3Times, [4]);   // v8
        CancellationTokenSource cts = new();
        datasetObserver.Setup((observer => observer.OnDatasetVersionEnd(8))).Callback(() => cts.Cancel());

        // act
        var task = datasetWatcher.SubscribeAllData(datasetObserver.Object, cts.Token);
        await task;

        // assert — snapshot at v5 means deltas 6..8 are replayed
        datasetObserver.Verify(o => o.OnDatasetVersionStart(6), Times.Once);
        datasetObserver.Verify(o => o.OnDatasetVersionStart(8), Times.Once);

        datasetObserver.Verify(o => o.OnCreate(testAdd, It.IsAny<ReadOnlyMemory<byte>>()), Times.Once);
        datasetObserver.Verify(o => o.OnDelete(testAddUpdateDeleted), Times.Once);
        datasetObserver.Verify(o => o.OnChange(testAddUpdate3Times, It.IsAny<ReadOnlyMemory<byte>>()), Times.Exactly(3));

        datasetObserver.Verify(o => o.OnDatasetVersionEnd(6), Times.Once);
        datasetObserver.Verify(o => o.OnDatasetVersionEnd(8), Times.Once);
    }

    private static async Task<(DatasetWriter<object> datasetWriter, Mock<IDatasetObserver<object>> datasetObserver, DatasetWatcher<object> datasetWatcher, IStorageProvider<object> storageProvider)> SetupWatcher()
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

        var datasetWriter = new DatasetWriter<object>(notificationsHub, Mock.Of<ILogger<DatasetWriter<object>>>(), storageProvider, Options.Create(new WriterConfig()), false);
        var datasetObserver = new Mock<IDatasetObserver<object>>();

        var datasetWatcher = new DatasetWatcher<object>(logger, readOnlyStorage, Options.Create(new DatasetWatcherConfig
        {
            CheckingIntervalInSeconds = 0
        }));
        return (datasetWriter, datasetObserver, datasetWatcher, storageProvider);
    }
}
