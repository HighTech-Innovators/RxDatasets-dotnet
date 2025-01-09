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
        var (datasetWriter, datasetObserver, datasetWatcher) = await SetupWatcher();
        const string testAdd = "Test";
        await datasetWriter.Put(testAdd, [1]);

        CancellationTokenSource cts = new();
        datasetObserver.Setup((observer => observer.OnDatasetVersionEnd(1))).Callback(() => cts.Cancel());

        // Act
        var task = datasetWatcher.SubscribeAllData(datasetObserver.Object, cts.Token);
        await task;

        
        // Assert
        datasetObserver.Verify(o=>o.OnDatasetVersionStart(1), Times.Once);
        datasetObserver.Verify(o=>o.OnDatasetVersionEnd(1), Times.Once);
    }

    

    [Fact]
    public async Task WhenThereIsADisposedSubscriber_ShouldNotCallOnVersionStart()
    {
        // Arrange
        var (datasetWriter, datasetObserver, datasetWatcher) = await SetupWatcher();

        const string testAdd = "Test";
        await datasetWriter.Put(testAdd, [1]);
        CancellationTokenSource cts = new();
        datasetObserver.Setup((observer => observer.OnDatasetVersionEnd(1))).Callback(() => cts.Cancel());

        
        // Act
        var task = datasetWatcher.SubscribeAllData(datasetObserver.Object, cts.Token);
        await task;
       

        // Assert
        datasetObserver.Verify(o=>o.OnDatasetVersionStart(1), Times.Once);
        datasetObserver.Verify(o=>o.OnDatasetVersionEnd(1), Times.Once);
        
        await datasetWriter.Put(testAdd, []);
        await Task.Delay(10);

        datasetObserver.Verify(o=>o.OnDatasetVersionStart(2), Times.Never);
        datasetObserver.Verify(o=>o.OnDatasetVersionEnd(2), Times.Never);
    }
    
    
    [Fact]
    public async Task WhenThereIsASubscriber_ShouldCallSubMethods()
    {
        // Arrange
        var (datasetWriter, datasetObserver, datasetWatcher) = await SetupWatcher();

        const string testAdd = "Test";
         await datasetWriter.Put(testAdd, [1]);
         
         const string testAddUpdateDeleted = "Test2";
         await datasetWriter.Put(testAddUpdateDeleted, [1]);
         await datasetWriter.Put(testAddUpdateDeleted, [2]);
         await datasetWriter.Delete(testAddUpdateDeleted);

         var testAddUpdate3Times = "Test3";
         await datasetWriter.Put(testAddUpdate3Times, [1]);
         await datasetWriter.Put(testAddUpdate3Times, [2]);
         await datasetWriter.Put(testAddUpdate3Times, [3]);
         await datasetWriter.Put(testAddUpdate3Times, [4]);
         CancellationTokenSource cts = new();
         datasetObserver.Setup((observer => observer.OnDatasetVersionEnd(7))).Callback(() => cts.Cancel());

         // act
         var task = datasetWatcher.SubscribeAllData(datasetObserver.Object, cts.Token);
         await task;

        // assert
        datasetObserver.Verify(o=>o.OnDatasetVersionStart(6), Times.Once);
        datasetObserver.Verify(o=>o.OnDatasetVersionStart(7), Times.Once);
        
        datasetObserver.Verify(o=>o.OnCreate(testAdd, It.IsAny<ReadOnlyMemory<byte>>()), Times.Once);
        datasetObserver.Verify(o=>o.OnDelete(testAddUpdateDeleted), Times.Once);
        datasetObserver.Verify(o=>o.OnChange(testAddUpdate3Times, It.IsAny<ReadOnlyMemory<byte>>()), Times.Exactly(3));

        datasetObserver.Verify(o=>o.OnDatasetVersionEnd(6), Times.Once);
        datasetObserver.Verify(o=>o.OnDatasetVersionEnd(7), Times.Once);
    }

    private static async Task<(DatasetWriter<object> datasetWriter, Mock<IDatasetObserver<object>> datasetObserver, DatasetWatcher<object> datasetWatcher)> SetupWatcher()
    {
        var logger = Mock.Of<ILogger<DatasetWatcher<object>>>();
        var lowLevelStorage = new LowLevelInMemoryStorage<object>(Mock.Of<ILogger<LowLevelInMemoryStorage<object>>>());
        var notificationsHub = new NotificationsHubSync<object>();
        var storageConfig = Options.Create(new StorageConfig());

        IStorageReader<object> storageReader = new ObjectStorageReader<object>(Mock.Of<ILogger<ObjectStorageReader<object>>>(), storageConfig, lowLevelStorage);
        IStorageProvider<object> storageProvider = new ObjectStorageProvider<object>(Mock.Of<ILogger<ObjectStorageProvider<object>>>(), storageConfig, lowLevelStorage, notificationsHub, storageReader,false );
        await ((ObjectStorageProvider<object>)storageProvider).StartAsync(CancellationToken.None);
        await UsingLowLevelInMemoryStorage.StartSnapshotCreationService(storageProvider, notificationsHub);

        var datasetWriter = new DatasetWriter<object>(notificationsHub, Mock.Of<ILogger<DatasetWriter<object>>>(), storageProvider, Options.Create(new WriterConfig()), false);
        var datasetObserver = new Mock<IDatasetObserver<object>>();
        
        var datasetWatcher = new DatasetWatcher<object>(logger, storageReader, Options.Create(new DatasetWatcherConfig
        {
            CheckingIntervalInSeconds = 0
        }));
        return (datasetWriter, datasetObserver, datasetWatcher);
    }
}