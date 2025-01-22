using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Reactive.Testing;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProvider.LocalDisk;
using RxDatasets.StorageProviders;
using RxDatasets.StorageTests.Common;

namespace RxDatasets.StorageTests.SpecTests.WithCustomTestContainersImplementation;

public class LocalDiskDatasetTests : StorageTestSpecWithTwoStepInit, IDisposable
{
    private readonly IOptions<LocalDiskStorageConfig> _config;
    private readonly LocalDiskStorage<object> _localStorage;

    public LocalDiskDatasetTests()
    {
        _config = Options.Create(new LocalDiskStorageConfig());
        _config.Value.RootFolder = Path.Combine(DatasetHelper.GetTempFileDirectory(), "UnitTestsData_localdisk_Tests/");

        if (Directory.Exists(_config.Value.RootFolder))
        {
            Directory.Delete(_config.Value.RootFolder, recursive: true);
        }

        _localStorage = new LocalDiskStorage<object>(new Mock<ILogger<LocalDiskStorage<object>>>().Object, _config);

    }

    protected override async Task<(DatasetReader<object>, DatasetWriter<object>)> SetupStorageAsync(
        CancellationToken ct,
        string memberName = "")
    {
        INotificationsHub<object> notificationsHub = new NotificationsHubSync<object>();
        var rootFolderName = new string(memberName.ToLower().Replace("_", "").TakeLast(63).ToArray());

        var storageConfig = Options.Create(new StorageConfig {CustomRootFolder = rootFolderName+"/"});
        IStorageReader<object> readOnlyStorage = new ObjectStorageReader<object>(Mock.Of<ILogger<ObjectStorageReader<object>>>(), storageConfig, _localStorage);

        var storageProvider = new ObjectStorageProvider<object>(new Mock<ILogger<ObjectStorageProvider<object>>>().Object, storageConfig, _localStorage, notificationsHub,  readOnlyStorage,false);
        storageProvider.StartAsync(CancellationToken.None).Wait();
        
        var writerLogger = new Mock<ILogger<DatasetWriter<object>>>();

        var datasetReader = new DatasetReader<object>(Mock.Of<ILogger<DatasetReader<object>>>(), storageProvider, notificationsHub);
        await datasetReader.StartAsync(CancellationToken.None);
        var datasetWriter = new DatasetWriter<object>(notificationsHub, writerLogger.Object, storageProvider, Options.Create(new WriterConfig { }), false);
        var snapshotCreationTaskLogger = new Mock<ILogger<SnapshotCreationPlugin<object>>>();
        var datasetUpdateConsumingServiceLogger = new Mock<ILogger<DatasetUpdateConsumingService<object, SnapshotCreationPlugin<object>>>>();
        var snapshotCreationPlugin = new SnapshotCreationPlugin<object>(snapshotCreationTaskLogger.Object, storageProvider, SnapshotsOptions, new TestScheduler());
        var datasetUpdateConsumingService = new DatasetUpdateConsumingService<object, SnapshotCreationPlugin<object>> (datasetUpdateConsumingServiceLogger.Object, notificationsHub, snapshotCreationPlugin);
        await datasetUpdateConsumingService.StartAsync(CancellationToken.None);
        return (datasetReader, datasetWriter);
    }
    
    public void Dispose()
    {
        if(Directory.Exists(_config.Value.RootFolder))
            Directory.Delete(_config.Value.RootFolder, true);
    }
}