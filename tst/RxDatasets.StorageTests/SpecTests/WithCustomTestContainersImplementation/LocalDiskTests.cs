using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProvider.LocalDisk;
using RxDatasets.StorageProviders;
using RxDatasets.StorageTests.Common;

namespace RxDatasets.StorageTests.SpecTests.WithCustomTestContainersImplementation;

public class LocalDiskDatasetTests : StorageTestSpec
{
    private readonly INotificationsHub<object> _notificationsHub = new NotificationsHubSync<object>();


    private IStorageProvider<object> _storageProvider;

    public LocalDiskDatasetTests()
    {
        IOptions<LocalDiskStorageConfig> config = Options.Create(new LocalDiskStorageConfig());
        config.Value.RootFolder = @"UnitTestsData/";

        if (Directory.Exists(config.Value.RootFolder))
        {
            Directory.Delete(config.Value.RootFolder, recursive: true);
        }

        LocalDiskStorageProvider<object> localStorageProvider = new LocalDiskStorageProvider<object>(new Mock<ILogger<LocalDiskStorageProvider<object>>>().Object, config);
        _storageProvider = new ObjectStorageProvider<object>(new Mock<ILogger<ObjectStorageProvider<object>>>().Object, Options.Create(new StorageConfig()), localStorageProvider, _notificationsHub, false);
        ((ObjectStorageProvider<object>)_storageProvider).StartAsync(CancellationToken.None).Wait();

    }

    protected override async Task<(DatasetReader<object>, DatasetWriter<object>)> SetupStorageAsync(string memberName = "")
    {
        var readerLogger = new Mock<ILogger<DatasetReader<object>>>();
        var writerLogger = new Mock<ILogger<DatasetWriter<object>>>();

        var datasetReader = new DatasetReader<object>(_notificationsHub, readerLogger.Object, _storageProvider, false);
        var datasetWriter = new DatasetWriter<object>(_notificationsHub, writerLogger.Object, _storageProvider, Options.Create(new WriterConfig { }), false);

        return (datasetReader, datasetWriter);
    }

}