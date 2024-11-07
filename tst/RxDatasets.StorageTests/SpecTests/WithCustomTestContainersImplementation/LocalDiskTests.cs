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

public class LocalDiskDatasetTests : StorageTestSpec, IDisposable
{
    private readonly INotificationsHub<object> _notificationsHub = new NotificationsHubSync<object>();


    private readonly IStorageProvider<object> _storageProvider;
    private readonly IOptions<LocalDiskStorageConfig> _config;

    public LocalDiskDatasetTests()
    {
        _config = Options.Create(new LocalDiskStorageConfig());
        _config.Value.RootFolder = Path.Combine(DatasetHelper.GetTempFileDirectory(), "UnitTestsData/");

        if (Directory.Exists(_config.Value.RootFolder))
        {
            Directory.Delete(_config.Value.RootFolder, recursive: true);
        }

        LocalDiskStorageProvider<object> localStorageProvider = new LocalDiskStorageProvider<object>(new Mock<ILogger<LocalDiskStorageProvider<object>>>().Object, _config);
        _storageProvider = new ObjectStorageProvider<object>(new Mock<ILogger<ObjectStorageProvider<object>>>().Object, Options.Create(new StorageConfig()), localStorageProvider, _notificationsHub, false);
        ((ObjectStorageProvider<object>)_storageProvider).StartAsync(CancellationToken.None).Wait();

    }

    protected override Task<(DatasetReader<object>, DatasetWriter<object>)> SetupStorageAsync(string memberName = "")
    {
        var readerLogger = new Mock<ILogger<DatasetReader<object>>>();
        var writerLogger = new Mock<ILogger<DatasetWriter<object>>>();

        var datasetReader = new DatasetReader<object>(_notificationsHub, readerLogger.Object, _storageProvider, false);
        var datasetWriter = new DatasetWriter<object>(_notificationsHub, writerLogger.Object, _storageProvider, Options.Create(new WriterConfig { }), false);

        return Task.FromResult((datasetReader, datasetWriter));
    }

    public void Dispose()
    {
        if(Directory.Exists(_config.Value.RootFolder))
            Directory.Delete(_config.Value.RootFolder, true);
    }
}