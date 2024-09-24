using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Notifications;
using RxDatasets.StorageProvider.LocalDisk;
using RxDatasets.StorageProviders;
using RxDatasets.StorageTests.Common;

namespace RxDatasets.StorageTests.SpecTests;

public class LocalDiskDatasetTests : StorageTestSpec
{
    private readonly INotificationsHub<object> _notificationsHub = new NotificationsHubSync<object>();


    private IStorageProvider<object> _storageProvider;
    public LocalDiskDatasetTests()
    {
        _storageProvider = new ObjectStorageProvider<object>(new Mock<ILogger<ObjectStorageProvider<object>>>().Object, Options.Create(new StorageConfig()), new LocalDiskStorageProvider<object>(new Mock<ILogger<LocalDiskStorageProvider<object>>>().Object, Options.Create(new LocalDiskStorageConfig())), _notificationsHub);
    }

    protected override async Task<(DatasetReader<object>, DatasetWriter<object>)> SetupStorageAsync(string memberName = "")
    {
        var readerLogger = new Mock<ILogger<DatasetReader<object>>>();
        var writerLogger = new Mock<ILogger<DatasetWriter<object>>>();

        
        var datasetReader = new DatasetReader<object>(_notificationsHub, DtoOptions, readerLogger.Object, _storageProvider, false);
        var datasetWriter = new DatasetWriter<object>(_notificationsHub, DtoOptions, writerLogger.Object, _storageProvider, false);

        await datasetReader.StartAsync(CancellationToken.None);
        await datasetWriter.StartAsync(CancellationToken.None);

        return (datasetReader, datasetWriter);
    }

}