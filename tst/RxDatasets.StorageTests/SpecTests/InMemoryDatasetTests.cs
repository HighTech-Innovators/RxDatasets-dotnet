using Microsoft.Extensions.Logging;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Notifications;
using RxDatasets.StorageProviders;
using RxDatasets.StorageTests.Common;

namespace RxDatasets.StorageTests.SpecTests;

public class InMemoryDatasetTests : StorageTestSpec
{
    private readonly INotificationsHub<object> _notificationsHub = new NotificationsHubSync<object>();


    private IStorageProvider<object> _storageProvider;
    public InMemoryDatasetTests()
    {
        _storageProvider = new InMemoryStorageProvider<object>(new Mock<ILogger<InMemoryStorageProvider<object>>>().Object, _notificationsHub);
    }

    protected override async Task<(DatasetReader<object>, DatasetWriter<object>)> SetupStorageAsync(string memberName = "")
    {
        var readerLogger = new Mock<ILogger<DatasetReader<object>>>();
        var writerLogger = new Mock<ILogger<DatasetWriter<object>>>();

        
        var datasetReader = new DatasetReader<object>(_notificationsHub, readerLogger.Object, _storageProvider, false);
        var datasetWriter = new DatasetWriter<object>(_notificationsHub, writerLogger.Object, _storageProvider, false);

        return (datasetReader, datasetWriter);
    }

}