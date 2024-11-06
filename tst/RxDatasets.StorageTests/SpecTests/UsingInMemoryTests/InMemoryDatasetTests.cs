using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProviders;
using RxDatasets.StorageTests.Common;

namespace RxDatasets.StorageTests.SpecTests.UsingInMemoryTests;

public class InMemoryDatasetTests : StorageTestSpec
{
    private readonly INotificationsHub<object> _notificationsHub = new NotificationsHubSync<object>();


    private readonly IStorageProvider<object> _storageProvider;
    public InMemoryDatasetTests()
    {
        _storageProvider = new InMemoryStorageProvider<object>(new Mock<ILogger<InMemoryStorageProvider<object>>>().Object, _notificationsHub);
    }

    protected override Task<(DatasetReader<object>, DatasetWriter<object>)> SetupStorageAsync(string memberName = "")
    {
        var readerLogger = new Mock<ILogger<DatasetReader<object>>>();
        var writerLogger = new Mock<ILogger<DatasetWriter<object>>>();


        var datasetReader = new DatasetReader<object>(_notificationsHub, readerLogger.Object, _storageProvider, false);
        var datasetWriter = new DatasetWriter<object>(_notificationsHub, writerLogger.Object, _storageProvider, Options.Create(new WriterConfig { }), false);

        return Task.FromResult((datasetReader, datasetWriter));
    }

}