using Microsoft.Extensions.Logging;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.DatasetUpdateNotifications;
using RxDatasets.StorageProviders;
using RxDatasets.StorageTests.Common;

namespace RxDatasets.StorageTests.SpecTests;

public class InMemoryDatasetTests : StorageTestSpec
{
    DatasetUpdateNotifierSync<object> datasetUpdateNotifier;
    private IStorageProvider<object> _storageProvider;
    public InMemoryDatasetTests()
    {
       datasetUpdateNotifier = new DatasetUpdateNotifierSync<object>();
        _storageProvider = new InMemoryStorageProvider<object>(new Mock<ILogger<InMemoryStorageProvider<object>>>().Object, datasetUpdateNotifier);
    }

    protected override async Task<(DatasetReader<object>, DatasetWriter<object>)> SetupStorageAsync(string memberName = "")
    {
        var readerLogger = new Mock<ILogger<DatasetReader<object>>>();
        var writerLogger = new Mock<ILogger<DatasetWriter<object>>>();

        
        var datasetReader = new DatasetReader<object>(datasetUpdateNotifier, DtoOptions, readerLogger.Object, _storageProvider, false);
        var datasetWriter = new DatasetWriter<object>(datasetUpdateNotifier, DtoOptions, writerLogger.Object, _storageProvider, false);

        await datasetReader.StartAsync(CancellationToken.None);
        await datasetWriter.StartAsync(CancellationToken.None);

        return (datasetReader, datasetWriter);
    }

}