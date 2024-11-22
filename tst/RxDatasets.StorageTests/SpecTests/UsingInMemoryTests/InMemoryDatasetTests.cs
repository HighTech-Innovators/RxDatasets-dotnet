using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Reactive.Testing;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
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

    protected override async Task<(DatasetReader<object>, DatasetWriter<object>)> SetupStorageAsync(CancellationToken ct,
        string memberName = "")
    {
        var readerLogger = new Mock<ILogger<DatasetReader<object>>>();
        var writerLogger = new Mock<ILogger<DatasetWriter<object>>>();


        var datasetReader = new DatasetReader<object>(_notificationsHub, readerLogger.Object, _storageProvider, false);
        var datasetWriter = new DatasetWriter<object>(_notificationsHub, writerLogger.Object, _storageProvider, Options.Create(new WriterConfig { }), false);

        var snapshotCreationTaskLogger = new Mock<ILogger<SnapshotCreationPlugin<object>>>();
        var datasetUpdateConsumingServiceLogger = new Mock<ILogger<DatasetUpdateConsumingService<object, SnapshotCreationPlugin<object>>>>();
        var snapshotCreationPlugin = new SnapshotCreationPlugin<object>(snapshotCreationTaskLogger.Object, _storageProvider, SnapshotsOptions, new TestScheduler());
        var datasetUpdateConsumingService = new DatasetUpdateConsumingService<object, SnapshotCreationPlugin<object>> (datasetUpdateConsumingServiceLogger.Object, _notificationsHub, snapshotCreationPlugin);
        _ = datasetUpdateConsumingService.StartAsync(ct);
        return (datasetReader, datasetWriter);
    }

}