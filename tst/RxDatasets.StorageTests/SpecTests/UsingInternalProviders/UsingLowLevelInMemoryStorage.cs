using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProvider.InMemory;
using RxDatasets.StorageProviders;
using RxDatasets.StorageTests.Common;

namespace RxDatasets.StorageTests.SpecTests.UsingInternalProviders;

public class UsingLowLevelInMemoryStorage : StorageTestSpec
{
    private readonly LowLevelInMemoryStorageProvider<object> _storageProvider;

    public UsingLowLevelInMemoryStorage()
    {
        var storageLogger = new Mock<ILogger<LowLevelInMemoryStorageProvider<object>>>();

        _storageProvider = new LowLevelInMemoryStorageProvider<object>(storageLogger.Object);

    }

    protected override async Task<(DatasetReader<object>, DatasetWriter<object>)> SetupStorageAsync([CallerMemberName] string memberName = "")
    {
        var BucketName = new string(memberName.ToLower().Replace("_", "").TakeLast(63).ToArray());

        var persistentStorageLogger = new Mock<ILogger<ObjectStorageProvider<object>>>();

        INotificationsHub<object> notificationsHub = new NotificationsHubSync<object>();

        IStorageProvider<object> persistentStorageProvider = new ObjectStorageProvider<object>(persistentStorageLogger.Object, DtoOptions, _storageProvider, notificationsHub, false);

        await ((ObjectStorageProvider<object>)persistentStorageProvider).StartAsync(CancellationToken.None);

        var datasetReader = new DatasetReader<object>(notificationsHub, Mock.Of<ILogger<DatasetReader<object>>>(), persistentStorageProvider, false);
        var datasetWriter = new DatasetWriter<object>(notificationsHub, Mock.Of<ILogger<DatasetWriter<object>>>(), persistentStorageProvider, Options.Create(new WriterConfig { }), false);


        return (datasetReader, datasetWriter);
    }
}