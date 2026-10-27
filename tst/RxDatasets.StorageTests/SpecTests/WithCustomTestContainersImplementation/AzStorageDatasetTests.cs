using System.Runtime.CompilerServices;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Reactive.Testing;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProvider.AzBlob;
using RxDatasets.StorageProviders;
using RxDatasets.StorageTests.Common;

namespace RxDatasets.StorageTests.SpecTests.WithCustomTestContainersImplementation;

public class AzStorageDatasetTests : StorageTestSpecWithTwoStepInit, IClassFixture<AzuriteContainerInstanceFixture>, IAsyncLifetime
{
    private readonly AzuriteContainerInstanceFixture _azuriteContainer;
    private IOptions<AzStorageConfig>? _azConfig;

    public AzStorageDatasetTests(AzuriteContainerInstanceFixture azuriteContainer)
    {
        _azuriteContainer = azuriteContainer;
    }

    public async Task InitializeAsync()
    {
        if (_azuriteContainer.Container.State != DotNet.Testcontainers.Containers.TestcontainersStates.Running)
            await _azuriteContainer.Container.StartAsync();

        _azConfig = Options.Create(new AzStorageConfig()
        {
            ConnectionString = _azuriteContainer.Container.GetConnectionString()

        });
    }

    public async Task DisposeAsync()
    {
        await _azuriteContainer.DisposeAsync();
    }

    protected override async Task<(DatasetReader<object>, DatasetWriter<object>)> SetupStorageAsync(
        CancellationToken ct,
        [CallerMemberName] string memberName = "")
    {
        _azConfig!.Value.ContainerName = new string(memberName.ToLower().Replace("_", "").TakeLast(63).ToArray());
        _azConfig.Value.ConnectionString = _azuriteContainer.Container.GetConnectionString();

        var storageLogger = new Mock<ILogger<AzStorage<object>>>();

        var persistentStorageLogger = new Mock<ILogger<ObjectStorageProvider<object>>>();

        INotificationsHub<object> notificationsHub = new NotificationsHubSync<object>();


        IDiskStorage<object> azStorage = new AzStorage<object>(storageLogger.Object, _azConfig, false);
        IStorageReader<object> readOnlyStorage = new ObjectStorageReader<object>(Mock.Of<ILogger<ObjectStorageReader<object>>>(), StorageConfig, azStorage);

        IStorageProvider<object> persistentStorageProvider = new ObjectStorageProvider<object>(persistentStorageLogger.Object, StorageConfig, azStorage, notificationsHub, readOnlyStorage, false);
        await ((ObjectStorageProvider<object>)persistentStorageProvider).StartAsync(CancellationToken.None);

        var datasetReader = new DatasetReader<object>(Mock.Of<ILogger<DatasetReader<object>>>(), persistentStorageProvider, notificationsHub);
        await datasetReader.StartAsync(CancellationToken.None);

        IOptions<WriterConfig> writerConfig = Options.Create(new WriterConfig { });
        var datasetWriter = new DatasetWriter<object>(notificationsHub, Mock.Of<ILogger<DatasetWriter<object>>>(), persistentStorageProvider, writerConfig, false);

        var snapshotCreationTaskLogger = new Mock<ILogger<SnapshotCreationPlugin<object>>>();
        var datasetUpdateConsumingServiceLogger = new Mock<ILogger<DatasetUpdateConsumingService<object, SnapshotCreationPlugin<object>>>>();
        var snapshotCreationPlugin = new SnapshotCreationPlugin<object>(snapshotCreationTaskLogger.Object, persistentStorageProvider, SnapshotsOptions, new TestScheduler());
        var datasetUpdateConsumingService = new DatasetUpdateConsumingService<object, SnapshotCreationPlugin<object>>(datasetUpdateConsumingServiceLogger.Object, notificationsHub, snapshotCreationPlugin);
        _ = datasetUpdateConsumingService.StartAsync(CancellationToken.None);
        return (datasetReader, datasetWriter);
    }


    [Fact]
    public async Task Conditional_Operations_Follow_The_Contract()
    {
        _azConfig!.Value.ContainerName = "conditional-ops";
        await ConditionalStorageContract.VerifyAsync(
            new AzStorage<object>(Mock.Of<ILogger<AzStorage<object>>>(), _azConfig, false));
    }

    [Fact]
    public async Task Should_Throw_Invalid_Operation_Exception_On_Inaccessible_AZ_Storage_After_Initialization()
    {
        _azConfig!.Value.ContainerName = new string(
            nameof(Should_Throw_Invalid_Operation_Exception_On_Inaccessible_AZ_Storage_After_Initialization).ToLower().Replace("_", "").TakeLast(63).ToArray()
        );
        var (_, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);
        await _azuriteContainer.Container.StopAsync(CancellationToken.None);

        var bytes = GenerateRandomData();

        var putAction = () => azBlobStorageDatasetWriter.Put("Should_Throw_Invalid_Operation_Exception_On_Inaccessible_AZ_Storage_After_Initialization", bytes, null, null);
        await putAction.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task Should_Not_Throw_Invalid_Operation_Exception_On_Accessible_AZ_Storage()
    {
        var (_, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);

        var bytes = GenerateRandomData();

        var putAction = () => azBlobStorageDatasetWriter.Put("Should_Not_Throw_Invalid_Operation_Exception_On_Accessible_AZ_Storage", bytes, null, null);
        await putAction.Should().NotThrowAsync<InvalidOperationException>();
    }
}