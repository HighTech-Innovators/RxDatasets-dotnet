using System.Runtime.CompilerServices;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProvider.AzBlob;
using RxDatasets.StorageProviders;
using RxDatasets.StorageTests.Common;

namespace RxDatasets.StorageTests.SpecTests;

public class AzStorageDatasetTests : StorageTestSpec, IClassFixture<AzuriteContainerInstanceFixture>, IAsyncLifetime
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

    protected override async Task<(DatasetReader<object>, DatasetWriter<object>)> SetupStorageAsync([CallerMemberName] string memberName = "")
    {
        _azConfig!.Value.ContainerName = new string(memberName.ToLower().Replace("_", "").TakeLast(63).ToArray());
        _azConfig.Value.ConnectionString = _azuriteContainer.Container.GetConnectionString(); 

        var storageLogger = new Mock<ILogger<AzStorageProvider<object>>>();

        var persistentStorageLogger = new Mock<ILogger<ObjectStorageProvider<object>>>();

        INotificationsHub<object> notificationsHub = new NotificationsHubSync<object>();


        IObjectStorageProvider<object> azStorageProvider = new AzStorageProvider<object>(storageLogger.Object, _azConfig, false);
        IStorageProvider<object> persistentStorageProvider = new ObjectStorageProvider<object>(persistentStorageLogger.Object, DtoOptions, azStorageProvider, notificationsHub, false);
        await ((ObjectStorageProvider<object>)persistentStorageProvider).StartAsync(CancellationToken.None);

        var datasetReader = new DatasetReader<object>(notificationsHub, Mock.Of<ILogger<DatasetReader<object>>>(), persistentStorageProvider, false);
        IOptions<WriterConfig> writerConfig = Options.Create(new WriterConfig { });
        var datasetWriter = new DatasetWriter<object>(notificationsHub, Mock.Of<ILogger<DatasetWriter<object>>>(), persistentStorageProvider, writerConfig, false);

        return (datasetReader, datasetWriter);
    }


    [Fact]
    public async Task Should_Throw_Invalid_Operation_Exception_On_Inaccessible_AZ_Storage_After_Initialization()
    {
        var config = DtoOptions;

        _azConfig!.Value.ContainerName = new string(
            nameof(Should_Throw_Invalid_Operation_Exception_On_Inaccessible_AZ_Storage_After_Initialization).ToLower().Replace("_", "").TakeLast(63).ToArray()
        );
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();
        await _azuriteContainer.Container.StopAsync(CancellationToken.None);

        var bytes = GenerateRandomData();

        var putAction = () => azBlobStorageDatasetWriter.Put("Should_Throw_Invalid_Operation_Exception_On_Inaccessible_AZ_Storage_After_Initialization", bytes);
        await putAction.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task Should_Not_Throw_Invalid_Operation_Exception_On_Accessible_AZ_Storage()
    {
        var (azBlobStorageDatasetReader, azBlobStorageDatasetWriter) = await SetupStorageAsync();

        var bytes = GenerateRandomData();

        var putAction = () => azBlobStorageDatasetWriter.Put("Should_Not_Throw_Invalid_Operation_Exception_On_Accessible_AZ_Storage", bytes);
        await putAction.Should().NotThrowAsync<InvalidOperationException>();
    }
}