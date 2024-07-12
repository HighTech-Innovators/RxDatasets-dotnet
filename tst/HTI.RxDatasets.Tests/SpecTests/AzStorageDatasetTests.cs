using System.Runtime.CompilerServices;
using FluentAssertions;
using HTI.RxDatasets.Abstractions;
using HTI.RxDatasets.DatasetUpdateNotifications;
using HTI.RxDatasets.StorageProvider.AzBlob;
using HTI.RxDatasets.StorageProviders;
using HTI.RxDatasets.Tests.Common;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;

namespace HTI.RxDatasets.Tests.SpecTests;

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
            ConnectionString = _azuriteContainer.Container.GetConnectionString(),

        });
    }

    public async Task DisposeAsync()
    {
        await _azuriteContainer.DisposeAsync();
    }

    protected override async Task<(DatasetReader<object>, DatasetWriter<object>)> SetupStorageAsync([CallerMemberName] string memberName = "")
    {
        DtoOptions.Value.BucketName = new string(memberName.ToLower().Replace("_", "").TakeLast(63).ToArray());

        _azConfig = Options.Create(new AzStorageConfig()
        {
            ConnectionString = _azuriteContainer.Container.GetConnectionString(),
        });
        var storageLogger = new Mock<ILogger<AzStorageProvider<object>>>();

        var persistentStorageLogger = new Mock<ILogger<ObjectStorageProvider<object>>>();

        DatasetUpdateNotifierSync<object> datasetUpdateNotifier = new DatasetUpdateNotifierSync<object>();

        IObjectStorageProvider<object> azStorageProvider = new AzStorageProvider<object>(storageLogger.Object, _azConfig, DtoOptions, false);
        IStorageProvider<object> persistentStorageProvider = new ObjectStorageProvider<object>(persistentStorageLogger.Object, azStorageProvider, datasetUpdateNotifier);

        var datasetReader = new DatasetReader<object>(datasetUpdateNotifier, DtoOptions, Mock.Of<ILogger<DatasetReader<object>>>(), persistentStorageProvider, false);
        var datasetWriter = new DatasetWriter<object>(datasetUpdateNotifier, DtoOptions, Mock.Of<ILogger<DatasetWriter<object>>>(), persistentStorageProvider, false);

        await ((ObjectStorageProvider<object>)persistentStorageProvider).StartAsync(CancellationToken.None);


        await datasetReader.StartAsync(CancellationToken.None);
        await datasetWriter.StartAsync(CancellationToken.None);

        return (datasetReader, datasetWriter);
    }


    [Fact]
    public async Task Should_Throw_Invalid_Operation_Exception_On_Inaccessible_AZ_Storage_After_Initialization()
    {
        var config = DtoOptions;

        config.Value.BucketName = new string(
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