using System.Runtime.CompilerServices;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProvider.S3;
using RxDatasets.StorageProviders;
using RxDatasets.StorageTests.Common;

namespace RxDatasets.StorageTests.SpecTests.WithCustomTestContainersImplementation;

public class S3StorageDatasetTests(MinioTestContainerFixture minioContainer)
    : StorageTestSpec, IClassFixture<MinioTestContainerFixture>, IAsyncLifetime
{
    private IOptions<S3Config>? _s3Config;

    public async Task InitializeAsync()
    {
        if (minioContainer.Container.State != DotNet.Testcontainers.Containers.TestcontainersStates.Running)
            await minioContainer.Container.StartAsync();
        _s3Config = Options.Create<S3Config>(new()
        {
            AccessKey = minioContainer.Container.GetAccessKey(),
            SecretKey = minioContainer.Container.GetSecretKey(),
            ServiceUrl = $"http://{minioContainer.Container.Hostname}:{minioContainer.Container.GetMappedPublicPort(9000)}/"
        });
    }

    public async Task DisposeAsync()
    {
        await minioContainer.DisposeAsync();
    }


    protected override async Task<(DatasetReader<object>, DatasetWriter<object>)> SetupStorageAsync([CallerMemberName] string memberName = "")
    {
        _s3Config!.Value.BucketName = new string(memberName.ToLower().Replace("_", "").TakeLast(63).ToArray());
        var storageLogger = new Mock<ILogger<S3StorageProvider<object>>>();

        var persistentStorageLogger = new Mock<ILogger<ObjectStorageProvider<object>>>();

        INotificationsHub<object> notificationsHub = new NotificationsHubSync<object>();


        IObjectStorageProvider<object> s3StorageProvider = new S3StorageProvider<object>(storageLogger.Object, _s3Config!, false);
        IStorageProvider<object> persistentStorageProvider = new ObjectStorageProvider<object>(persistentStorageLogger.Object, DtoOptions, s3StorageProvider, notificationsHub, false);

        await ((ObjectStorageProvider<object>)persistentStorageProvider).StartAsync(CancellationToken.None);

        var datasetReader = new DatasetReader<object>(notificationsHub, Mock.Of<ILogger<DatasetReader<object>>>(), persistentStorageProvider, false);
        var datasetWriter = new DatasetWriter<object>(notificationsHub, Mock.Of<ILogger<DatasetWriter<object>>>(), persistentStorageProvider, Options.Create(new WriterConfig { }), false);


        return (datasetReader, datasetWriter);
    }


    [Fact]
    public async Task Should_Throw_Invalid_Operation_Exception_On_Inaccessible_S3_After_Initialization()
    {
        _s3Config!.Value.BucketName = new string(
            nameof(Should_Throw_Invalid_Operation_Exception_On_Inaccessible_S3_After_Initialization).ToLower().Replace("_", "").TakeLast(63).ToArray()
        );
        var (_, datasetWriter) = await SetupStorageAsync();
        await minioContainer.Container.StopAsync(CancellationToken.None);

        var bytes = GenerateRandomData();

        var putAction = () => datasetWriter.Put("Should_Throw_Invalid_Operation_Exception_On_Inaccessible_S3_Storage_After_Initialization", bytes);
        await putAction.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task Should_Not_Throw_Invalid_Operation_Exception_On_Accessible_S3()
    {
        var (_, azBlobStorageDatasetWriter) = await SetupStorageAsync();

        var bytes = GenerateRandomData();

        var putAction = () => azBlobStorageDatasetWriter.Put("Should_Not_Throw_Invalid_Operation_Exception_On_Accessible_S3_Storage", bytes);
        await putAction.Should().NotThrowAsync<InvalidOperationException>();
    }
}