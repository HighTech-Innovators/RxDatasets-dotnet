using System.Runtime.CompilerServices;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.DatasetUpdateNotifications;
using RxDatasets.StorageProvider.Minio;
using RxDatasets.StorageProviders;
using RxDatasets.StorageTests.Common;

namespace RxDatasets.StorageTests.SpecTests;

public class S3StorageDatasetTests : StorageTestSpec, IClassFixture<MinioTestContainerFixture>, IAsyncLifetime
{
    private readonly MinioTestContainerFixture _minioContainer;
    private IOptions<MinioS3Config>? _s3Config;

    public S3StorageDatasetTests(MinioTestContainerFixture minioContainer)
    {
        _minioContainer = minioContainer;
    }

    public async Task InitializeAsync()
    {
        if (_minioContainer.Container.State != DotNet.Testcontainers.Containers.TestcontainersStates.Running)
            await _minioContainer.Container.StartAsync();
        _s3Config = Options.Create<MinioS3Config>(new()
        {
            AccessKey = _minioContainer.Container.GetAccessKey(),
            SecretKey = _minioContainer.Container.GetSecretKey(),
            ServiceURL = $"{_minioContainer.Container.Hostname}:{_minioContainer.Container.GetMappedPublicPort(9000)}"
        });
    }

    public async Task DisposeAsync()
    {
        await _minioContainer.DisposeAsync();
    }
    
    
    protected override async Task<(DatasetReader<object>, DatasetWriter<object>)> SetupStorageAsync([CallerMemberName] string memberName = "")
    {
        _s3Config!.Value.BucketName = new string(memberName.ToLower().Replace("_", "").TakeLast(63).ToArray());
        var storageLogger = new Mock<ILogger<MinioS3StorageProvider<object>>>();

        var persistentStorageLogger = new Mock<ILogger<ObjectStorageProvider<object>>>();

        var datasetUpdateNotifier = new DatasetUpdateNotifierSync<object>();

        IObjectStorageProvider<object> s3StorageProvider = new MinioS3StorageProvider<object>(storageLogger.Object, _s3Config!, false);
        IStorageProvider<object> persistentStorageProvider = new ObjectStorageProvider<object>(persistentStorageLogger.Object, DtoOptions, (s3StorageProvider), datasetUpdateNotifier);

        await ((ObjectStorageProvider<object>)persistentStorageProvider).StartAsync(CancellationToken.None);


        var datasetReader = new DatasetReader<object>(datasetUpdateNotifier, DtoOptions, Mock.Of<ILogger<DatasetReader<object>>>(), persistentStorageProvider, false);
        var datasetWriter = new DatasetWriter<object>(datasetUpdateNotifier, DtoOptions, Mock.Of<ILogger<DatasetWriter<object>>>(), persistentStorageProvider, false);

        await datasetReader.StartAsync(CancellationToken.None);
        await datasetWriter.StartAsync(CancellationToken.None);

         return (datasetReader, datasetWriter);
    }


    [Fact]
    public async Task Should_Throw_Invalid_Operation_Exception_On_Inaccessible_S3_After_Initialization()
    {
        _s3Config!.Value.BucketName = new string(
            nameof(Should_Throw_Invalid_Operation_Exception_On_Inaccessible_S3_After_Initialization).ToLower().Replace("_", "").TakeLast(63).ToArray()
        );
        var (_, datasetWriter) = await SetupStorageAsync();
        await _minioContainer.Container.StopAsync(CancellationToken.None);

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