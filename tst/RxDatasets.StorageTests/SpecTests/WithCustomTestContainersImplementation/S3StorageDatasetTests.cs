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
using RxDatasets.StorageProvider.S3;
using RxDatasets.StorageProviders;
using RxDatasets.StorageTests.Common;

namespace RxDatasets.StorageTests.SpecTests.WithCustomTestContainersImplementation;

public class S3StorageDatasetTests(MinioTestContainerFixture minioContainer)
    : StorageTestSpecWithTwoStepInit, IClassFixture<MinioTestContainerFixture>, IAsyncLifetime
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


    protected override async Task<(DatasetReader<object>, DatasetWriter<object>)> SetupStorageAsync(
        CancellationToken ct,
        [CallerMemberName] string memberName = "")
    {
        _s3Config!.Value.BucketName = new string(memberName.ToLower().Replace("_", "").TakeLast(63).ToArray());
        var storageLogger = new Mock<ILogger<S3Storage<object>>>();

        var persistentStorageLogger = new Mock<ILogger<ObjectStorageProvider<object>>>();

        INotificationsHub<object> notificationsHub = new NotificationsHubSync<object>();


        IDiskStorage<object> s3Storage = new S3Storage<object>(storageLogger.Object, _s3Config!, false);
        IStorageReader<object> storageReader = new ObjectStorageReader<object>(Mock.Of<ILogger<ObjectStorageReader<object>>>(), StorageConfig, s3Storage);
        IStorageProvider<object> persistentStorageProvider = new ObjectStorageProvider<object>(persistentStorageLogger.Object, StorageConfig, s3Storage, notificationsHub,  storageReader,false);

        await ((ObjectStorageProvider<object>)persistentStorageProvider).StartAsync(CancellationToken.None);
        

        var datasetReader = new DatasetReader<object>(Mock.Of<ILogger<DatasetReader<object>>>(), persistentStorageProvider, notificationsHub);
        await datasetReader.StartAsync(CancellationToken.None);
        var datasetWriter = new DatasetWriter<object>(notificationsHub, Mock.Of<ILogger<DatasetWriter<object>>>(), persistentStorageProvider, Options.Create(new WriterConfig { }), false);

        var snapshotCreationTaskLogger = new Mock<ILogger<SnapshotCreationPlugin<object>>>();
        var datasetUpdateConsumingServiceLogger = new Mock<ILogger<DatasetUpdateConsumingService<object, SnapshotCreationPlugin<object>>>>();
        var snapshotCreationPlugin = new SnapshotCreationPlugin<object>(snapshotCreationTaskLogger.Object, persistentStorageProvider, SnapshotsOptions, new TestScheduler());
        var datasetUpdateConsumingService = new DatasetUpdateConsumingService<object, SnapshotCreationPlugin<object>> (datasetUpdateConsumingServiceLogger.Object, notificationsHub, snapshotCreationPlugin);
        await datasetUpdateConsumingService.StartAsync(CancellationToken.None);
        return (datasetReader, datasetWriter);
    }


    [Fact]
    public async Task Should_Throw_Invalid_Operation_Exception_On_Inaccessible_S3_After_Initialization()
    {
        _s3Config!.Value.BucketName = new string(
            nameof(Should_Throw_Invalid_Operation_Exception_On_Inaccessible_S3_After_Initialization).ToLower().Replace("_", "").TakeLast(63).ToArray()
        );
        var (_, datasetWriter) = await SetupStorageAsync(CancellationToken.None);
        await minioContainer.Container.StopAsync(CancellationToken.None);

        var bytes = GenerateRandomData();

        var putAction = () => datasetWriter.Put("Should_Throw_Invalid_Operation_Exception_On_Inaccessible_S3_Storage_After_Initialization", bytes);
        await putAction.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task Should_Not_Throw_Invalid_Operation_Exception_On_Accessible_S3()
    {
        var (_, azBlobStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);

        var bytes = GenerateRandomData();

        var putAction = () => azBlobStorageDatasetWriter.Put("Should_Not_Throw_Invalid_Operation_Exception_On_Accessible_S3_Storage", bytes);
        await putAction.Should().NotThrowAsync<InvalidOperationException>();
    }
    
    [Fact]
    public async Task Should_Delete_Files()
    {
        // Arrange
        _s3Config!.Value.BucketName = new string(nameof(Should_Delete_Files).ToLower().Replace("_", "").TakeLast(63).ToArray());
        var storageLogger = new Mock<ILogger<S3Storage<object>>>();
        var persistentStorageLogger = new Mock<ILogger<ObjectStorageProvider<object>>>();
        INotificationsHub<object> notificationsHub = new NotificationsHubSync<object>();
        IDiskStorage<object> s3Storage = new S3Storage<object>(storageLogger.Object, _s3Config!, false);
        IStorageReader<object> readOnlyStorage = new ObjectStorageReader<object>(Mock.Of<ILogger<ObjectStorageReader<object>>>(), StorageConfig, s3Storage);

        IStorageProvider<object> persistentStorageProvider = new ObjectStorageProvider<object>(persistentStorageLogger.Object, StorageConfig, s3Storage, notificationsHub,  readOnlyStorage, false);
        await ((ObjectStorageProvider<object>)persistentStorageProvider).StartAsync(CancellationToken.None);
        
        await persistentStorageProvider.StoreDelta(1, [1], [1]);
        await persistentStorageProvider.StoreSnapshot(1, [1], [1]);
        await persistentStorageProvider.StoreDelta(2, [2], [2]);
        await persistentStorageProvider.StoreDelta(3, [3], [3]);
        await persistentStorageProvider.StoreSnapshot(3, [3], [3]);

        await persistentStorageProvider.StoreDelta(4, [4], [4]);
        await persistentStorageProvider.StoreDelta(5, [5], [5]);
        await persistentStorageProvider.StoreDelta(6, [6], [6]);
        await persistentStorageProvider.StoreSnapshot(6, [6], [6]);

        var versions = await persistentStorageProvider.ListVersions();
        versions.OldestDelta.Should().Be(1);
        versions.NewestDelta.Should().Be(6);
        versions.Snapshots.MinBy(s=>s.Version)?.Version.Should().Be(1);
        
        // act
        await persistentStorageProvider.DeleteOlderThan(3, false);
        
        
        // Assert

        versions = await persistentStorageProvider.ListVersions();
        versions.OldestDelta.Should().Be(4);
        versions.NewestDelta.Should().Be(6);
        versions.Snapshots.Select(s=>s.Version).Min().Should().Be(3);
        var snapshotsListFromLowLevelStorage = (await s3Storage.ListObjects("Object/snapshots/"))
            .GroupBy(s => SharedMethods.ExtractNumber(s.ObjectKey))
            .Select(g => g.Key);
        snapshotsListFromLowLevelStorage.Min().Should().Be(3);
    }
}