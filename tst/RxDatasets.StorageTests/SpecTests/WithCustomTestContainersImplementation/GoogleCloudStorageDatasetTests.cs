using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Reactive.Testing;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProvider.GoogleCloud;
using RxDatasets.StorageProviders;
using RxDatasets.StorageTests.Common;

namespace RxDatasets.StorageTests.SpecTests.WithCustomTestContainersImplementation;

[Collection(FakeGcsCollection.Name)]
public class GoogleCloudStorageDatasetTests : StorageTestSpecWithTwoStepInit, IClassFixture<GoogleCloudContainerInstanceFixture>, IAsyncLifetime
{
    private readonly GoogleCloudContainerInstanceFixture _googleCloudContainer;
    private IOptions<GoogleCloudConfig>? _googleCloudConfig;

    public GoogleCloudStorageDatasetTests(GoogleCloudContainerInstanceFixture googleCloudContainer)
    {
        _googleCloudContainer = googleCloudContainer;
    }

    public async Task InitializeAsync()
    {
        if (_googleCloudContainer.Container.State != DotNet.Testcontainers.Containers.TestcontainersStates.Running)
            await _googleCloudContainer.Container.StartAsync();

        // Set the emulator host environment variable for the Google Cloud Storage client
        var emulatorHost = _googleCloudContainer.GetConnectionString().Replace("http://", "").Replace("https://", "").TrimEnd('/');
        Environment.SetEnvironmentVariable("STORAGE_EMULATOR_HOST", emulatorHost);

        // Disable authentication for the emulator
        Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", "");
        Environment.SetEnvironmentVariable("GCLOUD_PROJECT", "test-project");

        _googleCloudConfig = Options.Create(new GoogleCloudConfig()
        {
            BucketName = "default-test-bucket", // Will be overridden in SetupStorageAsync
            ProjectId = "test-project", // Required by GoogleCloudConfig but not used by emulator
            EmulatorHost = emulatorHost
        });
    }

    public async Task DisposeAsync()
    {
        Environment.SetEnvironmentVariable("STORAGE_EMULATOR_HOST", null);
        Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", null);
        Environment.SetEnvironmentVariable("GCLOUD_PROJECT", null);
        await _googleCloudContainer.DisposeAsync();
    }

    protected override async Task<(DatasetReader<object>, DatasetWriter<object>)> SetupStorageAsync(
        CancellationToken ct,
        [CallerMemberName] string memberName = "")
    {
        _googleCloudConfig!.Value.BucketName = new string(memberName.ToLower().Replace("_", "").TakeLast(63).ToArray());

        // Create a test client to pre-create the bucket for testing
        var testClientBuilder = new Google.Cloud.Storage.V1.StorageClientBuilder();
        var emulatorHost = _googleCloudConfig.Value.EmulatorHost ?? Environment.GetEnvironmentVariable("STORAGE_EMULATOR_HOST");
        if (!string.IsNullOrEmpty(emulatorHost))
        {
            testClientBuilder.BaseUri = $"http://{emulatorHost}/storage/v1/";
            testClientBuilder.UnauthenticatedAccess = true;
        }
        var testClient = testClientBuilder.Build();

        // Create bucket for this test
        try
        {
            await testClient.CreateBucketAsync("test-project", new Google.Apis.Storage.v1.Data.Bucket
            {
                Name = _googleCloudConfig.Value.BucketName,
                Location = "us-central1"
            });
        }
        catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.Conflict)
        {
            // Bucket already exists, which is fine
        }

        var storageLogger = new Mock<ILogger<GoogleCloudStorage<object>>>();

        var persistentStorageLogger = new Mock<ILogger<ObjectStorageProvider<object>>>();

        INotificationsHub<object> notificationsHub = new NotificationsHubSync<object>();

        IDiskStorage<object> googleCloudStorage = new GoogleCloudStorage<object>(
            storageLogger.Object,
            _googleCloudConfig,
            retryInitialization: false,
            enableWriteQueue: false);
        IStorageReader<object> readOnlyStorage = new ObjectStorageReader<object>(Mock.Of<ILogger<ObjectStorageReader<object>>>(), StorageConfig, googleCloudStorage);

        IStorageProvider<object> persistentStorageProvider = new ObjectStorageProvider<object>(persistentStorageLogger.Object, StorageConfig, googleCloudStorage, notificationsHub, readOnlyStorage, false);
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
    public async Task Should_Throw_Invalid_Operation_Exception_On_Inaccessible_GoogleCloud_Storage_After_Initialization()
    {
        _googleCloudConfig!.Value.BucketName = new string(
            nameof(Should_Throw_Invalid_Operation_Exception_On_Inaccessible_GoogleCloud_Storage_After_Initialization).ToLower().Replace("_", "").TakeLast(63).ToArray()
        );
        var (_, googleCloudStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);
        await _googleCloudContainer.Container.StopAsync(CancellationToken.None);

        var bytes = GenerateRandomData();

        var putAction = () => googleCloudStorageDatasetWriter.Put("Should_Throw_Invalid_Operation_Exception_On_Inaccessible_GoogleCloud_Storage_After_Initialization", bytes, null, null);
        await putAction.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task Should_Not_Throw_Invalid_Operation_Exception_On_Accessible_GoogleCloud_Storage()
    {
        var (_, googleCloudStorageDatasetWriter) = await SetupStorageAsync(CancellationToken.None);

        var bytes = GenerateRandomData();

        var putAction = () => googleCloudStorageDatasetWriter.Put("Should_Not_Throw_Invalid_Operation_Exception_On_Accessible_GoogleCloud_Storage", bytes, null, null);
        await putAction.Should().NotThrowAsync<InvalidOperationException>();
    }
}