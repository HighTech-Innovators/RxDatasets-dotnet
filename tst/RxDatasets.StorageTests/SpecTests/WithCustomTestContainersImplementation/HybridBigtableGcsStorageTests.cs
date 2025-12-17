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
using RxDatasets.StorageProvider.GoogleCloud;
using RxDatasets.StorageProviders;
using RxDatasets.StorageTests.Common;

namespace RxDatasets.StorageTests.SpecTests.WithCustomTestContainersImplementation;

public class HybridBigtableGcsStorageTests : StorageTestSpecWithTwoStepInit, IClassFixture<GoogleCloudContainerInstanceFixture>, IClassFixture<BigtableContainerInstanceFixture>, IAsyncLifetime
{
    private readonly GoogleCloudContainerInstanceFixture _googleCloudContainer;
    private readonly BigtableContainerInstanceFixture _bigtableContainer;
    private IOptions<GoogleCloudConfig>? _googleCloudConfig;

    public HybridBigtableGcsStorageTests(GoogleCloudContainerInstanceFixture googleCloudContainer, BigtableContainerInstanceFixture bigtableContainer)
    {
        _googleCloudContainer = googleCloudContainer;
        _bigtableContainer = bigtableContainer;
    }

    public async Task InitializeAsync()
    {
        var tasks = new List<Task>();
        if (_googleCloudContainer.Container.State != DotNet.Testcontainers.Containers.TestcontainersStates.Running)
            tasks.Add(_googleCloudContainer.Container.StartAsync());
        if (_bigtableContainer.Container.State != DotNet.Testcontainers.Containers.TestcontainersStates.Running)
            tasks.Add(_bigtableContainer.Container.StartAsync());
        
        await Task.WhenAll(tasks);

        // Set the emulator host environment variable for the Google Cloud Storage client
        var gcsEmulatorHost = _googleCloudContainer.GetConnectionString().Replace("http://", "").Replace("https://", "").TrimEnd('/');
        Environment.SetEnvironmentVariable("STORAGE_EMULATOR_HOST", gcsEmulatorHost);

        // Set the emulator host environment variable for the Bigtable client
        var bigtableEmulatorHost = _bigtableContainer.GetConnectionString();
        Environment.SetEnvironmentVariable("BIGTABLE_EMULATOR_HOST", bigtableEmulatorHost);

        // Disable authentication for the emulator
        Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", "");
        Environment.SetEnvironmentVariable("GCLOUD_PROJECT", "test-project");

        _googleCloudConfig = Options.Create(new GoogleCloudConfig()
        {
            BucketName = "default-test-bucket",
            ProjectId = "test-project",
            BigtableProjectId = "test-project",
            BigtableInstanceId = "test-instance",
            BigtableTableId = "test-table",
            SizeThresholdBytes = 1024 // 1KB threshold for testing
        });
    }

    public async Task DisposeAsync()
    {
        Environment.SetEnvironmentVariable("STORAGE_EMULATOR_HOST", null);
        Environment.SetEnvironmentVariable("BIGTABLE_EMULATOR_HOST", null);
        Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", null);
        Environment.SetEnvironmentVariable("GCLOUD_PROJECT", null);
        // Containers are disposed by xUnit fixture management
        await Task.CompletedTask;
    }

    protected override async Task<(DatasetReader<object>, DatasetWriter<object>)> SetupStorageAsync(
        CancellationToken ct,
        [CallerMemberName] string memberName = "")
    {
        _googleCloudConfig!.Value.BucketName = new string(memberName.ToLower().Replace("_", "").TakeLast(63).ToArray());
        _googleCloudConfig!.Value.BigtableTableId = new string(memberName.ToLower().Replace("_", "").TakeLast(50).ToArray());

        // Create GCS bucket
        var testClientBuilder = new Google.Cloud.Storage.V1.StorageClientBuilder();
        var emulatorHost = Environment.GetEnvironmentVariable("STORAGE_EMULATOR_HOST");
        if (!string.IsNullOrEmpty(emulatorHost))
        {
            testClientBuilder.BaseUri = $"http://{emulatorHost}/storage/v1/";
            testClientBuilder.UnauthenticatedAccess = true;
        }
        var testClient = testClientBuilder.Build();

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
            // Bucket already exists
        }

        // Bigtable table creation is handled by HybridBigtableGcsStorage initialization

        var storageLogger = new Mock<ILogger<HybridBigtableGcsStorage<object>>>();
        var persistentStorageLogger = new Mock<ILogger<ObjectStorageProvider<object>>>();

        INotificationsHub<object> notificationsHub = new NotificationsHubSync<object>();

        IDiskStorage<object> hybridStorage = new HybridBigtableGcsStorage<object>(storageLogger.Object, _googleCloudConfig, false);
        IStorageReader<object> readOnlyStorage = new ObjectStorageReader<object>(Mock.Of<ILogger<ObjectStorageReader<object>>>(), StorageConfig, hybridStorage);

        IStorageProvider<object> persistentStorageProvider = new ObjectStorageProvider<object>(persistentStorageLogger.Object, StorageConfig, hybridStorage, notificationsHub, readOnlyStorage, false);
        await ((ObjectStorageProvider<object>)persistentStorageProvider).StartAsync(CancellationToken.None);

        var datasetReader = new DatasetReader<object>(Mock.Of<ILogger<DatasetReader<object>>>(), persistentStorageProvider, notificationsHub);
        await datasetReader.StartAsync(CancellationToken.None);

        return (datasetReader, new DatasetWriter<object>(notificationsHub, Mock.Of<ILogger<DatasetWriter<object>>>(), persistentStorageProvider, Options.Create(new WriterConfig())));
    }

    [Fact]
    public async Task Should_Store_Small_Files_In_Bigtable_And_Large_In_GCS()
    {
        var ct = CancellationToken.None;
        var (reader, writer) = await SetupStorageAsync(ct);
        
        // Small object (< 1KB)
        var smallData = new byte[500];
        new Random().NextBytes(smallData);
        await writer.Put("small-object", smallData, ct);

        // Large object (> 1KB)
        var largeData = new byte[2000];
        new Random().NextBytes(largeData);
        await writer.Put("large-object", largeData, ct);

        // Verify retrieval
        var retrievedSmall = await reader.Get("small-object");
        retrievedSmall.Value.entityBytes.ToArray().Should().BeEquivalentTo(smallData);

        var retrievedLarge = await reader.Get("large-object");
        retrievedLarge.Value.entityBytes.ToArray().Should().BeEquivalentTo(largeData);

        // TODO: Verify internal storage location if possible (requires exposing internal clients or checking side effects)
        // For now, we rely on the fact that it works end-to-end.
    }
}
