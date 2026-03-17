using System;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using Google.Cloud.Storage.V1;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using RxDatasets.StorageProvider.GoogleCloud;
using Xunit;

namespace RxDatasets.StorageTests.GoogleCloudStorage
{
    public class WriteQueueIntegrationTests : IClassFixture<GoogleCloudContainerInstanceFixture>, IAsyncLifetime
    {
        private readonly GoogleCloudContainerInstanceFixture _fixture;
        private GoogleCloudStorage<object> _storage;
        private string _bucketName;
        private const string ProjectId = "test-project";

        private Mock<ILogger<GoogleCloudStorage<object>>> _loggerMock;

        public WriteQueueIntegrationTests(GoogleCloudContainerInstanceFixture fixture)
        {
            _fixture = fixture;
        }

        public async Task InitializeAsync()
        {
            if (_fixture.Container.State != DotNet.Testcontainers.Containers.TestcontainersStates.Running)
            {
                await _fixture.Container.StartAsync();
            }
            
            var emulatorHost = _fixture.GetConnectionString().Replace("http://", "").Replace("https://", "").TrimEnd('/');
            // We still set env vars for other components that might rely on them, but we also pass config explicitly
            Environment.SetEnvironmentVariable("STORAGE_EMULATOR_HOST", emulatorHost);
            Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", "");
            Environment.SetEnvironmentVariable("GCLOUD_PROJECT", ProjectId);

            _bucketName = $"test-bucket-{Guid.NewGuid()}";
            
            // Create bucket
            var clientBuilder = new StorageClientBuilder
            {
                BaseUri = $"http://{emulatorHost}/storage/v1/",
                UnauthenticatedAccess = true
            };
            var client = clientBuilder.Build();
            try 
            {
                await client.CreateBucketAsync(ProjectId, _bucketName);
            }
            catch (Google.GoogleApiException ex) when (ex.HttpStatusCode == System.Net.HttpStatusCode.Conflict)
            {
                // Ignore if exists
            }

            var config = Options.Create(new GoogleCloudConfig
            {
                BucketName = _bucketName,
                ProjectId = ProjectId,
                EmulatorHost = emulatorHost
            });

            _loggerMock = new Mock<ILogger<GoogleCloudStorage<object>>>();
            
            // Enable write queue!
            _storage = new GoogleCloudStorage<object>(_loggerMock.Object, config, retryInitialization: false, enableWriteQueue: true);
        }

        public async Task DisposeAsync()
        {
            if (_storage != null)
            {
                await _storage.DisposeAsync();
            }
            
            Environment.SetEnvironmentVariable("STORAGE_EMULATOR_HOST", null);
            Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", null);
            Environment.SetEnvironmentVariable("GCLOUD_PROJECT", null);
        }

        [Fact]
        public async Task Should_Flush_Queue_On_Dispose_After_Rapid_Writes()
        {
            var objectKey = "DeltaInfo";
            var content1 = Encoding.UTF8.GetBytes("Content 1");
            var content2 = Encoding.UTF8.GetBytes("Content 2");
            var content3 = Encoding.UTF8.GetBytes("Content 3 - Final");

            // Act
            // Write rapidly. The first might go through, subsequent ones should be queued.
            await _storage.UploadBytes(objectKey, content1);
            await _storage.UploadBytes(objectKey, content2);
            await _storage.UploadBytes(objectKey, content3);

            // Dispose should flush the queue
            await _storage.DisposeAsync();

            // Verify
            // Create a fresh client to read the data
            // Use the emulator host from the fixture directly to avoid env var race conditions
            var emulatorHost = _fixture.GetConnectionString().Replace("http://", "").Replace("https://", "").TrimEnd('/');
            var clientBuilder = new StorageClientBuilder
            {
                BaseUri = $"http://{emulatorHost}/storage/v1/",
                UnauthenticatedAccess = true
            };
            var client = clientBuilder.Build();

            using var stream = new System.IO.MemoryStream();
            await client.DownloadObjectAsync(_bucketName, objectKey, stream);
            var downloadedContent = stream.ToArray();

            downloadedContent.Should().BeEquivalentTo(content3);

            // Verify logs - either we waited for in-flight flush or we flushed explicitly
            _loggerMock.Verify(
                x => x.Log(
                    LogLevel.Information,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) =>
                        (v.ToString().Contains("Waiting for") && v.ToString().Contains("in-flight flush(es)")) ||
                        v.ToString().Contains($"Flushing remaining write for {objectKey}")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception, string>>()),
                Times.AtLeastOnce);

            // Verify that the write was actually flushed (logged by ExecuteFlush)
            _loggerMock.Verify(
                x => x.Log(
                    LogLevel.Debug,
                    It.IsAny<EventId>(),
                    It.Is<It.IsAnyType>((v, t) => v.ToString().Contains($"Flushed queued write for {objectKey}")),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception, string>>()),
                Times.AtLeastOnce);
        }
    }
}
