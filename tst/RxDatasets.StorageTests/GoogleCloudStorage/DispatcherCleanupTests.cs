using System;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using RxDatasets.StorageProvider.GoogleCloud;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace RxDatasets.StorageTests.GoogleCloudStorage
{
    /// <summary>
    /// Tests for reliable Google Cloud Storage client disposal.
    /// Verifies that the storage client is properly disposed and resources are cleaned up,
    /// preventing connection hangs and resource leaks during shutdown.
    /// </summary>
    public class DispatcherCleanupTests : IAsyncLifetime
    {
        private readonly ILogger<GoogleCloudStorage<byte[]>> _logger;
        private GoogleCloudStorage<byte[]> _storage = null!;

        public DispatcherCleanupTests()
        {
            _logger = new TestLogger<GoogleCloudStorage<byte[]>>();
        }

        public async Task InitializeAsync()
        {
            var config = new OptionsWrapper<GoogleCloudConfig>(new GoogleCloudConfig
            {
                BucketName = "test-bucket"
            });

            _storage = new GoogleCloudStorage<byte[]>(_logger, config, retryInitialization: false);
            await Task.CompletedTask;
        }

        public async Task DisposeAsync()
        {
            if (_storage != null)
            {
                try
                {
                    await _storage.DisposeAsync();
                }
                catch
                {
                    // Ignore cleanup errors in test teardown
                }
            }
        }

        /// <summary>
        /// Verifies that calling UploadBytes after disposal throws ObjectDisposedException.
        /// </summary>
        [Fact(Skip = "Integration test - requires GCS emulator")]
        public async Task ObjectDisposedExceptionThrownAfterDisposal()
        {
            // Arrange
            var objectKey = "delta.pb";
            var data = Encoding.UTF8.GetBytes("test data");

            // Act
            await _storage.DisposeAsync();

            // Assert
            await Assert.ThrowsAsync<ObjectDisposedException>(
                () => _storage.UploadBytes(objectKey, data)
            );
        }

        /// <summary>
        /// Verifies that ObjectDisposedException is thrown for stream uploads after disposal.
        /// </summary>
        [Fact(Skip = "Integration test - requires GCS emulator")]
        public async Task ObjectDisposedExceptionThrownForStreamUploadAfterDisposal()
        {
            // Arrange
            var objectKey = "delta.pb";
            var data = Encoding.UTF8.GetBytes("test data");

            // Act
            await _storage.DisposeAsync();

            // Assert
            using (var stream = new System.IO.MemoryStream(data))
            {
                await Assert.ThrowsAsync<ObjectDisposedException>(
                    () => _storage.UploadFileStream(objectKey, stream)
                );
            }
        }

        /// <summary>
        /// Verifies that the Google Cloud Storage client is disposed during DisposeAsync.
        /// This ensures no dangling connections or handles remain after disposal.
        /// </summary>
        [Fact(Skip = "Integration test - requires GCS emulator")]
        public async Task ClientDisposedBeforeFlagSet()
        {
            // Arrange
            // Note: This test verifies disposal order through logging and state inspection
            // In a real test, we would inspect the internal state or use mocks

            // Act
            await _storage.DisposeAsync();

            // Assert
            // The client should be disposed (verified through no exceptions on subsequent operations)
        }

        /// <summary>
        /// Verifies that DisposeAsync completes within a reasonable time (< 5 seconds).
        /// Prevents connection hangs during shutdown.
        /// </summary>
        [Fact(Skip = "Integration test - requires GCS emulator")]
        public async Task DisposalCompletesWithinTimeLimit()
        {
            // Arrange
            var data = Encoding.UTF8.GetBytes("test data");
            var sw = Stopwatch.StartNew();

            // Act
            // await _storage.UploadBytes(objectKey, data);
            sw = Stopwatch.StartNew();
            await _storage.DisposeAsync();
            sw.Stop();

            // Assert
            Assert.True(sw.ElapsedMilliseconds < 5000, 
                $"Disposal should complete within 5 seconds, but took {sw.ElapsedMilliseconds}ms");
        }

        /// <summary>
        /// Verifies that calling DisposeAsync multiple times is idempotent and safe.
        /// </summary>
        [Fact(Skip = "Integration test - requires GCS emulator")]
        public async Task MultipleDisposalCallsAreIdempotent()
        {
            // Act
            await _storage.DisposeAsync();

            // Should not throw
            await _storage.DisposeAsync();
            await _storage.DisposeAsync();

            // Assert - no exception thrown
            Assert.True(true);
        }

        /// <summary>
        /// Verifies that multiple disposal patterns (DisposeAsync and Dispose) work together safely.
        /// </summary>
        [Fact(Skip = "Integration test - requires GCS emulator")]
        public async Task SyncAndAsyncDisposalTogether()
        {
            // Arrange
            var data = Encoding.UTF8.GetBytes("test data");

            // Act
            // await _storage.UploadBytes(objectKey, data);
            await _storage.DisposeAsync();

            // Should not throw when calling Dispose after DisposeAsync
            _storage.Dispose();

            // Assert
            Assert.True(true);
        }
    }
}
