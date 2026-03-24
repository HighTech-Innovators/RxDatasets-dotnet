using System;
using System.IO;
using System.IO.Pipelines;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Google.Cloud.Storage.V1;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;

namespace RxDatasets.StorageProvider.GoogleCloud
{
    public class GoogleCloudStorage<T> : IDiskStorage<T>, IAsyncDisposable, IDisposable
    {
        private readonly StorageClient _storageClient;
        private readonly ILogger<GoogleCloudStorage<T>> _logger;
        private readonly string _bucketName;
        private readonly string? _projectId;
        private readonly WriteQueueManager _writeQueueManager;
        private readonly bool _enableWriteQueue;
        private readonly IClock _clock;
        private readonly string? _emulatorHost;
        private readonly Task _initializationTask;
        private bool _initialized;
        private volatile bool _disposed;
        private (DateTimeOffset ObtainedAt, Exception? Exception) _lastResult;

        public GoogleCloudStorage(ILogger<GoogleCloudStorage<T>> logger, IOptions<GoogleCloudConfig> googleCloudConfig, bool retryInitialization = true, bool? enableWriteQueue = null)
            : this(logger, googleCloudConfig, null, retryInitialization, enableWriteQueue)
        {
        }

        public GoogleCloudStorage(ILogger<GoogleCloudStorage<T>> logger, IOptions<GoogleCloudConfig> googleCloudConfig, IClock? clock, bool retryInitialization = true, bool? enableWriteQueue = null)
        {
            _logger = logger;
            _bucketName = googleCloudConfig.Value.BucketName;
            _projectId = googleCloudConfig.Value.ProjectId;
            _clock = clock ?? new SystemClock();
            _enableWriteQueue = enableWriteQueue ?? googleCloudConfig.Value.EnableWriteQueue;

            var clientBuilder = new StorageClientBuilder();

            // Check if we're running against an emulator
            _emulatorHost = googleCloudConfig.Value.EmulatorHost ?? Environment.GetEnvironmentVariable("STORAGE_EMULATOR_HOST");
            if (!string.IsNullOrEmpty(_emulatorHost))
            {
                // Configure for emulator use
                clientBuilder.BaseUri = $"http://{_emulatorHost}/storage/v1/";
                clientBuilder.UnauthenticatedAccess = true;
            }
            else if (!string.IsNullOrEmpty(googleCloudConfig.Value.JsonCredentials))
            {
                clientBuilder.Credential = Google.Apis.Auth.OAuth2.CredentialFactory.FromJson<Google.Apis.Auth.OAuth2.GoogleCredential>(googleCloudConfig.Value.JsonCredentials);
            }

            _storageClient = clientBuilder.Build();

            // Initialize write queue manager with flush action and clock
            _writeQueueManager = new WriteQueueManager(_logger, FlushQueuedWriteAsync, _clock);

            if (retryInitialization)
            {
                _initializationTask = InitializationLoop();
            }
            else
            {
                Initialize().GetAwaiter().GetResult();
                _initializationTask = Task.CompletedTask;
            }
        }

        public (DateTimeOffset ObtainedAt, Exception? Exception) LastResult => _lastResult;

        /// <summary>
        /// Executes a queued write by calling the appropriate upload method (bytes or stream).
        /// Used as the flush action delegate for WriteQueueManager.
        /// </summary>
        private async Task FlushQueuedWriteAsync(QueuedWrite queuedWrite)
        {
            // Bytes-based write with retry logic
            for (var i = 0; i < 5; i++)
            {
                try
                {
                    using var stream = new MemoryStream(queuedWrite.Bytes);
                    await _storageClient.UploadObjectAsync(_bucketName, queuedWrite.ObjectKey, null, stream);
                    _logger.LogInformation("Uploaded {ObjectKey} ({Length} bytes) to Google Cloud Storage (from queue)",
                        queuedWrite.ObjectKey, queuedWrite.Bytes.Length);
                    SetHealthy();
                    return;
                }
                catch (Exception ex) when (IsGcs429(ex))
                {
                    _logger.LogWarning(ex, "Rate limit hit for {objectKey} during queue flush. Retrying in 1.1s. Attempt {attempt}",
                        queuedWrite.ObjectKey, i + 1);
                    await _clock.Delay(TimeSpan.FromMilliseconds(1100));
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to upload bytes to {objectKey} in Google Cloud Storage from queue",
                        queuedWrite.ObjectKey);
                    SetUnhealthy(ex);
                    throw new InvalidOperationException(ex.Message, ex);
                }
            }

            var finalEx = new InvalidOperationException($"Failed to upload {queuedWrite.ObjectKey} after multiple retries due to rate limiting.");
            SetUnhealthy(finalEx);
            throw finalEx;
        }

        private async Task EnsureInitialized()
        {
            if (_initialized)
                return;

            for (var i = 0; i < 1000; i++)
            {
                if (_initializationTask.IsFaulted)
                {
                    await _initializationTask;
                }

                await _clock.Delay(TimeSpan.FromMilliseconds(500));
                if (_initialized)
                    return;
            }
            throw new InvalidOperationException("Google Cloud Storage not yet initialized");
        }

        private async Task Initialize()
        {
            try
            {
                await EnsureBucketExistsAsync();
                _initialized = true;
                _logger.LogInformation("Google Cloud bucket access verified");
                SetHealthy();
            }
            catch (Exception ex)
            {
                SetUnhealthy(ex);
                _logger.LogError(ex, "Could not init Google Cloud Storage");
                throw;
            }
        }

        private async Task InitializationLoop()
        {
            while (true)
            {
                try
                {
                    await Initialize();
                    break;
                }
                catch (Exception ex)
                {
                    SetUnhealthy(ex);
                    _logger.LogError(ex, "Failed to initialize storage provider, retrying...");
                    await _clock.Delay(TimeSpan.FromSeconds(5));
                }
            }
        }

        public async Task DeleteFile(string objectKey)
        {
            await EnsureInitialized();
            try
            {
                await _storageClient.DeleteObjectAsync(_bucketName, objectKey);
                SetHealthy();
            }
            catch (Google.GoogleApiException e) when (e.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                SetHealthy();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to delete file {objectKey} from Google Cloud Storage", objectKey);
                SetUnhealthy(ex);
                throw new InvalidOperationException("Google Cloud Storage is unavailable", ex);
            }
        }

        public async Task DeleteFolder(string prefix)
        {
            await EnsureInitialized();
            _logger.LogInformation("Deleting 'folder' {folderName}", prefix);
            try
            {
                // Normalize prefix to ensure it represents a folder path.
                // Google Cloud Storage uses simple prefix matching, so "deltas/2" would also match "deltas/20".
                // Ensure a trailing slash so we only match objects directly under the intended folder.
                var normalizedPrefix = prefix.EndsWith('/') ? prefix : prefix + '/';
                var objects = _storageClient.ListObjectsAsync(_bucketName, normalizedPrefix);
                await foreach (var obj in objects)
                {
                    _logger.LogDebug("Deleting object {objectName}", obj.Name);
                    await _storageClient.DeleteObjectAsync(obj);
                }
                SetHealthy();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to delete folder {prefix} from Google Cloud Storage", prefix);
                SetUnhealthy(ex);
                throw new InvalidOperationException("Google Cloud Storage is unavailable", ex);
            }
        }

        private async Task EnsureBucketExistsAsync()
        {
            try
            {
                await _storageClient.GetBucketAsync(_bucketName);
                SetHealthy();
            }
            catch (Google.GoogleApiException e) when (e.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                try
                {
                    var projectId = _projectId ?? Environment.GetEnvironmentVariable("GOOGLE_PROJECT_ID");
                    if (string.IsNullOrEmpty(projectId))
                    {
                        _logger.LogError("GOOGLE_PROJECT_ID environment variable not set and ProjectId not in config. Cannot create bucket.");
                        throw new InvalidOperationException("GOOGLE_PROJECT_ID environment variable not set and ProjectId not in config. Cannot create bucket.");
                    }
                    await _storageClient.CreateBucketAsync(projectId, new Google.Apis.Storage.v1.Data.Bucket { Name = _bucketName, Location = "eu-north-1" });
                    SetHealthy();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to create bucket");
                    SetUnhealthy(ex);
                    throw;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to ensure bucket exists");
                SetUnhealthy(ex);
                throw;
            }
        }

        public async Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey)
        {
            await EnsureInitialized();
            try
            {
                using var stream = new MemoryStream();
                await _storageClient.DownloadObjectAsync(_bucketName, objectKey, stream);
                SetHealthy();
                return new ReadOnlyMemory<byte>(stream.ToArray());
            }
            catch (Google.GoogleApiException e) when (e.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                SetHealthy();
                throw new FileNotFoundException("File not found in Google Cloud Storage", objectKey, e);
            }
            catch (Exception ex)
            {
                SetUnhealthy(ex);
                throw;
            }
        }

        public async Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey, long position, int length)
        {
            await EnsureInitialized();
            try
            {
                var options = new DownloadObjectOptions
                {
                    Range = new RangeHeaderValue(position, position + length - 1),
                };
                if (!string.IsNullOrEmpty(_emulatorHost))
                {
                    // Configure for emulator use
                    options.DownloadValidationMode = DownloadValidationMode.Never;
                }
                using var stream = new MemoryStream();
                await _storageClient.DownloadObjectAsync(_bucketName, objectKey, stream, options);
                SetHealthy();
                return new ReadOnlyMemory<byte>(stream.ToArray());
            }
            catch (Google.GoogleApiException e) when (e.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                SetHealthy();
                throw new FileNotFoundException("File not found in Google Cloud Storage", objectKey, e);
            }
            catch (Exception ex)
            {
                SetUnhealthy(ex);
                throw;
            }
        }

        public async Task DownloadWithCallBack(string objectKey, Action<Stream> callback)
        {
            await EnsureInitialized();
            var pipe = new Pipe();

            async Task Produce()
            {
                try
                {
                    await _storageClient.DownloadObjectAsync(_bucketName, objectKey, pipe.Writer.AsStream(true));
                    await pipe.Writer.CompleteAsync();
                    SetHealthy();
                }
                catch (Google.GoogleApiException e) when (e.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    SetHealthy();
                    await pipe.Writer.CompleteAsync(new FileNotFoundException("File not found in Google Cloud Storage", objectKey, e));
                }
                catch (Exception e)
                {
                    SetUnhealthy(e);
                    await pipe.Writer.CompleteAsync(e);
                }
            }

            var producer = Produce();
            callback(pipe.Reader.AsStream(true));
            await producer;
        }

        public async Task<long> GetObjectContentLength(string objectKey)
        {
            await EnsureInitialized();
            try
            {
                var obj = await _storageClient.GetObjectAsync(_bucketName, objectKey);
                SetHealthy();
                return (long)obj.Size.GetValueOrDefault();
            }
            catch (Google.GoogleApiException e) when (e.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
            {
                SetHealthy();
                throw new FileNotFoundException("File not found in Google Cloud Storage", objectKey, e);
            }
            catch (Exception ex)
            {
                SetUnhealthy(ex);
                throw;
            }
        }

        public async Task<List<(string ObjectKey, DateTimeOffset DateTime)>> ListObjects(string prefix)
        {
            await EnsureInitialized();
            try
            {
                var objects = _storageClient.ListObjectsAsync(_bucketName, prefix);
                var result = new List<(string ObjectKey, DateTimeOffset DateTime)>();
                await foreach (var obj in objects)
                {
                    result.Add((obj.Name, obj.UpdatedDateTimeOffset.GetValueOrDefault()));
                }
                SetHealthy();
                return result;
            }
            catch (Exception ex)
            {
                SetUnhealthy(ex);
                throw;
            }
        }

        public async Task UploadBytes(string objectKey, byte[] bytes)
        {
            if (_disposed)
                throw new ObjectDisposedException(typeof(GoogleCloudStorage<T>).FullName);

            // Try to queue the write if it's within 2 seconds of the last write
            if (_enableWriteQueue && await _writeQueueManager.TryQueueWriteAsync(objectKey, bytes))
            {
                return;
            }

            await EnsureInitialized();
            for (var i = 0; i < 5; i++)
            {
                try
                {
                    using var stream = new MemoryStream(bytes);
                    await _storageClient.UploadObjectAsync(_bucketName, objectKey, null, stream);
                    _logger.LogInformation("Uploaded {ObjectKey} ({Length} bytes) to Google Cloud Storage", objectKey, bytes.Length);
                    
                    // Mark this write as successful for queue timing purposes
                    if (_enableWriteQueue)
                    {
                        await _writeQueueManager.MarkSuccessfulWriteAsync(objectKey);
                    }
                    
                    SetHealthy();
                    return;
                }
                catch (Exception ex) when (IsGcs429(ex))
                {
                    _logger.LogWarning(ex, "Rate limit hit for {objectKey}. Retrying in 1.1s. Attempt {attempt}", objectKey, i + 1);
                    await _clock.Delay(TimeSpan.FromMilliseconds(1100));
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to upload bytes to {objectKey} in Google Cloud Storage", objectKey);
                    SetUnhealthy(ex);
                    throw new InvalidOperationException(ex.Message, ex);
                }
            }

            var finalEx = new InvalidOperationException($"Failed to upload {objectKey} after multiple retries due to rate limiting.");
            SetUnhealthy(finalEx);
            throw finalEx;
        }

        public async Task UploadFileStream(string objectKey, Stream stream)
        {
            if (_disposed)
                throw new ObjectDisposedException(typeof(GoogleCloudStorage<T>).FullName);

            await EnsureInitialized();
            try
            {
                await _storageClient.UploadObjectAsync(_bucketName, objectKey, null, stream);
                long? length = null;
                if (stream.CanSeek)
                {
                    length = stream.Length;
                }
                _logger.LogInformation("Uploaded {ObjectKey}{LengthSuffix} to Google Cloud Storage",
                    objectKey,
                    length is null ? string.Empty : $" ({length.Value} bytes)");
                
                // Mark this write as successful for queue timing purposes
                if (_enableWriteQueue)
                {
                    await _writeQueueManager.MarkSuccessfulWriteAsync(objectKey);
                }
                
                SetHealthy();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to upload stream to {objectKey} in Google Cloud Storage", objectKey);
                SetUnhealthy(ex);
                throw new InvalidOperationException(ex.Message, ex);
            }
        }

        private static bool IsGcs429(Exception ex) =>
            ex is Google.GoogleApiException g &&
                ((int)g.HttpStatusCode == 429 || g.Error?.Code == 429 ||
                 g.Error?.Message?.Contains("rate limit for object mutation",
                                            StringComparison.OrdinalIgnoreCase) == true)
            || ex.InnerException is Google.GoogleApiException g2 && IsGcs429(g2)
            || ex is Grpc.Core.RpcException r &&
                (r.StatusCode is StatusCode.ResourceExhausted or StatusCode.Unavailable) &&
                (r.Status.Detail?.Contains("rate limit for object mutation",
                                           StringComparison.OrdinalIgnoreCase) == true);

        private void SetHealthy() => _lastResult = (_clock.UtcNow, null);

        private void SetUnhealthy(Exception ex) => _lastResult = (_clock.UtcNow, ex);

        /// <summary>
        /// Flushes all pending/queued writes to storage without disposing the provider.
        /// </summary>
        public async Task FlushAsync()
        {
            if (_enableWriteQueue)
            {
                await _writeQueueManager.FlushAsync();
            }
        }

        /// <summary>
        /// Asynchronously disposes the storage provider, flushing all queued writes before closing.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;

            _disposed = true;

            try
            {
                // Flush all queued writes before closing
                if (_enableWriteQueue)
                {
                    await _writeQueueManager.FlushAndDisposeAsync();
                }
            }
            catch (Exception ex)
        {
                _logger.LogWarning(ex, "Error during queue flush on disposal");
        }

            try
            {
                // Dispose the Google Cloud Storage client
                _storageClient?.Dispose();
            }
            catch (Exception ex)
        {
                _logger.LogWarning(ex, "Error disposing Google Cloud Storage client");
            }

            GC.SuppressFinalize(this);
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            try
            {
                // Try synchronous flush of queue (best effort)
                if (_enableWriteQueue)
                {
                    _writeQueueManager.FlushAndDisposeAsync().GetAwaiter().GetResult();
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error during queue flush on synchronous disposal");
            }

            try
            {
            _storageClient?.Dispose();
        }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error disposing Google Cloud Storage client");
            }

            GC.SuppressFinalize(this);
        }
    }
}
