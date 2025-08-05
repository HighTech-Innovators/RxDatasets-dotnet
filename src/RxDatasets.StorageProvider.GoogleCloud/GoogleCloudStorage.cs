using Google.Cloud.Storage.V1;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Google.Apis.Storage.v1.Data;
using System.Net.Http.Headers;
using System.IO.Pipelines;

namespace RxDatasets.StorageProvider.GoogleCloud
{
    public class GoogleCloudStorage<T> : IDiskStorage<T>, IDisposable
    {
        private readonly StorageClient _storageClient;
        private readonly ILogger<GoogleCloudStorage<T>> _logger;
        private readonly string _bucketName;
        private bool _initialized;
        private (DateTimeOffset ObtainedAt, Exception? Exception) _lastResult;

        public GoogleCloudStorage(ILogger<GoogleCloudStorage<T>> logger, IOptions<GoogleCloudConfig> googleCloudConfig, bool retryInitialization = true)
        {
            _logger = logger;
            _bucketName = googleCloudConfig.Value.BucketName;

            var clientBuilder = new StorageClientBuilder();
            if (!string.IsNullOrEmpty(googleCloudConfig.Value.JsonCredentials))
            {
                clientBuilder.JsonCredentials = googleCloudConfig.Value.JsonCredentials;
            }
            _storageClient = clientBuilder.Build();

            _ = retryInitialization ? InitializationLoop() : Initialize();
        }

        public (DateTimeOffset ObtainedAt, Exception? Exception) LastResult => _lastResult;

        private async Task EnsureInitialized()
        {
            if (_initialized)
                return;

            for (var i = 0; i < 1000; i++)
            {
                await Task.Delay(500);
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
                    await Task.Delay(5000);
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
            try
            {
                var objects = _storageClient.ListObjectsAsync(_bucketName, prefix);
                await foreach (var obj in objects)
                {
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
                    var projectId = Environment.GetEnvironmentVariable("GOOGLE_PROJECT_ID");
                    if(string.IsNullOrEmpty(projectId))
                    {
                        _logger.LogError("GOOGLE_PROJECT_ID environment variable not set. Cannot create bucket.");
                        throw new InvalidOperationException("GOOGLE_PROJECT_ID environment variable not set. Cannot create bucket.");
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
                    Range = new RangeHeaderValue(position, position + length - 1)
                };
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
            await EnsureInitialized();
            try
            {
                using var stream = new MemoryStream(bytes);
                await _storageClient.UploadObjectAsync(_bucketName, objectKey, null, stream);
                SetHealthy();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to upload bytes to {objectKey} in Google Cloud Storage", objectKey);
                SetUnhealthy(ex);
                throw;
            }
        }

        public async Task UploadFileStream(string objectKey, Stream stream)
        {
            await EnsureInitialized();
            try
            {
                await _storageClient.UploadObjectAsync(_bucketName, objectKey, null, stream);
                SetHealthy();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to upload stream to {objectKey} in Google Cloud Storage", objectKey);
                SetUnhealthy(ex);
                throw;
            }
        }

        private void SetHealthy()
        {
            _lastResult = (DateTimeOffset.UtcNow, null);
        }

        private void SetUnhealthy(Exception ex)
        {
            _lastResult = (DateTimeOffset.UtcNow, ex);
        }

        public void Dispose()
        {
            _storageClient?.Dispose();
        }
    }
}
