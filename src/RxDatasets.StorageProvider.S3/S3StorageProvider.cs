using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using Amazon;

namespace RxDatasets.StorageProvider.S3
{
    public class S3StorageProvider<T> : IObjectStorageProvider<T>, IDisposable
    {
        private readonly AmazonS3Client _s3Client;
        private readonly TransferUtility _transferUtility;
        private readonly ILogger<S3StorageProvider<T>> _logger;
        private readonly string _bucketName;
        private bool _initialized;
        private (DateTimeOffset ObtainedAt, Exception? Exception) _lastResult;

        public S3StorageProvider(ILogger<S3StorageProvider<T>> logger, IOptions<S3Config> s3Config,
            bool retryInitialization = true)
        {
            _logger = logger;
            _bucketName = s3Config.Value.BucketName;
            AmazonS3Config config;

            if (string.IsNullOrWhiteSpace(s3Config.Value.Region))
            {
                config = new AmazonS3Config
                {
                    ServiceURL = s3Config.Value.ServiceUrl,
                    ForcePathStyle = true
                };
            }
            else
            {
                config = new AmazonS3Config
                {
                    RegionEndpoint = RegionEndpoint.GetBySystemName(s3Config.Value.Region)
                };
            }

            _s3Client = new AmazonS3Client(s3Config.Value.AccessKey, s3Config.Value.SecretKey, config);
            _transferUtility = new TransferUtility(_s3Client); // Initialize once

            _ = retryInitialization ? InitializationLoop() : Initialize();
        }

        public (DateTimeOffset ObtainedAt, Exception? Exception) LastResult => _lastResult;

        private async Task EnsureInitialized()
        {
            if (_initialized)
                return;

            for (var i = 0; i < 100; i++)
            {
                await Task.Delay(10);
                if (_initialized)
                    return;
            }
            throw new InvalidOperationException("S3 Storage not yet initialized");
        }

        private async Task Initialize()
        {
            try
            {
                await EnsureBucketExistsAsync();
                _initialized = true;
                _logger.LogInformation("S3 bucket access verified");
                SetHealthy();
            }
            catch (AggregateException ex)
            {
                SetUnhealthy(ex);
                foreach (var innerException in ex.InnerExceptions)
                {
                    if (innerException is InvalidOperationException)
                    {
                        throw innerException;
                    }
                }
            }
            catch (Exception ex)
            {
                SetUnhealthy(ex);
                _logger.LogError($"Could not init S3 {ex.Message}");
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
                var deleteObjectRequest = new DeleteObjectRequest
                {
                    BucketName = _bucketName,
                    Key = objectKey
                };

                await _s3Client.DeleteObjectAsync(deleteObjectRequest);
                SetHealthy();
            }
            catch (AmazonS3Exception s3e) when (s3e is { ErrorCode: "NotFound" })
            {
                SetHealthy();
                return;
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                SetHealthy();
                return;
            }
            catch (AmazonS3Exception s3e)
            {
                _logger.LogError("Failed to delete {Key} in S3, amazon error {ErrorCode}", objectKey, s3e.ErrorCode);
                SetUnhealthy(s3e);
                throw new InvalidOperationException($"Failed to delete object; Amazon error {s3e.ErrorCode}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to delete from S3");
                SetUnhealthy(ex);
                throw new InvalidOperationException("S3 is unavailable", ex);
            }
        }

        public async Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey)
        {
            await EnsureInitialized();
            try
            {
                var getObjectRequest = new GetObjectRequest
                {
                    BucketName = _bucketName,
                    Key = objectKey
                };

                using var response = await _s3Client.GetObjectAsync(getObjectRequest);
                using var memoryStream = new MemoryStream();
                await response.ResponseStream.CopyToAsync(memoryStream);
                memoryStream.Position = 0;

                if (memoryStream.Length > int.MaxValue)
                    throw new InvalidOperationException("File size is too large");

                var readOnlyMemory =
                    new ReadOnlyMemory<byte>(memoryStream.GetBuffer(), 0, (int)memoryStream.Length);
                SetHealthy();
                return readOnlyMemory;
            }
            catch (AmazonS3Exception s3e) when (s3e is { ErrorCode: "NotFound" })
            {
                _logger.LogError("File Does Not Exists, {Key}", objectKey);
                SetHealthy();
                throw new FileNotFoundException($"File is not found.", objectKey);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                _logger.LogError("File Does Not Exists, {Key}", objectKey);
                SetHealthy();
                throw new FileNotFoundException($"File is not found.", objectKey);
            }
            catch (AmazonS3Exception s3e)
            {
                _logger.LogError("Failed to read object {Key}, amazon error {ErrorCode}", objectKey, s3e.ErrorCode);
                var invalidOperationException = new InvalidOperationException($"Failed to read object; Amazon error {s3e.ErrorCode}");
                SetUnhealthy(invalidOperationException);
                throw invalidOperationException;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to download from S3");
                var invalidOperationException = new InvalidOperationException($"S3 is unavailable.", ex);
                SetUnhealthy(invalidOperationException);
                throw invalidOperationException;
            }
        }

        public async Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey, long position, int length)
        {
            await EnsureInitialized();
            try
            {
                var getObjectRequest = new GetObjectRequest
                {
                    BucketName = _bucketName,
                    Key = objectKey,
                    ByteRange = new ByteRange(position, position + length - 1)
                };

                using var response = await _s3Client.GetObjectAsync(getObjectRequest);
                using var memoryStream = new MemoryStream(length);
                await response.ResponseStream.CopyToAsync(memoryStream);

                var readOnlyMemory = new ReadOnlyMemory<byte>(memoryStream.GetBuffer(), 0, length);
                SetHealthy();
                return readOnlyMemory;
            }
            catch (AmazonS3Exception s3e) when (s3e is { ErrorCode: "NotFound" })
            {
                _logger.LogError("File Does Not Exists, {Key}", objectKey);
                SetHealthy();
                throw new FileNotFoundException($"File is not found.", objectKey);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                _logger.LogError("File Does Not Exists, {Key}", objectKey);
                SetHealthy();
                throw new FileNotFoundException($"File is not found.", objectKey);
            }
            catch (AmazonS3Exception s3e)
            {
                _logger.LogError("Failed to read object {Key}, amazon error {ErrorCode}", objectKey, s3e.ErrorCode);
                var invalidOperationException = new InvalidOperationException($"Failed to read object; Amazon error {s3e.ErrorCode}");
                SetUnhealthy(invalidOperationException);
                throw invalidOperationException;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to download from S3");
                var invalidOperationException = new InvalidOperationException($"S3 is unavailable.", ex);
                SetUnhealthy(invalidOperationException);
                throw invalidOperationException;
            }
        }

        public async Task DownloadWithCallBack(string objectKey, Action<Stream> callback)
        {
            await EnsureInitialized();
            try
            {
                var getObjectRequest = new GetObjectRequest
                {
                    BucketName = _bucketName,
                    Key = objectKey
                };

                using var response = await _s3Client.GetObjectAsync(getObjectRequest);
                await using var stream = response.ResponseStream;
                try
                {
                    callback(stream);
                    SetHealthy();

                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failure while streaming data to application");
                    throw;
                }
            }
            catch (AmazonS3Exception s3e) when (s3e is { ErrorCode: "NotFound" })
            {
                _logger.LogError("File Does Not Exists, {Key}", objectKey);
                SetHealthy();
                throw new FileNotFoundException($"File is not found.", objectKey);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                _logger.LogError("File Does Not Exists, {Key}", objectKey);
                SetHealthy();
                throw new FileNotFoundException($"File is not found.", objectKey);
            }
            catch (AmazonS3Exception s3e)
            {
                _logger.LogError("Failed to read object {Key}, amazon error {ErrorCode}", objectKey, s3e.ErrorCode);
                SetUnhealthy(s3e);
                throw new InvalidOperationException($"Failed to read object; Amazon error {s3e.ErrorCode}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to download from S3");
                SetUnhealthy(ex);
                throw new InvalidOperationException("S3 is unavailable", ex);
            }
        }

        private async Task EnsureBucketExistsAsync()
        {
            try
            {
                // TODO once AWS exposes it, use HeadBucketRequest
                var request = new ListObjectsV2Request
                {
                    BucketName = _bucketName,
                    Prefix = "no_need_to_exist",
                    MaxKeys = 1,
                };

                var response = await _s3Client.ListObjectsV2Async(request);
                SetHealthy();
                return;
            }
            catch (AmazonS3Exception s3e) when (s3e is { ErrorCode: "NoSuchBucket" })
            {
            }
            catch (AmazonS3Exception s3e)
            {
                _logger.LogError("Failed to check bucket exists in S3, amazon error {ErrorCode}", s3e.ErrorCode);
                SetUnhealthy(s3e);
                throw new InvalidOperationException($"Failed to check bucket exists in S3; Amazon error {s3e.ErrorCode}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to check bucket exists in S3");
                SetUnhealthy(ex);
                throw;
            }

            // Bucket does NOT exist, let's try to make it
            try
            {
                await _s3Client.PutBucketAsync(new PutBucketRequest { BucketName = _bucketName });
                SetHealthy();
            }
            catch (AmazonS3Exception s3e) when (s3e is { ErrorCode: "BucketAlreadyOwnedByYou" } or { ErrorCode: "BucketAlreadyExists" })
            {
                _logger.LogWarning("Strange - no such bucket but then it exists all of a sudden");
            }
            catch (AmazonS3Exception s3e)
            {
                _logger.LogError("Failed to create bucket in S3, amazon error {ErrorCode}", s3e.ErrorCode);
                SetUnhealthy(s3e);
                throw new InvalidOperationException($"Failed to create bucket in S3; Amazon error {s3e.ErrorCode}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to ensure bucket exists in S3");
                SetUnhealthy(ex);
                throw;
            }
        }

        public async Task UploadBytes(string objectKey, byte[] bytes)
        {
            await EnsureInitialized();
            try
            {
                using var memoryStream = new MemoryStream(bytes);
                var uploadRequest = new TransferUtilityUploadRequest
                {
                    InputStream = memoryStream,
                    Key = objectKey,
                    BucketName = _bucketName,
                    AutoCloseStream = true
                };

                await _transferUtility.UploadAsync(uploadRequest);
                SetHealthy();
            }
            catch (AmazonS3Exception s3e)
            {
                SetUnhealthy(s3e);
                _logger.LogError("Failed to write object {Key}, amazon error {ErrorCode}", objectKey, s3e.ErrorCode);
                throw new InvalidOperationException($"Failed to write object; Amazon error {s3e.ErrorCode}");
            }
            catch (Exception ex)
            {
                SetUnhealthy(ex);
                _logger.LogError(ex, "Failed to upload to S3");
                throw new InvalidOperationException("S3 is unavailable", ex);
            }
        }

        public async Task UploadFileStream(string objectKey, Stream stream)
        {
            await EnsureInitialized();
            try
            {
                var uploadRequest = new TransferUtilityUploadRequest
                {
                    InputStream = stream,
                    Key = objectKey,
                    BucketName = _bucketName,
                    AutoCloseStream = false
                };

                await _transferUtility.UploadAsync(uploadRequest);
                SetHealthy();
            }
            catch (AmazonS3Exception s3e)
            {
                _logger.LogError("Failed to write object {Key}, amazon error {ErrorCode}", objectKey, s3e.ErrorCode);
                SetUnhealthy(s3e);
                throw new InvalidOperationException($"Failed to write object; Amazon error {s3e.ErrorCode}");
            }
            catch (Exception ex)
            {
                SetUnhealthy(ex);
                _logger.LogError(ex, "Failed to upload to S3");
                throw new InvalidOperationException("S3 is unavailable", ex);
            }
        }

        public async Task<List<(string ObjectKey, DateTimeOffset DateTime)>> ListObjects(string prefix)
        {
            await EnsureInitialized();
            try
            {
                var request = new ListObjectsV2Request
                {
                    BucketName = _bucketName,
                    Prefix = prefix
                };

                var response = await _s3Client.ListObjectsV2Async(request);
                var results = response.S3Objects.Select(s3Object => (ObjectKey: s3Object.Key, DateTime: new DateTimeOffset(s3Object.LastModified))).ToList();
                SetHealthy();
                return results;
            }
            catch (AmazonS3Exception s3e)
            {
                _logger.LogError("Failed to list objects {prefix}, amazon error {ErrorCode}", prefix, s3e.ErrorCode);
                SetUnhealthy(s3e);
                throw new InvalidOperationException($"Failed to list object; Amazon error {s3e.ErrorCode}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to list objects {prefix}", prefix);
                SetUnhealthy(ex);
                throw new InvalidOperationException("S3 is unavailable", ex);
            }
        }

        private void SetHealthy()
        {
            _lastResult = new(DateTimeOffset.Now, null);
        }
        private void SetUnhealthy(Exception? exception)
        {
            _lastResult = new(DateTimeOffset.Now, exception);
        }
        void IDisposable.Dispose()
        {
            _transferUtility.Dispose();
            _s3Client.Dispose();
        }
    }
}
