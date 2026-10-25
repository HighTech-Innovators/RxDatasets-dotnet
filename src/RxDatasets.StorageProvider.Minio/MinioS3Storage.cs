using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Minio;
using Minio.DataModel;
using Minio.DataModel.Args;
using Minio.Exceptions;
using RxDatasets.Abstractions;

namespace RxDatasets.StorageProvider.Minio;

public class MinioS3Storage<T> : IConditionalDiskStorage<T>
{
    private readonly IMinioClient _s3Client;

    private readonly ILogger<MinioS3Storage<T>> _logger;
    private readonly IOptions<MinioS3Config> _s3Config;
    private bool _initialized;
    private (DateTimeOffset ObtainedAt, Exception? Exception) _lastResult;

    public MinioS3Storage(ILogger<MinioS3Storage<T>> logger, IOptions<MinioS3Config> s3Config, bool retryInitialization = true)
    {
        _logger = logger;
        _s3Config = s3Config;
        var minioClient = new MinioClient().WithCredentials(s3Config.Value.AccessKey, s3Config.Value.SecretKey);
        if (!string.IsNullOrWhiteSpace(s3Config.Value.AwsRegion))
        {
            var endpoint = $"{s3Config.Value.BucketName}.s3.{s3Config.Value.AwsRegion}.amazonaws.com";
            minioClient = minioClient
                .WithEndpoint(endpoint)
                .WithRegion(s3Config.Value.AwsRegion)
                .WithSSL();
            _logger.LogInformation(
                $"Region was provided for aws S3, hence, using the following as the endpoint: {endpoint}");
        }
        else
        {
            minioClient = minioClient.WithEndpoint(s3Config.Value.ServiceURL);
        }
        minioClient.Build();
        _s3Client = minioClient;
        if (retryInitialization)
            InitializationLoop();
        else
            Initialize();
    }

    public (DateTimeOffset ObtainedAt, Exception? Exception) LastResult => _lastResult;

    private void Initialize()
    {
        try
        {
            EnsureBucketExists(_s3Config.Value.BucketName).Wait();
            _initialized = true;
            SetHealthy();
        }
        catch (AggregateException ex)
        {
            SetUnhealthy(ex);
            _logger.LogError(ex, "Failed to connect to S3");
            if (ex.InnerException is MinioException minioEx && MinioExceptionExt.IsTransient(minioEx))
            {
                throw new InvalidOperationException($"S3 is unavailable", ex);
            }

            throw;
        }
        catch (Exception ex)
        {
            SetUnhealthy(ex);
            _logger.LogError("Could not init s3");
            _logger.LogError(ex.Message);
            throw;

        }
    }
    private async void InitializationLoop()
    {
        while (true)
        {
            try
            {
                Initialize();
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to initialize storage provider");
                SetUnhealthy(ex);
                await Task.Delay(5 * 1000);
            }
        }
    }

    /// <summary>
    /// Download a MemoryStream from S3, rewinded to 0.
    /// </summary>
    /// <param name="objectKey">The name of the file on S3</param>
    /// <returns>A MemoryStream, rewinded to position 0</returns>
    /// <exception cref="InvalidOperationException">In case no connection to S3 can be made</exception>
    /// <exception cref="FileNotFoundException">In case a file does not exist on S3 </exception>
    public async Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey)
    {
        await EnsureInitialized();

        using var resultStream = new MemoryStream();
        try
        {
            await _s3Client.GetObjectAsync(new GetObjectArgs()
                .WithBucket(_s3Config.Value.BucketName)
                .WithObject(objectKey)
                .WithCallbackStream(x => x.CopyTo(resultStream)));
            SetHealthy();

        }
        catch (MinioException ex) when (ex.IsTransient())
        {
            SetUnhealthy(ex);
            throw new InvalidOperationException("S3 is unavailable", ex);
        }
        catch (MinioException ex) when (ex is ObjectNotFoundException or InvalidObjectNameException)
        {
            if (ex is ObjectNotFoundException)
            {
                SetHealthy();
                throw new FileNotFoundException($"File is not found.", objectKey, ex);
            }

            SetUnhealthy(ex);
            throw ex switch
            {
                InvalidObjectNameException => new InvalidOperationException("Some S3 error happened but we don't know it, it's hidden in the response", ex),
                _ => ex
            };
        }

        resultStream.Position = 0;

        // Create a ReadOnlyMemory<byte> directly from the MemoryStream's buffer
        var readOnlyMemory = new ReadOnlyMemory<byte>(resultStream.GetBuffer(), 0, (int)resultStream.Length);
        return readOnlyMemory;
    }

    /// <summary>
    /// Download a MemoryStream from S3, rewinded to 0, based on a position and length.
    /// </summary>
    /// <param name="objectKey">The name of the file on S3</param>
    /// <param name="position">The positions where to start the download</param>
    /// <param name="length">The number of bytes to download</param>
    /// <returns>The bytes of the object from the position and for length</returns>
    /// <exception cref="InvalidOperationException">Exception in case no connection to S3 could be made</exception>
    /// <exception cref="FileNotFoundException">In case a file does not exist on S3 </exception>
    public async Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey, long position, int length)
    {
        await EnsureInitialized();

        var resultStream = new MemoryStream(length);
        try
        {
            await _s3Client.GetObjectAsync(new GetObjectArgs()
                .WithBucket(_s3Config.Value.BucketName)
                .WithObject(objectKey)
                .WithOffsetAndLength(position, length)
                .WithCallbackStream(x => x.CopyTo(resultStream)));
            SetHealthy();
        }
        catch (MinioException ex) when (ex.IsTransient())
        {
            _logger.LogError(ex, "Failed to download from S3");
            var invalidOperationException = new InvalidOperationException("S3 is unavailable", ex);
            SetUnhealthy(ex);
            throw invalidOperationException;
        }
        catch (ObjectNotFoundException ex)
        {
            var fileNotFoundException = new FileNotFoundException($"File is not found.", objectKey, ex);
            SetHealthy();
            throw fileNotFoundException;
        }
        catch (Exception ex)
        {
            SetUnhealthy(ex);
            throw;
        }

        resultStream.Position = 0;

        // Create a ReadOnlyMemory<byte> directly from the MemoryStream's buffer
        var readOnlyMemory = new ReadOnlyMemory<byte>(resultStream.GetBuffer(), 0, length);
        return readOnlyMemory;
    }

    /// <summary>
    /// Start a Stream of a file from S3.
    /// </summary>
    /// <param name="objectKey">The name of the file on S3</param>
    /// <param name="callback">The action to apply for the stream</param>
    /// <returns>Nothing</returns>
    /// <exception cref="InvalidOperationException">Exception in case no connection to S3 could be made</exception>
    /// <exception cref="FileNotFoundException">In case a file does not exist on S3 </exception>
    public async Task DownloadWithCallBack(string objectKey, Action<Stream> callback)
    {
        try
        {
            await EnsureInitialized();
            await _s3Client.GetObjectAsync(new GetObjectArgs()
               .WithBucket(_s3Config.Value.BucketName)
               .WithObject(objectKey)
               .WithCallbackStream(callback));
            SetHealthy();
        }
        catch (MinioException ex) when (ex.IsTransient())
        {
            _logger.LogError(ex, "Failed to download from S3");
            throw new InvalidOperationException($"S3 is unavailable", ex);
        }
        catch (ObjectNotFoundException ex)
        {
            throw new FileNotFoundException("File could not be found!", objectKey, ex);
        }
    }

    public async Task EnsureBucketExists(string folderName)
    {
        var bucketExists = await _s3Client.BucketExistsAsync(new BucketExistsArgs().WithBucket(folderName));

        if (!bucketExists)
        {
            _logger.LogInformation($"Bucket {folderName} does not exist, creating it");
            await _s3Client.MakeBucketAsync(new MakeBucketArgs().WithBucket(folderName));
        }

        _logger.LogInformation($"{folderName} bucket was found or created.");
    }

    public async Task UploadBytes(string objectKey, byte[] bytes)
    {
        try
        {
            await EnsureInitialized();
            await DeleteFile(objectKey);
            await _s3Client.PutObjectAsync(new PutObjectArgs()
                .WithBucket(_s3Config.Value.BucketName)
                .WithObject(objectKey)
                .WithStreamData(new MemoryStream(bytes))
                .WithObjectSize(bytes.Length));
            SetHealthy();
        }
        catch (MinioException ex) when (ex.IsTransient())
        {
            _logger.LogError(ex, "Failed to upload to S3");
            SetUnhealthy(ex);
            throw new InvalidOperationException($"S3 is unavailable", ex);
        }
    }

    public async Task DeleteFile(string objectKey)
    {
        try
        {
            await EnsureInitialized();
            await _s3Client.RemoveObjectAsync(new RemoveObjectArgs()
                .WithBucket(_s3Config.Value.BucketName)
                .WithObject(objectKey));
            SetHealthy();
        }
        catch (MinioException ex) when (ex.IsTransient())
        {
            _logger.LogError(ex, "Failed to delete from S3");
            SetUnhealthy(ex);
            throw new InvalidOperationException($"S3 is unavailable", ex);
        }
    }

    public async Task DeleteFolder(string prefix)
    {
        try
        {
            await EnsureInitialized();
            var normalizedPrefix = prefix.EndsWith('/') ? prefix : prefix + '/';

            var listArgs = new ListObjectsArgs()
                .WithBucket(_s3Config.Value.BucketName)
                .WithPrefix(normalizedPrefix);

            await foreach (var item in _s3Client.ListObjectsEnumAsync(listArgs))
            {
                await _s3Client.RemoveObjectAsync(new RemoveObjectArgs()
                    .WithBucket(_s3Config.Value.BucketName)
                    .WithObject(item.Key));
            }
            ;

            SetHealthy();
        }
        catch (MinioException ex) when (ex.IsTransient())
        {
            _logger.LogError(ex, "Failed to delete folder from S3");
            SetUnhealthy(ex);
            throw new InvalidOperationException("S3 is unavailable", ex);
        }
    }

    public async Task<(ReadOnlyMemory<byte> Bytes, string Version)> DownloadBytesWithVersion(string objectKey)
    {
        await EnsureInitialized();
        using var resultStream = new MemoryStream();
        try
        {
            ObjectStat stat = await _s3Client.GetObjectAsync(new GetObjectArgs()
                .WithBucket(_s3Config.Value.BucketName)
                .WithObject(objectKey)
                .WithCallbackStream(x => x.CopyTo(resultStream)));
            SetHealthy();
            return (resultStream.ToArray(), stat.ETag);
        }
        catch (ObjectNotFoundException ex)
        {
            SetHealthy();
            throw new FileNotFoundException("File is not found.", objectKey, ex);
        }
        catch (MinioException ex)
        {
            SetUnhealthy(ex);
            throw new InvalidOperationException("S3 is unavailable", ex);
        }
    }

    public Task<string?> TryUploadBytes(string objectKey, byte[] bytes, string? expectedVersion) =>
        TryUpload(objectKey, new MemoryStream(bytes), expectedVersion);

    public async Task<bool> TryCreateFromStream(string objectKey, Stream stream) =>
        await TryUpload(objectKey, stream, null) != null;

    private async Task<string?> TryUpload(string objectKey, Stream stream, string? expectedVersion)
    {
        await EnsureInitialized();
        try
        {
            var args = new PutObjectArgs()
                .WithBucket(_s3Config.Value.BucketName)
                .WithObject(objectKey)
                .WithStreamData(stream)
                .WithObjectSize(stream.Length - stream.Position);
            args = expectedVersion == null ? args.WithNotMatchETag("*") : args.WithMatchETag(expectedVersion);
            var response = await _s3Client.PutObjectAsync(args);
            SetHealthy();
            return response.Etag.Trim('"'); // quoted here, unquoted in the stat of a read
        }
        // If-Match on an absent object comes back as NoSuchKey; the SDK only maps that to a typed exception for reads
        catch (MinioException ex) when (ex is PreconditionFailedException or ObjectNotFoundException
                                        || ex.Response?.Code is "PreconditionFailed" or "NoSuchKey" or "ConditionalRequestConflict")
        {
            SetHealthy();
            return null;
        }
        catch (MinioException ex)
        {
            SetUnhealthy(ex);
            throw new InvalidOperationException("S3 is unavailable", ex);
        }
    }

    public async Task UploadFileStream(string objectKey, Stream stream)
    {
        stream.Position = 0;
        try
        {
            await EnsureInitialized();
            await _s3Client.PutObjectAsync(new PutObjectArgs()
                .WithBucket(_s3Config.Value.BucketName)
                .WithObject(objectKey)
                .WithStreamData(stream)
                .WithObjectSize(stream.Length));
            SetHealthy();
        }
        catch (MinioException ex) when (ex.IsTransient())
        {
            _logger.LogError(ex, "Failed to upload to S3");
            SetUnhealthy(ex);
            throw new InvalidOperationException($"S3 is unavailable", ex);
        }
    }

    public async Task<List<(string ObjectKey, DateTimeOffset DateTime)>> ListObjects(string prefix)
    {
        try
        {
            await EnsureInitialized();

            var asyncEnum = _s3Client.ListObjectsEnumAsync(new ListObjectsArgs()
                .WithBucket(_s3Config.Value.BucketName)
                .WithPrefix($"{prefix}")
                .WithRecursive(true));

            var result = await asyncEnum
                .Select(obj => (obj.Key, new DateTimeOffset(obj.LastModifiedDateTime!.Value.ToUniversalTime(), TimeSpan.Zero)))
                .OrderBy(x => x.Key)
                .ToListAsync();

            SetHealthy();
            return result;
        }
        catch (MinioException ex) when (ex.IsTransient())
        {
            _logger.LogError(ex, "Failed connection to get objects from S3");
            SetUnhealthy(ex);
            throw new InvalidOperationException($"S3 is unavailable", ex);
        }
    }

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

    private void SetHealthy()
    {
        _lastResult = new(DateTimeOffset.Now, null);
    }
    private void SetUnhealthy(Exception? exception)
    {
        _lastResult = new(DateTimeOffset.Now, exception);
    }

    public async Task<long> GetObjectContentLength(string objectKey)
    {
        await EnsureInitialized();
        try
        {
            // Get object metadata
            var statObjectArgs = new StatObjectArgs()
            .WithBucket(_s3Config.Value.BucketName)
                                    .WithObject(objectKey);

            ObjectStat objectStat = await _s3Client.StatObjectAsync(statObjectArgs);

            long size = objectStat.Size;

            SetHealthy();
            return size;

        }
        catch (MinioException ex) when (ex.IsTransient())
        {
            SetUnhealthy(ex);
            throw new InvalidOperationException("S3 is unavailable", ex);
        }
        catch (MinioException ex) when (ex is ObjectNotFoundException or InvalidObjectNameException)
        {
            if (ex is ObjectNotFoundException)
            {
                SetHealthy();
                throw new FileNotFoundException($"File is not found.", objectKey, ex);
            }

            SetUnhealthy(ex);
            throw ex switch
            {
                InvalidObjectNameException => new InvalidOperationException("Some S3 error happened but we don't know it, it's hidden in the response", ex),
                _ => ex
            };
        }
    }
}

internal static class MinioExceptionExt
{
    public static bool IsTransient(this MinioException ex) =>
    ex is InternalClientException
    || ex is ConnectionException
    || ex.InnerException is HttpRequestException
    || (ex.Message?.Contains("Unsuccessful response from server", StringComparison.OrdinalIgnoreCase) ?? false);
}
