using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Minio;
using Minio.DataModel.Args;
using Minio.Exceptions;
using RxDatasets.Abstractions;

namespace RxDatasets.StorageProvider.Minio;

public class MinioS3StorageProvider<T> : IObjectStorageProvider<T>
{
    private readonly IMinioClient _s3Client;

    private readonly ILogger<MinioS3StorageProvider<T>> _logger;
    private readonly IOptions<MinioS3Config> _s3Config;

    public MinioS3StorageProvider(ILogger<MinioS3StorageProvider<T>> logger, IOptions<MinioS3Config> s3Config, bool retryInitialization = true)
    {
        _logger = logger;
        _s3Config = s3Config;
        var minioClient = new MinioClient().WithCredentials(s3Config.Value.AccessKey, s3Config.Value.SecretKey);
        if (!string.IsNullOrWhiteSpace(s3Config.Value.AwsRegion))
        {
            minioClient = minioClient.WithRegion(s3Config.Value.AwsRegion);
            var endpoint = $"s3.{s3Config.Value.AwsRegion}.amazonaws.com";
            minioClient = minioClient.WithEndpoint(endpoint);
            _logger.LogInformation($"Region was provided for aws S3, hence, using the following as the endpoint: {endpoint}");
        }
        else
        {
            minioClient = minioClient.WithEndpoint(s3Config.Value.ServiceURL);
        }
        minioClient.Build();
        _s3Client = minioClient;
        if (retryInitialization) InitializationLoop(); else Initialize();
    }
    private void Initialize()
    {
        try
        {
            EnsureBucketExists(_s3Config.Value.BucketName).Wait();
        }
        catch (AggregateException ex)
        {
            _logger.LogError(ex, "Failed to connect to S3");
            if (ex.InnerException is ConnectionException)
            {
                throw new InvalidOperationException($"S3 is unavailable", ex);
            }

            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError("Could not init s3");
            _logger.LogError(ex.Message);
            throw;
            
        }
    }
    private void InitializationLoop()
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
                Task.Delay(5 * 1000);
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
        using var resultStream = new MemoryStream();
        try
        {
            await _s3Client.GetObjectAsync(new GetObjectArgs()
                .WithBucket(_s3Config.Value.BucketName)
                .WithObject(objectKey)
                .WithCallbackStream(x => x.CopyTo(resultStream)));
        }
        catch (MinioException ex) when (ex is ConnectionException or ObjectNotFoundException or InvalidObjectNameException)
        {
            throw ex switch
            {
                ConnectionException => new InvalidOperationException("S3 is unavailable", ex),
                ObjectNotFoundException => new FileNotFoundException("File could not be found!", objectKey, ex),
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
        var resultStream = new MemoryStream(length);
        try
        {
            await _s3Client.GetObjectAsync(new GetObjectArgs()
                .WithBucket(_s3Config.Value.BucketName)
                .WithObject(objectKey)
                .WithOffsetAndLength(position, length)
                .WithCallbackStream(x => x.CopyTo(resultStream)));
        }
        catch (ConnectionException ex)
        {
            _logger.LogError(ex, "Failed to download from S3");
            throw new InvalidOperationException("S3 is unavailable", ex);
        }
        catch (ObjectNotFoundException ex)
        {
            throw new FileNotFoundException("File could not be found!", objectKey, ex);
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
            await _s3Client.GetObjectAsync(new GetObjectArgs()
               .WithBucket(_s3Config.Value.BucketName)
               .WithObject(objectKey)
               .WithCallbackStream(callback));
        }
        catch (ConnectionException ex)
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
            await DeleteFile(objectKey);
            await _s3Client.PutObjectAsync(new PutObjectArgs()
                .WithBucket(_s3Config.Value.BucketName)
                .WithObject(objectKey)
                .WithStreamData(new MemoryStream(bytes))
                .WithObjectSize(bytes.Length));
        }
        catch (ConnectionException ex)
        {
            _logger.LogError(ex, "Failed to upload to S3");
            throw new InvalidOperationException($"S3 is unavailable", ex);
        }
    }

    public async Task DeleteFile(string objectKey)
    {
        try
        {
            await _s3Client.RemoveObjectAsync(new RemoveObjectArgs()
                .WithBucket(_s3Config.Value.BucketName)
                .WithObject(objectKey));
        }
        catch (ConnectionException ex)
        {
            _logger.LogError(ex, "Failed to delete from S3");
            throw new InvalidOperationException($"S3 is unavailable", ex);
        }
    }


    public async Task UploadFileStream(string objectKey, FileStream stream)
    {
        stream.Position = 0;
        try
        {
            await _s3Client.PutObjectAsync(new PutObjectArgs()
                .WithBucket(_s3Config.Value.BucketName)
                .WithObject(objectKey)
                .WithStreamData(stream)
                .WithObjectSize(stream.Length));
        }
        catch (ConnectionException ex)
        {
            _logger.LogError(ex, "Failed to upload to S3");
            throw new InvalidOperationException($"S3 is unavailable", ex);
        }
    }

    public async Task<List<(string ObjectKey, DateTimeOffset DateTime)>> ListObjects(string prefix)
    {
        try
        {
            var observable = _s3Client.ListObjectsAsync(new ListObjectsArgs()
                .WithBucket(_s3Config.Value.BucketName)
                .WithPrefix($"{prefix}")
                .WithRecursive(true));

            return (await observable
                .Select(obj => (obj.Key, new DateTimeOffset(obj.LastModifiedDateTime!.Value.ToUniversalTime(), TimeSpan.Zero)))
                .ToList()
                .ToTask()
                )
                .OrderBy(x => x.Key)
                .ToList();
        }
        catch (ConnectionException ex)
        {
            _logger.LogError(ex, "Failed connection to get objects from S3");
            throw new InvalidOperationException($"S3 is unavailable", ex);
        }
    }
}
