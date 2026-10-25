using Azure;
using Azure.Core.Pipeline;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;

namespace RxDatasets.StorageProvider.AzBlob;

public class AzStorage<T> : IConditionalDiskStorage<T>
{
    private const string UnavailableMessage = "Azure Blob Storage is unavailable";
    private const string DownloadFailedMessage = "Failed to download from Azure Blob Storage";
    private const string File404Message = $"File is not found.";
    private readonly BlobContainerClient _containerClient;
    private readonly ILogger<AzStorage<T>> _logger;
    private bool _initialized;
    private (DateTimeOffset ObtainedAt, Exception? Exception) _lastResult;

    public AzStorage(ILogger<AzStorage<T>> logger, IOptions<AzStorageConfig> azStorageConfig, bool retryInitialization = true)
    {
        _logger = logger;
        var config = azStorageConfig.Value;
        var blobClientOptions = new BlobClientOptions
        {
            RetryPolicy = new RetryPolicy(2)
        };

        var blobServiceClient = string.IsNullOrEmpty(config.AccountUrl)
            ? new BlobServiceClient(config.ConnectionString, blobClientOptions)
            : new BlobServiceClient(new Uri(config.AccountUrl), CreateCredential(config), blobClientOptions);

        _containerClient = blobServiceClient.GetBlobContainerClient(config.ContainerName);
        if (retryInitialization)
            InitializationLoop();
        else
            Initialize();
    }

    private static DefaultAzureCredential CreateCredential(AzStorageConfig config)
    {
        var options = new DefaultAzureCredentialOptions();
        if (!string.IsNullOrEmpty(config.ManagedIdentityClientId))
            options.ManagedIdentityClientId = config.ManagedIdentityClientId;

        return new DefaultAzureCredential(options);
    }

    public (DateTimeOffset ObtainedAt, Exception? Exception) LastResult => _lastResult;

    private void Initialize()
    {
        try
        {
            EnsureBucketExistsAsync().Wait();
            _initialized = true;
            SetHealthy();
        }
        catch (AggregateException ex)
        {
            foreach (var innerException in ex.InnerExceptions)
            {
                SetUnhealthy(ex);
                _logger.LogError($"Could not init S3 {innerException.Message}");
                throw innerException;
            }
        }
        catch (Exception ex)
        {
            SetUnhealthy(ex);

            _logger.LogError($"Could not init S3 {ex.Message}");
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
                SetUnhealthy(ex);
                _logger.LogError(ex, "Failed to initialize storage provider");
                await Task.Delay(5 * 1000);
            }
        }
    }


    public async Task DeleteFile(string objectKey)
    {
        try
        {
            await EnsureInitialized();
            var blobClient = _containerClient.GetBlobClient(objectKey);
            await blobClient.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots);
            SetHealthy();
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            SetHealthy();
            throw new FileNotFoundException(File404Message, objectKey);
        }
        catch (Exception ex)
        {
            SetUnhealthy(ex);
            _logger.LogError(ex, DownloadFailedMessage);
            throw new InvalidOperationException(UnavailableMessage, ex);
        }
    }

    public async Task DeleteFolder(string prefix)
    {
        try
        {
            await EnsureInitialized();
            var normalizedPrefix = prefix.EndsWith('/') ? prefix : prefix + '/';
            var blobClients = _containerClient.GetBlobsAsync(prefix: normalizedPrefix);
            await foreach (var blobItem in blobClients)
            {
                // Delete each blob
                var blobClient = _containerClient.GetBlobClient(blobItem.Name);
                await blobClient.DeleteIfExistsAsync(snapshotsOption: DeleteSnapshotsOption.IncludeSnapshots);
            }
            SetHealthy();
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            SetHealthy();
            throw new FileNotFoundException(File404Message, prefix);
        }
        catch (Exception ex)
        {
            SetUnhealthy(ex);
            _logger.LogError(ex, UnavailableMessage);
            throw new InvalidOperationException(UnavailableMessage, ex);
        }
    }


    /// <summary>
    /// Download bytes from Azure Blob Storage (Max file size is 2gb)
    /// </summary>
    /// <param name="objectKey"></param>
    /// <returns></returns>
    /// <exception cref="FileNotFoundException"></exception>
    /// <exception cref="InvalidOperationException"></exception>
    public async Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey)
    {
        try
        {
            await EnsureInitialized();
            var blobClient = _containerClient.GetBlobClient(objectKey);

            // Use a pooled memory stream to reduce allocations and improve performance
            using var memoryStream = new MemoryStream();
            await blobClient.DownloadToAsync(memoryStream);
            memoryStream.Position = 0;

            if (memoryStream.Length > int.MaxValue)
                throw new InvalidOperationException("File size is too large");

            // Create a ReadOnlyMemory<byte> directly from the MemoryStream's buffer to avoid copying data
            var readOnlyMemory = new ReadOnlyMemory<byte>(memoryStream.GetBuffer(), 0, (int)memoryStream.Length);
            SetHealthy();
            return readOnlyMemory;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            SetHealthy();
            throw new FileNotFoundException(File404Message, objectKey);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, DownloadFailedMessage);
            SetUnhealthy(ex);
            throw new InvalidOperationException(UnavailableMessage, ex);
        }
    }

    public async Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey, long position, int length)
    {
        try
        {
            await EnsureInitialized();
            var blobClient = _containerClient.GetBlobClient(objectKey);

            // Set the range to download only the specified part of the blob
            HttpRange range = new HttpRange(position, length);

            // Start downloading the specified range
            var response = await blobClient.DownloadStreamingAsync(range, new BlobRequestConditions(), true, CancellationToken.None);

            // Use a MemoryStream with a pre-defined capacity to avoid resizing during the download
            using var memoryStream = new MemoryStream(length);
            await response.Value.Content.CopyToAsync(memoryStream);

            // Create a ReadOnlyMemory<byte> directly from the MemoryStream's buffer
            var readOnlyMemory = new ReadOnlyMemory<byte>(memoryStream.GetBuffer(), 0, length);
            SetHealthy();

            return readOnlyMemory;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            SetHealthy();
            throw new FileNotFoundException(File404Message, objectKey);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, DownloadFailedMessage);
            SetUnhealthy(ex);
            throw new InvalidOperationException(UnavailableMessage, ex);
        }
    }

    public async Task DownloadWithCallBack(string objectKey, Action<Stream> callback)
    {
        try
        {
            await EnsureInitialized();
            var blobClient = _containerClient.GetBlobClient(objectKey);

            var response = await blobClient.DownloadStreamingAsync();

            callback(response.Value.Content);
            SetHealthy();
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            SetHealthy();
            throw new FileNotFoundException(File404Message, objectKey);
        }
        catch (Exception ex)
        {
            SetUnhealthy(ex);
            _logger.LogError(ex, DownloadFailedMessage);
            throw new InvalidOperationException(UnavailableMessage, ex);
        }
    }
    private async Task EnsureBucketExistsAsync()
    {
        try
        {
            await _containerClient.CreateIfNotExistsAsync();
            SetHealthy();
        }
        catch (AggregateException ex)
        {
            _logger.LogError(ex, "Failed to connect to blob storage");
            throw new InvalidOperationException("Failed to connect to blob storage");
        }
        catch (Exception ex)
        {
            SetUnhealthy(ex);
            _logger.LogError(ex, "Failed to ensure container exists in Azure Blob Storage");
            throw;
        }
    }

    public async Task UploadBytes(string objectKey, byte[] bytes)
    {
        try
        {
            await EnsureInitialized();
            var blobClient = _containerClient.GetBlobClient(objectKey);
            await blobClient.UploadAsync(new MemoryStream(bytes), true);
            SetHealthy();
        }
        catch (Exception ex)
        {
            SetUnhealthy(ex);
            _logger.LogError(ex, "Failed to upload to Azure Blob Storage");
            throw new InvalidOperationException(UnavailableMessage, ex);
        }
    }

    public async Task<(ReadOnlyMemory<byte> Bytes, string Version)> DownloadBytesWithVersion(string objectKey)
    {
        try
        {
            await EnsureInitialized();
            var response = await _containerClient.GetBlobClient(objectKey).DownloadContentAsync();
            SetHealthy();
            return (response.Value.Content.ToMemory(), response.Value.Details.ETag.ToString());
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            SetHealthy();
            throw new FileNotFoundException(File404Message, objectKey);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, DownloadFailedMessage);
            SetUnhealthy(ex);
            throw new InvalidOperationException(UnavailableMessage, ex);
        }
    }

    public async Task<string?> TryUploadBytes(string objectKey, byte[] bytes, string? expectedVersion)
    {
        try
        {
            await EnsureInitialized();
            var conditions = expectedVersion == null
                ? new BlobRequestConditions { IfNoneMatch = ETag.All }
                : new BlobRequestConditions { IfMatch = new ETag(expectedVersion) };
            var response = await _containerClient.GetBlobClient(objectKey)
                .UploadAsync(new BinaryData(bytes), new BlobUploadOptions { Conditions = conditions });
            SetHealthy();
            return response.Value.ETag.ToString();
        }
        // 409 BlobAlreadyExists (If-None-Match: *), 412 ConditionNotMet (If-Match), 404 If-Match on a deleted blob
        catch (RequestFailedException ex) when (ex.Status is 404 or 409 or 412)
        {
            SetHealthy();
            return null;
        }
        catch (Exception ex)
        {
            SetUnhealthy(ex);
            _logger.LogError(ex, "Failed to upload to Azure Blob Storage");
            throw new InvalidOperationException(UnavailableMessage, ex);
        }
    }

    public async Task UploadFileStream(string objectKey, Stream stream)
    {
        try
        {
            await EnsureInitialized();
            stream.Position = 0;
            var blobClient = _containerClient.GetBlobClient(objectKey);
            await blobClient.UploadAsync(stream, true);
            SetHealthy();
        }
        catch (Exception ex)
        {
            SetUnhealthy(ex);
            _logger.LogError(ex, "Failed to upload to Azure Blob Storage");
            throw new InvalidOperationException(UnavailableMessage, ex);
        }
    }

    public async Task<List<(string ObjectKey, DateTimeOffset DateTime)>> ListObjects(string prefix)
    {
        try
        {
            await EnsureInitialized();
            var results = new List<(string ObjectKey, DateTimeOffset DateTime)>();

            // Fetch blobs with pagination handling
            var pages = _containerClient.GetBlobsAsync(prefix: prefix).AsPages(default, 5000); // Adjust PageSizeHint if needed

            await foreach (var page in pages)
            {
                results.AddRange(page.Values.Select(blobItem =>
                (
                    ObjectKey: blobItem.Name,
                    DateTime: blobItem.Properties.LastModified.HasValue
                        ? new DateTimeOffset(blobItem.Properties.LastModified.Value.UtcDateTime, TimeSpan.Zero)
                        : DateTimeOffset.MinValue // Handle cases where LastModified is null
                )));
            }

            SetHealthy();
            return results.OrderBy(x => x.ObjectKey).ToList();
        }
        catch (Exception ex)
        {
            SetUnhealthy(ex);
            _logger.LogError(ex, "Failed to list objects from Azure Blob Storage");
            throw new InvalidOperationException("Azure Blob Storage is unavailable", ex);
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
        throw new InvalidOperationException("Azure Blob Storage not yet initialized");
    }

    public async Task<long> GetObjectContentLength(string objectKey)
    {
        await EnsureInitialized();

        try
        {
            var blobClient = _containerClient.GetBlobClient(objectKey);
            var objectProperties = await blobClient.GetPropertiesAsync();
            SetHealthy();
            return objectProperties.Value.ContentLength;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            SetHealthy();
            throw new FileNotFoundException(File404Message, objectKey);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, DownloadFailedMessage);
            SetUnhealthy(ex);
            throw new InvalidOperationException(UnavailableMessage, ex);
        }
    }
}
