using Azure;
using Azure.Core.Pipeline;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;

namespace RxDatasets.StorageProvider.AzBlob;

public class AzStorageProvider<T> : IObjectStorageProvider<T>
{
    private readonly BlobContainerClient _containerClient;
    private readonly ILogger<AzStorageProvider<T>> _logger;
    private bool _initialized;
    private (DateTimeOffset ObtainedAt, Exception? Exception) _lastResult;

    public AzStorageProvider(ILogger<AzStorageProvider<T>> logger, IOptions<AzStorageConfig> azStorageConfig, bool retryInitialization = true)
    {
        _logger = logger;
        _containerClient = new BlobServiceClient(
            azStorageConfig.Value.ConnectionString, 
            new BlobClientOptions
            {
                RetryPolicy = new RetryPolicy(2)
            }).GetBlobContainerClient(azStorageConfig.Value.ContainerName);
        if (retryInitialization) InitializationLoop(); else Initialize();
    }

    public (DateTimeOffset ObtainedAt, Exception? Exception) LastResult => _lastResult;

    private void Initialize()
    {
        try{
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
            throw new FileNotFoundException($"File is not found.", objectKey);
        }
        catch (Exception ex)
        {
            SetUnhealthy(ex);
            _logger.LogError(ex, "Failed to download from Azure Blob Storage");
            throw new InvalidOperationException("Azure Blob Storage is unavailable", ex);
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

            if (memoryStream.Length > int.MaxValue) throw new InvalidOperationException("File size is too large");

            // Create a ReadOnlyMemory<byte> directly from the MemoryStream's buffer to avoid copying data
            var readOnlyMemory = new ReadOnlyMemory<byte>(memoryStream.GetBuffer(), 0, (int)memoryStream.Length);
            SetHealthy();
            return readOnlyMemory;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            SetHealthy();
            throw new FileNotFoundException($"File is not found.", objectKey);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to download from Azure Blob Storage");
            SetUnhealthy(ex);
            throw new InvalidOperationException("Azure Blob Storage is unavailable", ex);
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
            throw new FileNotFoundException($"File is not found.", objectKey);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to download from Azure Blob Storage");
            SetUnhealthy(ex);
            throw new InvalidOperationException("Azure Blob Storage is unavailable", ex);
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
        catch (RequestFailedException ex) when (ex.Status==404)
        {
            SetHealthy();
            throw new FileNotFoundException($"File is not found.", objectKey);
        }
        catch (Exception ex)
        {
            SetUnhealthy(ex);
            _logger.LogError(ex, "Failed to download from Azure Blob Storage");
            throw new InvalidOperationException("Azure Blob Storage is unavailable", ex);
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
            throw new InvalidOperationException("Azure Blob Storage is unavailable", ex);
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
            throw new InvalidOperationException("Azure Blob Storage is unavailable", ex);
        }
    }

    public async Task<List<(string ObjectKey, DateTimeOffset DateTime)>> ListObjects(string prefix)
    {
        try
        {
            await EnsureInitialized();
            var pages = _containerClient.GetBlobsAsync(prefix: $"{prefix}").AsPages();
            var results = new List<(string Key, DateTimeOffset CreatedOn)>();

            await foreach (var page in pages)
            {
                results.AddRange(page.Values.Select(blobItem => (blobItem.Name,
                    new DateTimeOffset(blobItem.Properties.CreatedOn.GetValueOrDefault().UtcDateTime,
                        TimeSpan.Zero))));
            }
            SetHealthy();
            return results;
            
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
}