using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
namespace RxDatasets.StorageProvider.LocalDisk
{
    public class LocalDiskStorageProvider<T> : IObjectStorageProvider<T>
    {
        private readonly string _rootFolder;
        private readonly ILogger<LocalDiskStorageProvider<T>> _logger;
        private bool _initialized;
        private (DateTimeOffset ObtainedAt, Exception? Exception) _lastResult;

        public LocalDiskStorageProvider(ILogger<LocalDiskStorageProvider<T>> logger, IOptions<LocalDiskStorageConfig> config)
        {
            _logger = logger;
            _rootFolder = config.Value.RootFolder;
            Initialize();
        }

        public (DateTimeOffset ObtainedAt, Exception? Exception) LastResult => _lastResult;

        private void Initialize()
        {
            try
            {
                if (!Directory.Exists(_rootFolder))
                {
                    Directory.CreateDirectory(_rootFolder);
                }
                _initialized = true;
                SetHealthy();
            }
            catch (Exception ex)
            {
                SetUnhealthy(ex);
                _logger.LogError($"Could not initialize local disk storage: {ex.Message}");
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

        private string GetFullPath(string key)
        {
            var fullPath = Path.Combine(_rootFolder, key);
            var directory = Path.GetDirectoryName(fullPath);

            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
            }

            return fullPath;
        }

        public async Task<ReadOnlyMemory<byte>> DownloadBytes(string key)
        {
            try
            {
                var filePath = GetFullPath(key);

                if (!File.Exists(filePath))
                {
                    throw new FileNotFoundException($"File not found: {filePath}");
                }

                return await File.ReadAllBytesAsync(filePath);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error downloading file: {key}");
                throw;
            }
        }

        public async Task<ReadOnlyMemory<byte>> DownloadBytes(string key, long offset, int length)
        {
            try
            {
                var filePath = GetFullPath(key);

                if (!File.Exists(filePath))
                {
                    throw new FileNotFoundException($"File not found: {filePath}");
                }

                await using var fileStream = File.OpenRead(filePath);
                fileStream.Seek(offset, SeekOrigin.Begin);

                var buffer = new byte[length];
                var readBytes = await fileStream.ReadAsync(buffer, 0, length);

                if (readBytes < length)
                {
                    throw new EndOfStreamException($"Requested {length} bytes but only {readBytes} bytes were read.");
                }

                return buffer;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error downloading file with offset and length: {key}");
                throw;
            }
        }

        public async Task DownloadWithCallBack(string key, Action<Stream> callback)
        {
            try
            {
                var filePath = GetFullPath(key);

                if (!File.Exists(filePath))
                {
                    throw new FileNotFoundException($"File not found: {filePath}");
                }

                await using var fileStream = File.OpenRead(filePath);
                callback(fileStream);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error downloading file with callback: {key}");
                throw;
            }
        }

        public async Task UploadBytes(string key, byte[] data)
        {
            try
            {
                var filePath = GetFullPath(key);
                await File.WriteAllBytesAsync(filePath, data);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error uploading bytes to file: {key}");
                throw;
            }
        }

        public async Task UploadFileStream(string key, Stream stream)
        {
            try
            {
                var filePath = GetFullPath(key);

                // Log the stream length if it's seekable, helps debug issues with empty streams.
                if (stream.CanSeek)
                {
                    _logger.LogInformation($"Uploading file {key} with length: {stream.Length} bytes.");
                }

                await using var fileStream = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None);

                // Copy the stream asynchronously to ensure all data is flushed properly.
                await stream.CopyToAsync(fileStream);

                // Ensure the stream is flushed to disk.
                await fileStream.FlushAsync();

                _logger.LogInformation($"Successfully uploaded file {key} to disk.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error uploading stream to file: {key}");
                throw;
            }
        }


        public async Task DeleteFile(string key)
        {
            try
            {
                var filePath = GetFullPath(key);

                if (File.Exists(filePath))
                {
                    File.Delete(filePath);
                }
                else
                {
                    _logger.LogWarning($"File not found when trying to delete: {key}");
                }

                await Task.CompletedTask;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error deleting file: {key}");
                throw;
            }
        }

        public async Task<List<(string ObjectKey, DateTimeOffset DateTime)>> ListObjects(string prefix)
        {
            try
            {
                var searchPattern = Path.GetFileName(prefix) + "*";
                var searchPath = Path.Combine(_rootFolder, Path.GetDirectoryName(prefix) ?? string.Empty);

                if (!Directory.Exists(searchPath))
                {
                    return new List<(string ObjectKey, DateTimeOffset DateTime)>();
                }

                var files = Directory.GetFiles(searchPath, searchPattern, SearchOption.AllDirectories)
                    .Select(path => (path.Substring(_rootFolder.Length).TrimStart(Path.DirectorySeparatorChar).Replace(Path.DirectorySeparatorChar, '/'), DateTimeOffset.UtcNow))
                    .ToList();

                return await Task.FromResult(files);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error listing objects with prefix: {prefix}");
                return new List<(string ObjectKey, DateTimeOffset DateTime)>();
            }
        }
    }

    public class LocalDiskStorageConfig
    {
        public string RootFolder { get; set; } = "dataset-localstorage";
    }
}
