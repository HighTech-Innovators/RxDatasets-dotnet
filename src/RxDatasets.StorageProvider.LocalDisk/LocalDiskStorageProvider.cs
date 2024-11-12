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
            catch (Exception ex) when ( ex is not FileNotFoundException)            
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
            catch (Exception ex) when ( ex is not FileNotFoundException)            
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
            catch (Exception ex) when ( ex is not FileNotFoundException)            {
                _logger.LogError(ex, $"Error downloading file with callback: {key}");
                throw;
            }
        }

        public async Task UploadBytes(string key, byte[] data)
        {
            var filePath = GetFullPath(key);
            var tempFilePath = Path.Combine(Path.GetDirectoryName(filePath), $".tmp_{Path.GetFileName(filePath)}");

            try
            {
                // Write to the temporary file with a `.tmp_` prefix
                await File.WriteAllBytesAsync(tempFilePath, data);

                // Atomically replace the original file if it exists, or move if it doesn’t
                if (File.Exists(filePath))
                {
                    File.Replace(tempFilePath, filePath, null);
                }
                else
                {
                    File.Move(tempFilePath, filePath);
                }

                _logger.LogDebug($"Successfully uploaded bytes to file: {key}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error uploading bytes to file: {key}");
                throw;
            }
            finally
            {
                // Ensure temporary file is deleted if it still exists
                if (File.Exists(tempFilePath))
                {
                    File.Delete(tempFilePath);
                }
            }
        }

        public async Task UploadFileStream(string key, Stream stream)
        {
            var filePath = GetFullPath(key);
            var tempFilePath = Path.Combine(Path.GetDirectoryName(filePath), $".tmp_{Path.GetFileName(filePath)}");

            try
            {
                if (stream.CanSeek)
                {
                    _logger.LogInformation($"Uploading file {key} with length: {stream.Length} bytes.");
                }

                // Write to a temporary file with a `.tmp_` prefix
                await using (var tempFileStream = new FileStream(tempFilePath, FileMode.Create, FileAccess.Write, FileShare.None))
                {
                    await stream.CopyToAsync(tempFileStream);
                    await tempFileStream.FlushAsync(); // Ensure all data is flushed
                }

                // Atomically replace the original file if it exists, or move if it doesn’t
                if (File.Exists(filePath))
                {
                    File.Replace(tempFilePath, filePath, null);
                }
                else
                {
                    File.Move(tempFilePath, filePath);
                }

                _logger.LogDebug($"Successfully uploaded file {key} to disk.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error uploading stream to file: {key}");
                throw;
            }
            finally
            {
                // Ensure temporary file is deleted if it still exists
                if (File.Exists(tempFilePath))
                {
                    File.Delete(tempFilePath);
                }
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

        public Task<long> GetObjectContentLength(string objectKey)
        {
            try
            {
                var filePath = GetFullPath(objectKey);

                if (!File.Exists(filePath))
                {
                    throw new FileNotFoundException($"File not found: {filePath}");
                }

               FileInfo fileInfo = new FileInfo(filePath);
               return Task.FromResult(fileInfo.Length);

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error downloading file: {objectKey}");
                throw;
            }
        }
    }

    public class LocalDiskStorageConfig
    {
        public string RootFolder { get; set; } = "dataset-localstorage";
    }
}
