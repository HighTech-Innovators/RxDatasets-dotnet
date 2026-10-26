using System.IO;
using System.Security.Cryptography;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
namespace RxDatasets.StorageProvider.LocalDisk
{
    public class LocalDiskStorage<T> : IConditionalDiskStorage<T>
    {
        private readonly string _rootFolder;
        private readonly ILogger<LocalDiskStorage<T>> _logger;
        private (DateTimeOffset ObtainedAt, Exception? Exception) _lastResult;

        public LocalDiskStorage(ILogger<LocalDiskStorage<T>> logger, IOptions<LocalDiskStorageConfig> config)
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
                SetHealthy();
            }
            catch (Exception ex)
            {
                SetUnhealthy(ex);
                _logger.LogError(ex, "Could not initialize local disk storage");
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
                Directory.CreateDirectory(directory!);
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
            catch (Exception ex) when (ex is not FileNotFoundException)
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
            catch (Exception ex) when (ex is not FileNotFoundException)
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
            catch (Exception ex) when (ex is not FileNotFoundException)
            {
                _logger.LogError(ex, $"Error downloading file with callback: {key}");
                throw;
            }
        }

        public async Task UploadBytes(string key, byte[] data)
        {
            var filePath = GetFullPath(key);
            var tempFilePath = Path.Combine(Path.GetDirectoryName(filePath)!, $".tmp_{Path.GetFileName(filePath)}");

            try
            {
                // Write to the temporary file with a `.tmp_` prefix
                await File.WriteAllBytesAsync(tempFilePath, data);

                // Atomically replace the original file if it exists, or move if it doesn’t
                if (File.Exists(filePath))
                {
                    try
                    {
                        File.Replace(tempFilePath, filePath, null);
                    }
                    catch (UnauthorizedAccessException)
                    {
                        // Fallback: delete original and move temp file
                        File.Delete(filePath);
                        File.Move(tempFilePath, filePath);
                    }
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

        // The version is a hash of the content. Compare-and-swap holds an exclusive handle (FileShare.None, which .NET
        // maps to flock on Unix) across read, compare and rewrite, so it is atomic between processes on this machine.
        // ponytail: in-place rewrite, not crash-atomic; fine for small coordination files like the lock, not for data.
        public async Task<(ReadOnlyMemory<byte> Bytes, string Version)> DownloadBytesWithVersion(string key)
        {
            byte[] bytes = (await DownloadBytes(key)).ToArray();
            return (bytes, ContentVersion(bytes));
        }

        public async Task<string?> TryUploadBytes(string key, byte[] data, string? expectedVersion)
        {
            if (expectedVersion == null)
            {
                return await TryCreate(key, file => file.WriteAsync(data).AsTask()) ? ContentVersion(data) : null;
            }

            try
            {
                await using var fileStream = new FileStream(GetFullPath(key), FileMode.Open, FileAccess.ReadWrite, FileShare.None);
                var current = new byte[fileStream.Length];
                await fileStream.ReadExactlyAsync(current);
                if (ContentVersion(current) != expectedVersion)
                {
                    return null;
                }

                fileStream.SetLength(0);
                await fileStream.WriteAsync(data);
                return ContentVersion(data);
            }
            catch (FileNotFoundException)
            {
                return null; // deleted since it was read
            }
        }

        public Task<bool> TryCreateFromStream(string key, Stream stream) => TryCreate(key, stream.CopyToAsync);

        // Create-only writes a uniquely named temp file and moves it without overwriting, so a reader never sees a partly
        // written object and of concurrent creators exactly one wins.
        private async Task<bool> TryCreate(string key, Func<FileStream, Task> write)
        {
            var filePath = GetFullPath(key);
            var tempFilePath = Path.Combine(Path.GetDirectoryName(filePath)!, $".tmp_{Guid.NewGuid():N}_{Path.GetFileName(filePath)}");
            try
            {
                await using (var tempFileStream = new FileStream(tempFilePath, FileMode.CreateNew, FileAccess.Write, FileShare.None))
                {
                    await write(tempFileStream);
                }

                File.Move(tempFilePath, filePath, overwrite: false);
                return true;
            }
            catch (IOException) when (File.Exists(filePath))
            {
                return false;
            }
            finally
            {
                File.Delete(tempFilePath);
            }
        }

        private static string ContentVersion(byte[] bytes) => Convert.ToHexString(SHA256.HashData(bytes));

        public async Task UploadFileStream(string key, Stream stream)
        {
            var filePath = GetFullPath(key);
            var tempFilePath = Path.Combine(Path.GetDirectoryName(filePath)!, $".tmp_{Path.GetFileName(filePath)}");

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
                var filePath = Path.Combine(_rootFolder, key);

                if (File.Exists(filePath))
                {
                    File.Delete(filePath);
                    _logger.LogDebug($"Successfully deleted file {key} from disk.");
                }
                else
                {
                    _logger.LogWarning("File not found when trying to delete: {Key}", key);
                }

                await Task.CompletedTask;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error deleting file: {Key}", key);
                throw;
            }
        }

        public async Task DeleteFolder(string prefix)
        {
            var filePath = Path.Combine(_rootFolder, prefix);

            try
            {

                if (Directory.Exists(filePath))
                {
                    Directory.Delete(filePath, true);
                    _logger.LogDebug($"Successfully deleted folder {prefix} from disk.");
                }
                else
                {
                    _logger.LogWarning("Folder was not found when trying to delete: {FilePath}", filePath);
                }

                await Task.CompletedTask;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error deleting folder: {FilePath}", filePath);
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
                    .Select(path => (path.Substring(_rootFolder.Length).TrimStart(Path.DirectorySeparatorChar).Replace(Path.DirectorySeparatorChar, '/'), new FileInfo(path).LastWriteTimeUtc))
                    .Select(x => (x.Item1, new DateTimeOffset(x.Item2)))
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
