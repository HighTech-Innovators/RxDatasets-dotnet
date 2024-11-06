using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using RxDatasets.Abstractions;

namespace RxDatasets.TestImplementations.InMemoryProvider
{
    public class LowLevelInMemoryStorageProvider<T> : IObjectStorageProvider<T>
    {
        private readonly ILogger<LowLevelInMemoryStorageProvider<T>> _logger;
        private readonly ConcurrentDictionary<string, (byte[] Data, DateTimeOffset CreatedAt)> _storage;

        private (DateTimeOffset ObtainedAt, Exception? Exception) _lastResult;

        public LowLevelInMemoryStorageProvider(ILogger<LowLevelInMemoryStorageProvider<T>> logger)
        {
            _logger = logger;
            _storage = new ConcurrentDictionary<string, (byte[] Data, DateTimeOffset CreatedAt)>();
            SetHealthy();
        }

        public (DateTimeOffset ObtainedAt, Exception? Exception) LastResult => _lastResult;

        private void SetHealthy()
        {
            _lastResult = (DateTimeOffset.UtcNow, null);
        }

        public async Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey)
        {
            if (_storage.TryGetValue(objectKey, out var entry))
            {
                return await Task.FromResult(entry.Data);
            }
            else
            {
                _logger.LogError($"Error downloading file: {objectKey} not found in storage.");
                throw new FileNotFoundException($"File not found in memory storage: {objectKey}");
            }
        }

        public async Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey, long position, int length)
        {
            if (_storage.TryGetValue(objectKey, out var entry))
            {
                if (position + length > entry.Data.Length)
                {
                    throw new EndOfStreamException($"Requested {length} bytes but only {entry.Data.Length - position} bytes are available from offset.");
                }
                return await Task.FromResult(entry.Data.AsMemory((int)position, length));
            }
            else
            {
                _logger.LogError($"Error downloading file with offset and length: {objectKey} not found in storage.");
                throw new FileNotFoundException($"File not found in memory storage: {objectKey}");
            }
        }

        public async Task DownloadWithCallBack(string objectKey, Action<Stream> callback)
        {
            if (_storage.TryGetValue(objectKey, out var entry))
            {
                using var memoryStream = new MemoryStream(entry.Data);
                callback(memoryStream);
                await Task.CompletedTask;
            }
            else
            {
                _logger.LogError($"Error downloading file with callback: {objectKey} not found in storage.");
                throw new FileNotFoundException($"File not found in memory storage: {objectKey}");
            }
        }

        public async Task UploadBytes(string objectKey, byte[] bytes)
        {
            _storage[objectKey] = (bytes, DateTimeOffset.UtcNow);
            await Task.CompletedTask;
        }

        public async Task UploadFileStream(string objectKey, Stream stream)
        {
            using var memoryStream = new MemoryStream();
            await stream.CopyToAsync(memoryStream);
            _storage[objectKey] = (memoryStream.ToArray(), DateTimeOffset.UtcNow);
            _logger.LogInformation($"Successfully uploaded file {objectKey} to in-memory storage.");
        }

        public async Task DeleteFile(string objectKey)
        {
            if (_storage.TryRemove(objectKey, out _))
            {
                _logger.LogInformation($"Successfully deleted file {objectKey} from in-memory storage.");
            }
            else
            {
                _logger.LogWarning($"File not found when trying to delete: {objectKey}");
            }
            await Task.CompletedTask;
        }

        public async Task<List<(string ObjectKey, DateTimeOffset DateTime)>> ListObjects(string prefix)
        {
            var results = _storage
                .Where(kv => kv.Key.StartsWith(prefix))
                .Select(kv => (kv.Key, kv.Value.CreatedAt))
                .ToList();

            return await Task.FromResult(results);
        }

        public async Task<long> GetObjectContentLength(string objectKey)
        {
            if (_storage.TryGetValue(objectKey, out var entry))
            {
                return await Task.FromResult(entry.Data.Length);
            }
            else
            {
                _logger.LogError($"Error downloading file: {objectKey} not found in storage.");
                throw new FileNotFoundException($"File not found in memory storage: {objectKey}");
            }        }
    }
}
