using Microsoft.Extensions.Logging;
using RxDatasets.Abstractions;
using System.Collections.Concurrent;

namespace RxDatasets.StorageProvider.InMemory
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

        private void SetUnhealthy(Exception ex)
        {
            _lastResult = (DateTimeOffset.UtcNow, ex);
        }

        public async Task<ReadOnlyMemory<byte>> DownloadBytes(string key)
        {
            if (_storage.TryGetValue(key, out var entry))
            {
                return await Task.FromResult(entry.Data);
            }
            else
            {
                _logger.LogError($"Error downloading file: {key} not found in storage.");
                throw new FileNotFoundException($"File not found in memory storage: {key}");
            }
        }

        public async Task<ReadOnlyMemory<byte>> DownloadBytes(string key, long offset, int length)
        {
            if (_storage.TryGetValue(key, out var entry))
            {
                if (offset + length > entry.Data.Length)
                {
                    throw new EndOfStreamException($"Requested {length} bytes but only {entry.Data.Length - offset} bytes are available from offset.");
                }
                return await Task.FromResult(entry.Data.AsMemory((int)offset, length));
            }
            else
            {
                _logger.LogError($"Error downloading file with offset and length: {key} not found in storage.");
                throw new FileNotFoundException($"File not found in memory storage: {key}");
            }
        }

        public async Task DownloadWithCallBack(string key, Action<Stream> callback)
        {
            if (_storage.TryGetValue(key, out var entry))
            {
                using var memoryStream = new MemoryStream(entry.Data);
                callback(memoryStream);
                await Task.CompletedTask;
            }
            else
            {
                _logger.LogError($"Error downloading file with callback: {key} not found in storage.");
                throw new FileNotFoundException($"File not found in memory storage: {key}");
            }
        }

        public async Task UploadBytes(string key, byte[] data)
        {
            _storage[key] = (data, DateTimeOffset.UtcNow);
            await Task.CompletedTask;
        }

        public async Task UploadFileStream(string key, Stream stream)
        {
            using var memoryStream = new MemoryStream();
            await stream.CopyToAsync(memoryStream);
            _storage[key] = (memoryStream.ToArray(), DateTimeOffset.UtcNow);
            _logger.LogInformation($"Successfully uploaded file {key} to in-memory storage.");
        }

        public async Task DeleteFile(string key)
        {
            if (_storage.TryRemove(key, out _))
            {
                _logger.LogInformation($"Successfully deleted file {key} from in-memory storage.");
            }
            else
            {
                _logger.LogWarning($"File not found when trying to delete: {key}");
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
    }
}
