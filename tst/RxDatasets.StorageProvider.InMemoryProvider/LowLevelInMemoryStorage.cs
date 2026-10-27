using System.Collections.Concurrent;
using System.Security.Cryptography;
using Microsoft.Extensions.Logging;
using RxDatasets.Abstractions;

namespace RxDatasets.TestImplementations.InMemoryProvider
{
    public class LowLevelInMemoryStorage<T> : IConditionalDiskStorage<T>
    {
        private readonly ILogger<LowLevelInMemoryStorage<T>> _logger;
        private readonly ConcurrentDictionary<string, (byte[] Data, DateTimeOffset CreatedAt)> _storage;

        private (DateTimeOffset ObtainedAt, Exception? Exception) _lastResult;

        public LowLevelInMemoryStorage(ILogger<LowLevelInMemoryStorage<T>> logger)
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

        public Task<(ReadOnlyMemory<byte> Bytes, string Version)> DownloadBytesWithVersion(string objectKey)
        {
            if (!_storage.TryGetValue(objectKey, out var entry))
            {
                throw new FileNotFoundException($"File not found in memory storage: {objectKey}");
            }
            return Task.FromResult((new ReadOnlyMemory<byte>(entry.Data), Version(entry.Data)));
        }

        public Task<string?> TryUploadBytes(string objectKey, byte[] bytes, string? expectedVersion)
        {
            var newEntry = (bytes, DateTimeOffset.UtcNow);
            bool written = expectedVersion == null
                ? _storage.TryAdd(objectKey, newEntry)
                // TryUpdate compares the whole entry (array by reference), so this is a compare-and-swap on what we checked
                : _storage.TryGetValue(objectKey, out var current)
                  && Version(current.Data) == expectedVersion
                  && _storage.TryUpdate(objectKey, newEntry, current);
            return Task.FromResult(written ? Version(bytes) : null);
        }

        public async Task<bool> TryCreateFromStream(string objectKey, Stream stream)
        {
            using var memoryStream = new MemoryStream();
            await stream.CopyToAsync(memoryStream);
            return await TryUploadBytes(objectKey, memoryStream.ToArray(), null) != null;
        }

        private static string Version(byte[] data) => Convert.ToHexString(SHA256.HashData(data));

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

        public async Task DeleteFolder(string prefix)
        {
            _logger.LogInformation($"Deleting folder {prefix} from in-memory storage.");
            var normalizedPrefix = prefix.EndsWith('/') ? prefix : prefix + '/';
            var keysToDelete = _storage.Keys.Where(key => key.StartsWith(normalizedPrefix, StringComparison.Ordinal)).ToList();

            foreach (var key in keysToDelete)
            {
                _storage.TryRemove(key, out _);
                _logger.LogInformation($"Deleted file {key} from in-memory storage.");
            }

            if (keysToDelete.Count == 0)
            {
                _logger.LogWarning($"No files found with prefix {prefix} to delete.");
            }

            await Task.CompletedTask;
        }


        public async Task<List<(string ObjectKey, DateTimeOffset DateTime)>> ListObjects(string prefix)
        {
            var results = _storage
                .Where(kv => kv.Key.StartsWith(prefix, StringComparison.Ordinal))
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
            }
        }
    }
}
