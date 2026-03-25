using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProvider.GoogleCloud;
using RxDatasets.StorageProviders;

namespace RxDatasets.StorageTests.SpecTests.WithCustomTestContainersImplementation;

public class HybridBigtableGcsStorageTests
{
    private static readonly IOptions<StorageConfig> StorageConfig = Options.Create(new StorageConfig());

    [Fact]
    public async Task Should_Store_Small_Deltas_Inline_And_Large_Data_In_Fallback_Blob_Storage()
    {
        var rowStore = new InMemoryVersionedArtifactRowStore(1024);
        var blobStorage = new InMemoryDiskStorage<object>();
        var legacyReader = new ObjectStorageReader<object>(Mock.Of<ILogger<ObjectStorageReader<object>>>(), StorageConfig, blobStorage);
        var hybridReader = new HybridBigtableStorageReader<object>(
            Mock.Of<ILogger<HybridBigtableStorageReader<object>>>(),
            StorageConfig,
            rowStore,
            blobStorage,
            legacyReader);

        var provider = new HybridBigtableStorageProvider<object>(
            Mock.Of<ILogger<HybridBigtableStorageProvider<object>>>(),
            StorageConfig,
            blobStorage,
            rowStore,
            new NotificationsHubSync<object>(),
            hybridReader,
            false);
        await provider.StartAsync(CancellationToken.None);

        var smallDeltaData = new byte[500];
        var largeDeltaData = new byte[2000];
        new Random(1).NextBytes(smallDeltaData);
        new Random(2).NextBytes(largeDeltaData);

        await provider.StoreDelta(1, [1, 2, 3], smallDeltaData);
        await provider.StoreDelta(2, [4, 5, 6], new MemoryStream(largeDeltaData));

        var datasetPrefix = DatasetHelper.GetObjectNamePrefix<object>(StorageConfig.Value);
        var firstDelta = await rowStore.GetArtifact(datasetPrefix, ObjectNameTypeEnum.Delta, 1);
        var secondDelta = await rowStore.GetArtifact(datasetPrefix, ObjectNameTypeEnum.Delta, 2);

        firstDelta.Should().NotBeNull();
        firstDelta!.InlineIndex.Should().Equal([1, 2, 3]);
        firstDelta.InlineData.Should().Equal(smallDeltaData);
        firstDelta.DataFallbackObjectKey.Should().BeNull();

        secondDelta.Should().NotBeNull();
        secondDelta!.InlineIndex.Should().Equal([4, 5, 6]);
        secondDelta.InlineData.Should().BeNull();
        secondDelta.DataFallbackObjectKey.Should().NotBeNull();

        var fallbackLength = await blobStorage.GetObjectContentLength(secondDelta.DataFallbackObjectKey!);
        fallbackLength.Should().Be(largeDeltaData.Length);

        var largeBytes = await hybridReader.ReadDataFromSources([(ObjectNameTypeEnum.Delta, 2, 0)], largeDeltaData.Length);
        largeBytes.ToArray().Should().BeEquivalentTo(largeDeltaData);
    }

    [Fact]
    public async Task Should_Store_Processed_Versions_In_Row_Store()
    {
        var rowStore = new InMemoryVersionedArtifactRowStore(1024);
        var blobStorage = new InMemoryDiskStorage<object>();
        var legacyReader = new ObjectStorageReader<object>(Mock.Of<ILogger<ObjectStorageReader<object>>>(), StorageConfig, blobStorage);
        var hybridReader = new HybridBigtableStorageReader<object>(
            Mock.Of<ILogger<HybridBigtableStorageReader<object>>>(),
            StorageConfig,
            rowStore,
            blobStorage,
            legacyReader);

        var provider = new HybridBigtableStorageProvider<object>(
            Mock.Of<ILogger<HybridBigtableStorageProvider<object>>>(),
            StorageConfig,
            blobStorage,
            rowStore,
            new NotificationsHubSync<object>(),
            hybridReader,
            false);
        await provider.StartAsync(CancellationToken.None);

        await provider.StoreProcessedVersion("consumer-a", 12);
        await provider.StoreProcessedVersion("consumer-b", 5);

        var datasetPrefix = DatasetHelper.GetObjectNamePrefix<object>(StorageConfig.Value);
        var processedVersions = await rowStore.GetProcessedVersions(datasetPrefix);

        processedVersions["consumer-a"].Should().Be(12);
        processedVersions["consumer-b"].Should().Be(5);
        (await provider.GetProcessedVersion("consumer-a")).Should().Be(12);
        (await provider.GetLowestProcessedVersion()).Should().Be(5);
    }

    private sealed class InMemoryVersionedArtifactRowStore(long maxInlinePayloadBytes) : IVersionedArtifactRowStore
    {
        private readonly Dictionary<(string DatasetPrefix, ObjectNameTypeEnum Type, ulong Version), HybridArtifactRecord> _artifacts = [];
        private readonly Dictionary<string, Dictionary<string, ulong>> _processedVersions = [];

        public ArtifactWritePlan CreateWritePlan(long indexLength, long dataLength)
        {
            return new ArtifactWritePlan(indexLength <= maxInlinePayloadBytes, dataLength <= maxInlinePayloadBytes);
        }

        public Task UpsertArtifact(string datasetPrefix, HybridArtifactRecord artifact, CancellationToken cancellationToken = default)
        {
            _artifacts[(datasetPrefix, artifact.Type, artifact.Version)] = artifact;
            return Task.CompletedTask;
        }

        public Task<HybridArtifactRecord?> GetArtifact(string datasetPrefix, ObjectNameTypeEnum type, ulong version, CancellationToken cancellationToken = default)
            => GetArtifact(datasetPrefix, type, version, ArtifactReadScope.All, cancellationToken);

        public Task<HybridArtifactRecord?> GetArtifact(string datasetPrefix, ObjectNameTypeEnum type, ulong version, ArtifactReadScope scope, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(_artifacts.TryGetValue((datasetPrefix, type, version), out var artifact) ? artifact : null);
        }

        public Task<ulong?> GetLatestArtifactVersion(string datasetPrefix, ObjectNameTypeEnum type, CancellationToken cancellationToken = default)
        {
            var latest = _artifacts
                .Where(kvp => kvp.Key.DatasetPrefix == datasetPrefix && kvp.Key.Type == type)
                .Select(kvp => (ulong?)kvp.Key.Version)
                .Max();
            return Task.FromResult(latest);
        }

        public Task<IReadOnlyList<HybridArtifactSummary>> ListArtifacts(string datasetPrefix, ObjectNameTypeEnum type, CancellationToken cancellationToken = default)
        {
            IReadOnlyList<HybridArtifactSummary> result = _artifacts
                .Where(kvp => kvp.Key.DatasetPrefix == datasetPrefix && kvp.Key.Type == type)
                .Select(kvp => new HybridArtifactSummary(kvp.Key.Version, kvp.Value.CreatedAt))
                .OrderBy(summary => summary.Version)
                .ToList();
            return Task.FromResult(result);
        }

        public IAsyncEnumerable<HybridArtifactRecord> GetArtifactRange(
            string datasetPrefix, ObjectNameTypeEnum type, ulong fromVersionExclusive, ulong toVersionInclusive,
            CancellationToken cancellationToken = default)
            => GetArtifactRange(datasetPrefix, type, fromVersionExclusive, toVersionInclusive, ArtifactReadScope.All, cancellationToken);

        public async IAsyncEnumerable<HybridArtifactRecord> GetArtifactRange(
            string datasetPrefix,
            ObjectNameTypeEnum type,
            ulong fromVersionExclusive,
            ulong toVersionInclusive,
            ArtifactReadScope scope,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var kvp in _artifacts
                .Where(kvp => kvp.Key.DatasetPrefix == datasetPrefix && kvp.Key.Type == type
                    && kvp.Key.Version > fromVersionExclusive && kvp.Key.Version <= toVersionInclusive)
                .OrderBy(kvp => kvp.Key.Version))
            {
                yield return kvp.Value;
            }
            await Task.CompletedTask;
        }

        public Task DeleteArtifact(string datasetPrefix, ObjectNameTypeEnum type, ulong version, CancellationToken cancellationToken = default)
        {
            _artifacts.Remove((datasetPrefix, type, version));
            return Task.CompletedTask;
        }

        public Task<IReadOnlyDictionary<string, ulong>> GetProcessedVersions(string datasetPrefix, CancellationToken cancellationToken = default)
        {
            IReadOnlyDictionary<string, ulong> result = _processedVersions.TryGetValue(datasetPrefix, out var processedVersions)
                ? new Dictionary<string, ulong>(processedVersions, StringComparer.Ordinal)
                : new Dictionary<string, ulong>(StringComparer.Ordinal);
            return Task.FromResult(result);
        }

        public Task PutProcessedVersion(string datasetPrefix, string who, ulong version, CancellationToken cancellationToken = default)
        {
            if (!_processedVersions.TryGetValue(datasetPrefix, out var processedVersions))
            {
                processedVersions = new Dictionary<string, ulong>(StringComparer.Ordinal);
                _processedVersions[datasetPrefix] = processedVersions;
            }

            processedVersions[who] = version;
            return Task.CompletedTask;
        }

        public Task<(string Token, DateTimeOffset Expiry)?> TryAcquireLock(string datasetPrefix, string lockName, TimeSpan duration, CancellationToken cancellationToken = default)
        {
            // InMemory implementation for tests
            return Task.FromResult<(string Token, DateTimeOffset Expiry)?>(("test-token", DateTimeOffset.UtcNow.Add(duration)));
        }

        public Task<bool> RenewLock(string datasetPrefix, string lockName, string lockToken, TimeSpan duration, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(true);
        }

        public Task ReleaseLock(string datasetPrefix, string lockName, string lockToken, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

        public Task RequestLock(string datasetPrefix, string requestLockName, string instanceId, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

        public Task<bool> ShouldYield(string datasetPrefix, string requestLockName, string instanceId, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(false);
        }

        public Task DeleteRequestLock(string datasetPrefix, string requestLockName, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class InMemoryDiskStorage<T> : IDiskStorage<T>
    {
        private readonly ConcurrentDictionary<string, byte[]> _objects = new(StringComparer.Ordinal);

        public Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey)
        {
            if (_objects.TryGetValue(objectKey, out var data))
            {
                return Task.FromResult<ReadOnlyMemory<byte>>(data);
            }

            throw new FileNotFoundException("File not found", objectKey);
        }

        public async Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey, long position, int length)
        {
            var data = await DownloadBytes(objectKey);
            return data.Slice((int)position, length);
        }

        public async Task DownloadWithCallBack(string objectKey, Action<Stream> callback)
        {
            var data = await DownloadBytes(objectKey);
            using var stream = new MemoryStream(data.ToArray(), writable: false);
            callback(stream);
        }

        public Task UploadBytes(string objectKey, byte[] bytes)
        {
            _objects[objectKey] = bytes.ToArray();
            return Task.CompletedTask;
        }

        public async Task UploadFileStream(string objectKey, Stream stream)
        {
            using var memoryStream = new MemoryStream();
            if (stream.CanSeek)
            {
                stream.Position = 0;
            }

            await stream.CopyToAsync(memoryStream);
            _objects[objectKey] = memoryStream.ToArray();
        }

        public Task DeleteFile(string objectKey)
        {
            _objects.TryRemove(objectKey, out _);
            return Task.CompletedTask;
        }

        public Task DeleteFolder(string prefix)
        {
            var normalizedPrefix = prefix.EndsWith('/') ? prefix : prefix + '/';
            foreach (var key in _objects.Keys.Where(key => key.StartsWith(normalizedPrefix, StringComparison.Ordinal)).ToList())
            {
                _objects.TryRemove(key, out _);
            }

            return Task.CompletedTask;
        }

        public Task<List<(string ObjectKey, DateTimeOffset DateTime)>> ListObjects(string prefix)
        {
            var result = _objects.Keys
                .Where(key => key.StartsWith(prefix, StringComparison.Ordinal))
                .Select(key => (key, DateTimeOffset.UtcNow))
                .ToList();
            return Task.FromResult(result);
        }

        public Task<long> GetObjectContentLength(string objectKey)
        {
            if (_objects.TryGetValue(objectKey, out var data))
            {
                return Task.FromResult((long)data.Length);
            }

            throw new FileNotFoundException("File not found", objectKey);
        }
    }
}
