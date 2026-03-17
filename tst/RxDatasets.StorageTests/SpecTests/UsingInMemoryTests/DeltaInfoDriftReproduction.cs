using FluentAssertions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Reactive.Testing;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProviders;
using RxDatasets.TestImplementations.InMemoryProvider;

namespace RxDatasets.StorageTests.SpecTests.UsingInMemoryTests;

/// <summary>
/// Regression tests for #72: DeltaInfo.LatestDeltaVersion drift bug.
///
/// Root cause: ObjectStorageProvider.StoreDelta used DeltaInfo.LatestDeltaVersion++
/// instead of DeltaInfo.LatestDeltaVersion = deltaVersion. When SaveDeltaInfo failed
/// transiently (e.g. GCS rate-limiting on the DeltaInfo.pb hotspot), the in-memory
/// DeltaInfo was already incremented. On retry, ++ would increment again, causing
/// permanent forward drift (+1 per transient failure).
///
/// Fix: Use DeltaInfo.LatestDeltaVersion = deltaVersion (idempotent assignment).
/// </summary>
public class DeltaInfoDriftReproduction
{
    private readonly IOptions<StorageConfig> _storageConfig = Options.Create(new StorageConfig());
    private readonly IOptions<SnapshotCreationConfig> _snapshotsOptions = Options.Create(new SnapshotCreationConfig { SnapshotInterval = 50 });

    /// <summary>
    /// Verifies that after a transient SaveDeltaInfo failure and retry,
    /// DeltaInfo.LatestDeltaVersion stays consistent with actual delta files.
    ///
    /// Before fix: DeltaInfo++ caused double-increment on retry → drift.
    /// After fix: DeltaInfo = deltaVersion is idempotent → no drift.
    /// </summary>
    [Fact]
    public async Task DeltaInfo_Stays_Consistent_After_Transient_SaveDeltaInfo_Failure()
    {
        // Arrange
        var innerStorage = new LowLevelInMemoryStorage<object>(Mock.Of<ILogger<LowLevelInMemoryStorage<object>>>());
        var faultyStorage = new FaultyInMemoryStorage<object>(innerStorage);

        INotificationsHub<object> notificationsHub = new NotificationsHubSync<object>();
        IStorageReader<object> readOnlyStorage = new ObjectStorageReader<object>(
            Mock.Of<ILogger<ObjectStorageReader<object>>>(), _storageConfig, faultyStorage);

        var storageProvider = new ObjectStorageProvider<object>(
            Mock.Of<ILogger<ObjectStorageProvider<object>>>(), _storageConfig, faultyStorage,
            notificationsHub, readOnlyStorage, false);

        await storageProvider.StartAsync(CancellationToken.None);

        var datasetWriter = new DatasetWriter<object>(
            notificationsHub, Mock.Of<ILogger<DatasetWriter<object>>>(),
            storageProvider, Options.Create(new WriterConfig()), false);

        // Act - write 5 deltas successfully
        for (int i = 1; i <= 5; i++)
        {
            await datasetWriter.Put($"entity_{i}", BitConverter.GetBytes(i));
        }

        // Verify: DeltaInfo should be 5, actual files up to 5
        var versionsBeforeFailure = await storageProvider.ListVersions();
        versionsBeforeFailure.NewestDelta.Should().Be(5);
        VerifyDeltaFilesExistUpTo(innerStorage, 5);

        // Now make SaveDeltaInfo fail on the next upload
        faultyStorage.FailNextDeltaInfoUpload();

        // Write delta 6 - should fail because SaveDeltaInfo will throw
        Func<Task> failingWrite = () => datasetWriter.Put("entity_6", BitConverter.GetBytes(6));
        await failingWrite.Should().ThrowAsync<Exception>();

        // In-memory DeltaInfo should be rolled back to 5 (not advanced to 6)
        var versionsAfterFailure = await storageProvider.ListVersions();
        versionsAfterFailure.NewestDelta.Should().Be(5,
            "DeltaInfo should be rolled back after SaveDeltaInfo failure");

        // Now retry - this should write delta 6 again (same version, since _index wasn't updated)
        var retryResult = await datasetWriter.Put("entity_6", BitConverter.GetBytes(6));

        // Assert: with the fix, DeltaInfo should NOT drift
        var versionsAfterRetry = await storageProvider.ListVersions();

        retryResult.NewDatasetVersion.Should().Be(6, "We should have written version 6");

        // With = deltaVersion (idempotent), DeltaInfo correctly shows 6
        versionsAfterRetry.NewestDelta.Should().Be(6,
            "DeltaInfo should match actual delta files after transient failure + retry");

        // No phantom version 7 should exist
        DeltaFileExists(innerStorage, 7).Should().BeFalse("No delta was written for version 7");

        // Delta files for version 6 DO exist
        DeltaFileExists(innerStorage, 6).Should().BeTrue("Delta 6 was written successfully on retry");
    }

    /// <summary>
    /// Verifies that multiple transient SaveDeltaInfo failures with retries
    /// do not cause cumulative DeltaInfo drift.
    ///
    /// Before fix: 5 transient failures caused DeltaInfo to drift ahead by 5.
    /// After fix: DeltaInfo stays at 100 regardless of transient failures.
    /// </summary>
    [Fact]
    public async Task DeltaInfo_Does_Not_Drift_With_Multiple_Transient_Failures()
    {
        // Arrange
        var innerStorage = new LowLevelInMemoryStorage<object>(Mock.Of<ILogger<LowLevelInMemoryStorage<object>>>());
        var faultyStorage = new FaultyInMemoryStorage<object>(innerStorage);

        INotificationsHub<object> notificationsHub = new NotificationsHubSync<object>();
        IStorageReader<object> readOnlyStorage = new ObjectStorageReader<object>(
            Mock.Of<ILogger<ObjectStorageReader<object>>>(), _storageConfig, faultyStorage);

        var storageProvider = new ObjectStorageProvider<object>(
            Mock.Of<ILogger<ObjectStorageProvider<object>>>(), _storageConfig, faultyStorage,
            notificationsHub, readOnlyStorage, false);

        await storageProvider.StartAsync(CancellationToken.None);

        var datasetWriter = new DatasetWriter<object>(
            notificationsHub, Mock.Of<ILogger<DatasetWriter<object>>>(),
            storageProvider, Options.Create(new WriterConfig()), false);

        // Act - write with periodic failures
        int transientFailures = 0;
        int totalSuccessfulWrites = 0;

        for (int i = 1; i <= 100; i++)
        {
            // Inject a failure every 20 writes
            if (i % 20 == 0)
            {
                faultyStorage.FailNextDeltaInfoUpload();

                try
                {
                    await datasetWriter.Put($"entity_{i}", BitConverter.GetBytes(i));
                }
                catch
                {
                    transientFailures++;
                    // Retry the same write
                    await datasetWriter.Put($"entity_{i}", BitConverter.GetBytes(i));
                }

                totalSuccessfulWrites++;
            }
            else
            {
                await datasetWriter.Put($"entity_{i}", BitConverter.GetBytes(i));
                totalSuccessfulWrites++;
            }
        }

        // Assert
        transientFailures.Should().Be(5, "We injected 5 transient failures");
        totalSuccessfulWrites.Should().Be(100, "All 100 writes should eventually succeed");

        var versions = await storageProvider.ListVersions();

        // With the fix, DeltaInfo should exactly match actual writes (no drift)
        versions.NewestDelta.Should().Be(100,
            "DeltaInfo should be 100 despite {0} transient SaveDeltaInfo failures", transientFailures);

        // No phantom versions should exist
        DeltaFileExists(innerStorage, 101).Should().BeFalse(
            "No delta should exist beyond the 100 successful writes");
    }

    /// <summary>
    /// Stress test with 3 concurrent writers and async NotificationsHub (production config).
    /// Uses simulated GCS latency. Even without injected failures, this test verifies
    /// that DeltaInfo stays consistent under concurrent load.
    /// </summary>
    [Fact]
    public async Task ConcurrentWriters_With_AsyncNotificationsHub_DeltaInfo_Stays_Consistent()
    {
        // Arrange - use async NotificationsHub like production
        var innerStorage = new LowLevelInMemoryStorage<object>(Mock.Of<ILogger<LowLevelInMemoryStorage<object>>>());
        var slowStorage = new SlowInMemoryStorage<object>(innerStorage, uploadDelay: TimeSpan.FromMilliseconds(5));

        INotificationsHub<object> notificationsHub = new NotificationsHub<object>();
        IStorageReader<object> readOnlyStorage = new ObjectStorageReader<object>(
            Mock.Of<ILogger<ObjectStorageReader<object>>>(), _storageConfig, slowStorage);

        var storageProvider = new ObjectStorageProvider<object>(
            Mock.Of<ILogger<ObjectStorageProvider<object>>>(), _storageConfig, slowStorage,
            notificationsHub, readOnlyStorage, false);

        var snapshotPlugin = new SnapshotCreationPlugin<object>(
            Mock.Of<ILogger<SnapshotCreationPlugin<object>>>(),
            storageProvider, _snapshotsOptions, new TestScheduler());

        var consumingService = new DatasetUpdateConsumingService<object, SnapshotCreationPlugin<object>>(
            Mock.Of<ILogger<DatasetUpdateConsumingService<object, SnapshotCreationPlugin<object>>>>(),
            notificationsHub, snapshotPlugin);

        await storageProvider.StartAsync(CancellationToken.None);
        await consumingService.StartAsync(CancellationToken.None);

        var datasetWriter = new DatasetWriter<object>(
            notificationsHub, Mock.Of<ILogger<DatasetWriter<object>>>(),
            storageProvider, Options.Create(new WriterConfig()), false);

        var datasetReader = new DatasetReader<object>(
            Mock.Of<ILogger<DatasetReader<object>>>(), storageProvider, notificationsHub);
        await datasetReader.StartAsync(CancellationToken.None);

        const int writesPerWriter = 50;
        const int numWriters = 3;

        // Act - 3 concurrent writers
        var writerTasks = Enumerable.Range(0, numWriters).Select(writerId =>
            Task.Run(async () =>
            {
                for (int i = 0; i < writesPerWriter; i++)
                {
                    var entityName = $"writer{writerId}/entity_{i}";
                    await datasetWriter.Put(entityName, BitConverter.GetBytes(writerId * 1000 + i));
                }
            })).ToArray();

        await Task.WhenAll(writerTasks);

        // Allow snapshot creation to finish
        await Task.Delay(200);

        // Assert
        var versions = await storageProvider.ListVersions();
        ulong expectedVersion = numWriters * writesPerWriter;

        // The writer's index should match the total number of writes
        datasetReader.GetLatestVersion().Should().Be(expectedVersion,
            "All {0} writes should be reflected in the reader", expectedVersion);

        // DeltaInfo should exactly match the actual number of writes
        versions.NewestDelta.Should().Be(expectedVersion,
            "DeltaInfo.LatestDeltaVersion should equal the actual number of deltas written ({0})", expectedVersion);

        // Verify all delta files exist
        for (ulong v = 1; v <= expectedVersion; v++)
        {
            DeltaFileExists(innerStorage, v).Should().BeTrue(
                $"Delta file for version {v} should exist");
        }

        // Verify no phantom delta files exist beyond the expected version
        DeltaFileExists(innerStorage, expectedVersion + 1).Should().BeFalse(
            "No phantom delta should exist beyond version {0}", expectedVersion);
    }

    private bool DeltaFileExists(LowLevelInMemoryStorage<object> storage, ulong version)
    {
        var objectName = DatasetHelper.GetObjectName<object>(
            version, ObjectNameIndexOrDataEnum.Index, ObjectNameTypeEnum.Delta, _storageConfig.Value);
        try
        {
            storage.GetObjectContentLength(objectName).GetAwaiter().GetResult();
            return true;
        }
        catch (FileNotFoundException)
        {
            return false;
        }
    }

    private void VerifyDeltaFilesExistUpTo(LowLevelInMemoryStorage<object> storage, ulong version)
    {
        for (ulong v = 1; v <= version; v++)
        {
            DeltaFileExists(storage, v).Should().BeTrue($"Delta file for version {v} should exist");
        }
    }
}

/// <summary>
/// Wraps IDiskStorage to inject transient failures for DeltaInfo.pb uploads,
/// simulating a GCS transient error during SaveDeltaInfo.
/// </summary>
internal class FaultyInMemoryStorage<T> : IDiskStorage<T>
{
    private readonly IDiskStorage<T> _inner;
    private int _failDeltaInfoUploadCountdown;

    public FaultyInMemoryStorage(IDiskStorage<T> inner)
    {
        _inner = inner;
    }

    public void FailNextDeltaInfoUpload(int count = 1)
    {
        _failDeltaInfoUploadCountdown = count;
    }

    public async Task UploadBytes(string objectKey, byte[] bytes)
    {
        // Fail DeltaInfo.pb uploads when requested
        if (objectKey.EndsWith("DeltaInfo.pb", StringComparison.Ordinal)
            && Interlocked.Decrement(ref _failDeltaInfoUploadCountdown) >= 0)
        {
            throw new IOException("Simulated transient GCS failure during DeltaInfo.pb upload");
        }

        await _inner.UploadBytes(objectKey, bytes);
    }

    // All other methods delegate directly
    public Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey) => _inner.DownloadBytes(objectKey);
    public Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey, long position, int length) => _inner.DownloadBytes(objectKey, position, length);
    public Task DownloadWithCallBack(string objectKey, Action<Stream> callback) => _inner.DownloadWithCallBack(objectKey, callback);
    public Task UploadFileStream(string objectKey, Stream stream) => _inner.UploadFileStream(objectKey, stream);
    public Task DeleteFile(string objectKey) => _inner.DeleteFile(objectKey);
    public Task DeleteFolder(string prefix) => _inner.DeleteFolder(prefix);
    public Task<List<(string ObjectKey, DateTimeOffset DateTime)>> ListObjects(string prefix) => _inner.ListObjects(prefix);
    public Task<long> GetObjectContentLength(string objectKey) => _inner.GetObjectContentLength(objectKey);
}

/// <summary>
/// Wraps IDiskStorage to add configurable delays to upload operations,
/// simulating GCS latency (~100ms per upload in production).
/// </summary>
internal class SlowInMemoryStorage<T> : IDiskStorage<T>
{
    private readonly IDiskStorage<T> _inner;
    private readonly TimeSpan _uploadDelay;

    public SlowInMemoryStorage(IDiskStorage<T> inner, TimeSpan uploadDelay)
    {
        _inner = inner;
        _uploadDelay = uploadDelay;
    }

    public async Task UploadBytes(string objectKey, byte[] bytes)
    {
        await Task.Delay(_uploadDelay);
        await _inner.UploadBytes(objectKey, bytes);
    }

    public async Task UploadFileStream(string objectKey, Stream stream)
    {
        await Task.Delay(_uploadDelay);
        await _inner.UploadFileStream(objectKey, stream);
    }

    // Read operations don't need delays for this test
    public Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey) => _inner.DownloadBytes(objectKey);
    public Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey, long position, int length) => _inner.DownloadBytes(objectKey, position, length);
    public Task DownloadWithCallBack(string objectKey, Action<Stream> callback) => _inner.DownloadWithCallBack(objectKey, callback);
    public Task DeleteFile(string objectKey) => _inner.DeleteFile(objectKey);
    public Task DeleteFolder(string prefix) => _inner.DeleteFolder(prefix);
    public Task<List<(string ObjectKey, DateTimeOffset DateTime)>> ListObjects(string prefix) => _inner.ListObjects(prefix);
    public Task<long> GetObjectContentLength(string objectKey) => _inner.GetObjectContentLength(objectKey);
}
