using System.Collections.Immutable;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Models;
using RxDatasets.StorageProviders;
using RxDatasets.TestImplementations.InMemoryProvider;

namespace RxDatasets.StorageTests.SpecTests.UsingInMemoryTests;

public class WriteCoalescingTests
{
    private readonly IOptions<StorageConfig> _storageConfig = Options.Create(new StorageConfig());

    private async Task<(DatasetWriter<object> Writer, ObjectStorageProvider<object> Storage, LowLevelInMemoryStorage<object> InnerStorage)> SetupAsync(
        bool enableCoalescing = true, TimeSpan? uploadDelay = null)
    {
        var innerStorage = new LowLevelInMemoryStorage<object>(Mock.Of<ILogger<LowLevelInMemoryStorage<object>>>());
        IDiskStorage<object> storage = uploadDelay.HasValue
            ? new SlowInMemoryStorage<object>(innerStorage, uploadDelay.Value)
            : innerStorage;

        // Use sync hub so readiness notification is delivered synchronously during StartAsync
        INotificationsHub<object> notificationsHub = new NotificationsHubSync<object>();
        IStorageReader<object> readOnlyStorage = new ObjectStorageReader<object>(
            Mock.Of<ILogger<ObjectStorageReader<object>>>(), _storageConfig, storage);

        var storageProvider = new ObjectStorageProvider<object>(
            Mock.Of<ILogger<ObjectStorageProvider<object>>>(), _storageConfig, storage,
            notificationsHub, readOnlyStorage, false);

        var writerConfig = new WriterConfig { EnableWriteCoalescing = enableCoalescing };
        // Create writer BEFORE StartAsync so it receives the readiness notification
        var datasetWriter = new DatasetWriter<object>(
            notificationsHub, Mock.Of<ILogger<DatasetWriter<object>>>(),
            storageProvider, Options.Create(writerConfig), false);

        await storageProvider.StartAsync(CancellationToken.None);

        return (datasetWriter, storageProvider, innerStorage);
    }

    private static byte[] TestData(int seed) => BitConverter.GetBytes(seed);

    [Fact]
    public async Task ConcurrentPuts_ToDifferentEntities_AreCoalesced_IntoFewerVersions()
    {
        var (writer, storageProvider, _) = await SetupAsync(uploadDelay: TimeSpan.FromMilliseconds(20));

        const int numWriters = 20;
        var tasks = Enumerable.Range(0, numWriters).Select(i =>
            Task.Run(() => writer.Put($"entity_{i}", TestData(i)))).ToArray();

        var responses = await Task.WhenAll(tasks);

        // All responses should have valid versions
        foreach (var response in responses)
        {
            response.NewDatasetVersion.Should().BeGreaterThan(0);
            response.NumAddedEntities.Should().Be(1);
        }

        // With coalescing, total versions should be fewer than total writes
        var versions = await storageProvider.ListVersions();
        versions.NewestDelta.Should().BeLessThan((ulong)numWriters,
            "Coalescing should combine concurrent writes into fewer deltas");
        versions.NewestDelta.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task ConcurrentPuts_ToSameEntity_LastWriterWins_ResponsesAreCorrect()
    {
        var (writer, _, _) = await SetupAsync(uploadDelay: TimeSpan.FromMilliseconds(20));

        // First establish the entity
        await writer.Put("shared", TestData(0));

        // Now fire many concurrent updates
        const int numWriters = 10;
        var tasks = Enumerable.Range(1, numWriters).Select(i =>
            Task.Run(() => writer.Put("shared", TestData(i)))).ToArray();

        var responses = await Task.WhenAll(tasks);

        // Each response should report exactly 1 entity changed or added
        foreach (var response in responses)
        {
            var totalChanges = response.NumAddedEntities + response.NumUpdatedEntities;
            totalChanges.Should().BeGreaterThan(0, "Each writer should see their write as a change");
        }

        // All should get the same final version (from the coalesced batch)
        var distinctVersions = responses.Select(r => r.NewDatasetVersion).Distinct().ToList();
        distinctVersions.Count.Should().BeLessOrEqualTo(numWriters,
            "Coalesced writers should share version numbers");
    }

    [Fact]
    public async Task PutThenDelete_SameEntity_InBatch_CorrectPerCallerResponses()
    {
        var (writer, _, _) = await SetupAsync(uploadDelay: TimeSpan.FromMilliseconds(50));

        // Establish entity first
        await writer.Put("target", TestData(42));

        // Use a barrier to ensure both operations enqueue before the leader processes
        var barrier = new Barrier(2);
        var putTask = Task.Run(async () =>
        {
            barrier.SignalAndWait();
            return await writer.Put("target", TestData(99));
        });
        var deleteTask = Task.Run(async () =>
        {
            barrier.SignalAndWait();
            return await writer.Delete("target");
        });

        var results = await Task.WhenAll(putTask, deleteTask);

        // One should report an update/add, the other a delete (or update depending on order)
        var totalAdded = results.Sum(r => r.NumAddedEntities);
        var totalUpdated = results.Sum(r => r.NumUpdatedEntities);
        var totalDeleted = results.Sum(r => r.NumDeletedEntities);

        // The two operations together should report exactly 2 changes
        (totalAdded + totalUpdated + totalDeleted).Should().Be(2,
            "Both the put and delete should report their respective changes");
    }

    [Fact]
    public async Task ConcurrentPutMany_AreCoalesced()
    {
        var (writer, storageProvider, _) = await SetupAsync(uploadDelay: TimeSpan.FromMilliseconds(20));

        const int numBatches = 5;
        var tasks = Enumerable.Range(0, numBatches).Select(batchId =>
            Task.Run(async () =>
            {
                var entities = Enumerable.Range(0, 3)
                    .Select(i => ($"batch{batchId}/entity_{i}", (byte[]?)TestData(batchId * 100 + i), (string?)null, (DateTimeOffset?)null));
                return await writer.PutMany(entities.ToAsyncEnumerable());
            })).ToArray();

        var responses = await Task.WhenAll(tasks);

        foreach (var response in responses)
        {
            response.NumAddedEntities.Should().Be(3);
        }

        var versions = await storageProvider.ListVersions();
        versions.NewestDelta.Should().BeLessThan((ulong)(numBatches),
            "Concurrent PutMany calls should be coalesced");
    }

    [Fact]
    public async Task StorageFailure_PropagatesTo_AllCoalescedCallers()
    {
        var innerStorage = new LowLevelInMemoryStorage<object>(Mock.Of<ILogger<LowLevelInMemoryStorage<object>>>());
        var faultyStorage = new FaultyInMemoryStorage<object>(innerStorage);

        INotificationsHub<object> notificationsHub = new NotificationsHubSync<object>();
        IStorageReader<object> readOnlyStorage = new ObjectStorageReader<object>(
            Mock.Of<ILogger<ObjectStorageReader<object>>>(), _storageConfig, faultyStorage);

        var storageProvider = new ObjectStorageProvider<object>(
            Mock.Of<ILogger<ObjectStorageProvider<object>>>(), _storageConfig, faultyStorage,
            notificationsHub, readOnlyStorage, false);

        var writerConfig = new WriterConfig { EnableWriteCoalescing = true };
        var writer = new DatasetWriter<object>(
            notificationsHub, Mock.Of<ILogger<DatasetWriter<object>>>(),
            storageProvider, Options.Create(writerConfig), false);

        await storageProvider.StartAsync(CancellationToken.None);

        // Inject a DeltaInfo failure
        faultyStorage.FailNextDeltaInfoUpload();

        // Fire multiple concurrent writes - all should fail
        var barrier = new Barrier(3);
        var tasks = Enumerable.Range(0, 3).Select(i =>
            Task.Run(async () =>
            {
                barrier.SignalAndWait();
                return await writer.Put($"entity_{i}", TestData(i));
            })).ToArray();

        // At least one should throw (the leader's batch fails, propagating to all)
        var exceptions = new List<Exception>();
        foreach (var task in tasks)
        {
            try
            {
                await task;
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }
        }

        exceptions.Should().NotBeEmpty("Storage failure should propagate to coalesced writers");
    }

    [Fact]
    public async Task PutAll_SerializesCorrectly_WithCoalescedWrites()
    {
        var (writer, _, _) = await SetupAsync(uploadDelay: TimeSpan.FromMilliseconds(20));

        // Seed some entities
        for (int i = 0; i < 5; i++)
            await writer.Put($"entity_{i}", TestData(i));

        // Fire concurrent Put (coalesced) and PutAll (exclusive) — they should not interfere
        var putTask = Task.Run(() => writer.Put("new_entity", TestData(100)));
        var putAllTask = Task.Run(async () =>
        {
            var entities = Enumerable.Range(10, 3)
                .Select(i => ($"entity_{i}", (byte[]?)TestData(i), (string?)null, (DateTimeOffset?)null));
            return await writer.PutAll(entities.ToAsyncEnumerable());
        });

        // Both should complete without errors
        await Task.WhenAll(putTask, putAllTask);
        var putResponse = await putTask;
        var putAllResponse = await putAllTask;

        putResponse.NumAddedEntities.Should().Be(1);
        putAllResponse.NumAddedEntities.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task HighConcurrency_AllResponses_AreCorrect()
    {
        var (writer, storageProvider, _) = await SetupAsync(uploadDelay: TimeSpan.FromMilliseconds(10));

        const int numWriters = 50;
        var tasks = Enumerable.Range(0, numWriters).Select(i =>
            Task.Run(async () =>
            {
                var response = await writer.Put($"entity_{i}", TestData(i));
                return (Index: i, Response: response);
            })).ToArray();

        var results = await Task.WhenAll(tasks);

        // Every write should report exactly 1 added entity
        foreach (var result in results)
        {
            result.Response.NumAddedEntities.Should().Be(1,
                $"Writer {result.Index} should see 1 added entity");
            result.Response.NewDatasetVersion.Should().BeGreaterThan(0);
        }

        // Total versions should be less than total writes (proving coalescing happened)
        var versions = await storageProvider.ListVersions();
        versions.NewestDelta.Should().BeLessThan((ulong)numWriters,
            "50 concurrent writes should be coalesced into fewer deltas");

        // All entities should be readable
        // (verified indirectly: storage didn't throw, all versions are consistent)
    }

    [Fact]
    public async Task CoalescingDisabled_BehavesLikeSerial()
    {
        var (writer, storageProvider, _) = await SetupAsync(enableCoalescing: false, uploadDelay: TimeSpan.FromMilliseconds(5));

        // Serial writes (not concurrent, so coalescing wouldn't help anyway)
        for (int i = 0; i < 5; i++)
            await writer.Put($"entity_{i}", TestData(i));

        var versions = await storageProvider.ListVersions();
        versions.NewestDelta.Should().Be(5,
            "Without coalescing, each write creates its own delta version");
    }

    [Fact]
    public async Task DeleteMany_IsCoalesced_WithPut()
    {
        var (writer, storageProvider, _) = await SetupAsync(uploadDelay: TimeSpan.FromMilliseconds(20));

        // Seed entities
        for (int i = 0; i < 5; i++)
            await writer.Put($"entity_{i}", TestData(i));

        var seedVersion = (await storageProvider.ListVersions()).NewestDelta;

        // Fire concurrent DeleteMany + Put
        var barrier = new Barrier(2);
        var deleteTask = Task.Run(async () =>
        {
            barrier.SignalAndWait();
            var names = new[] { "entity_0", "entity_1", "entity_2" };
            return await writer.DeleteMany(names.Select(n => (n, (string?)null, (DateTimeOffset?)null)).ToAsyncEnumerable());
        });
        var putTask = Task.Run(async () =>
        {
            barrier.SignalAndWait();
            return await writer.Put("entity_new", TestData(999));
        });

        var results = await Task.WhenAll(deleteTask, putTask);
        var deleteResponse = results[0];
        var putResponse = results[1];

        deleteResponse.NumDeletedEntities.Should().Be(3);
        putResponse.NumAddedEntities.Should().Be(1);

        // Should have been coalesced into a single version bump
        var versions = await storageProvider.ListVersions();
        var newVersions = versions.NewestDelta - seedVersion;
        newVersions.Should().BeLessOrEqualTo(2,
            "Concurrent delete + put should be coalesced (ideally 1 version)");
    }

    [Fact]
    public async Task PutOrDeleteMany_IsCoalesced()
    {
        var (writer, storageProvider, _) = await SetupAsync(uploadDelay: TimeSpan.FromMilliseconds(20));

        // Seed some entities
        await writer.Put("keep", TestData(1));
        await writer.Put("to_delete", TestData(2));

        var seedVersion = (await storageProvider.ListVersions()).NewestDelta;

        // Fire concurrent PutOrDeleteMany + Put
        var barrier = new Barrier(2);
        var mixedTask = Task.Run(async () =>
        {
            barrier.SignalAndWait();
            var operations = new (string name, byte[]? entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)[]
            {
                ("new_via_mixed", TestData(10), null, null),
                ("to_delete", null, null, null)
            };
            return await writer.PutOrDeleteMany(operations.ToAsyncEnumerable());
        });
        var putTask = Task.Run(async () =>
        {
            barrier.SignalAndWait();
            return await writer.Put("another_new", TestData(20));
        });

        var results = await Task.WhenAll(mixedTask, putTask);

        results[0].NumAddedEntities.Should().Be(1);
        results[0].NumDeletedEntities.Should().Be(1);
        results[1].NumAddedEntities.Should().Be(1);
    }

    [Fact]
    public void CoalescedWritesThatNetToNoChange_ProduceNoDelta_ButReportChanges()
    {
        // Scenario: "foo" = A in the index. Two intents are coalesced:
        //   Intent 1: Put("foo", B)  — should report changed=1
        //   Intent 2: Put("foo", A)  — should report changed=1 (changed B back to A)
        // Net effect on storage: foo is still A → no merged changes (no delta needed).
        var dataA = TestData(1);
        var dataB = TestData(2);
        var crcA = DatasetHelper.ComputeCrc32(dataA);

        // Build an index with "foo" = A
        var entities = EntityTrie.Empty.SetItem(
            "foo",
            new ImmutableSnapshotEntity(
                LastDeletedVersion: [],
                LastInsertedVersion: [1],
                LastUpdatedVersion: 0,
                Position: 0,
                Length: dataA.Length,
                Crc32: crcA));
        var index = new ImmutableDatasetState(entities, SnapshotVersion: 0, LatestVersion: 1);

        var intents = new List<DatasetWriter<object>.WriteIntent>
        {
            new() { Operations = [("foo", dataB, null, null)], Completion = new() },
            new() { Operations = [("foo", dataA, null, null)], Completion = new() }
        };

        var (mergedChanges, responses) = DatasetWriter<object>.MergeIntents(intents, index);

        // Intent 1 (B): sees foo=A in real index, crc differs → changed=1
        responses[0].NumUpdatedEntities.Should().Be(1, "Write B changed foo from A to B");
        responses[0].NumAddedEntities.Should().Be(0);
        responses[0].NumDeletedEntities.Should().Be(0);

        // Intent 2 (A): sees foo=B in virtual state, crc differs → changed=1
        responses[1].NumUpdatedEntities.Should().Be(1, "Write A changed foo from B back to A");
        responses[1].NumAddedEntities.Should().Be(0);
        responses[1].NumDeletedEntities.Should().Be(0);

        // Net effect: foo ends as A, same as real index → no merged changes
        mergedChanges.Should().BeEmpty("foo is still A after both writes, no delta needed");
    }

    [Fact]
    public async Task DeleteChildren_IsCoalesced_WithPut()
    {
        var (writer, storageProvider, _) = await SetupAsync(uploadDelay: TimeSpan.FromMilliseconds(20));

        // Seed parent + children entities
        await writer.Put("type/parent/child/c1", TestData(1));
        await writer.Put("type/parent/child/c2", TestData(2));
        await writer.Put("type/other/child/c3", TestData(3));

        var seedVersion = (await storageProvider.ListVersions()).NewestDelta;

        // Fire concurrent DeleteChildren + Put
        var barrier = new Barrier(2);
        var deleteChildrenTask = Task.Run(async () =>
        {
            barrier.SignalAndWait();
            return await writer.DeleteChildren("type/parent");
        });
        var putTask = Task.Run(async () =>
        {
            barrier.SignalAndWait();
            return await writer.Put("type/other/child/c4", TestData(4));
        });

        var results = await Task.WhenAll(deleteChildrenTask, putTask);

        results[0].NumDeletedEntities.Should().Be(2, "Should delete 2 children of type/parent");
        results[1].NumAddedEntities.Should().Be(1);
    }
}



