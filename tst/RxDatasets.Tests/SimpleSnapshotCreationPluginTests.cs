using System.Reactive.Concurrency;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RxDatasets.BackgroundServices;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.Models;
using RxDatasets.StorageProviders;

namespace RxDatasets.Tests;

public class SimpleSnapshotCreationPluginTests
{
    private static IndexEntity MakeEntity(ulong version = 1, long position = 0, int length = 10, uint crc32 = 42)
        => new(version, position, length, crc32);

    /// <summary>
    /// Builds snapshot data bytes from entities: each entity is length-prefixed protobuf-style,
    /// matching what DatasetHelper.WriteProtobufLengthPrefix + raw data produces.
    /// Returns the data blob and a map of entity name → (position, length).
    /// </summary>
    private static (byte[] data, Dictionary<string, (long position, int length)> positions) BuildSnapshotData(
        params (string name, byte[] payload)[] entities)
    {
        using var ms = new MemoryStream();
        var positions = new Dictionary<string, (long position, int length)>();

        foreach (var (name, payload) in entities)
        {
            var prefixLen = DatasetHelper.WriteProtobufLengthPrefix(ms, payload.Length);
            var pos = ms.Position;
            positions[name] = (pos, payload.Length);
            ms.Write(payload);
        }

        return (ms.ToArray(), positions);
    }

    private static byte[] BuildSnapshotIndex(
        ulong version, string idPattern, Dictionary<string, (long position, int length)> positions,
        uint crc32 = 42)
    {
        var root = new SnapshotBucket();

        foreach (var (name, (position, length)) in positions)
        {
            var segments = name.Split('/');
            var current = root;
            for (int i = 0; i < segments.Length - 1; i++)
            {
                if (!current.Children.TryGetValue(segments[i], out var child))
                {
                    child = new SnapshotBucket();
                    current.Children[segments[i]] = child;
                }
                current = child;
            }

            var entity = new SnapshotEntity
            {
                Position = position,
                Length = length,
                Crc32 = crc32,
                LastUpdatedVersion = version,
            };
            entity.LastInsertedVersion.Add(version);
            current.Leaves[segments[^1]] = entity;
        }

        return StreamingIndexCodec.WriteSnapshot(version, idPattern, root);
    }

    private InMemoryStorageProvider<TestEntity> CreateStorageProvider()
    {
        var hub = Substitute.For<INotificationsHub<TestEntity>>();
        hub.Notify(Arg.Any<INotification>()).Returns(Task.CompletedTask);
        return new InMemoryStorageProvider<TestEntity>(
            NullLogger<InMemoryStorageProvider<TestEntity>>.Instance, hub);
    }

    private static IOptions<SnapshotCreationConfig> CreateConfig(ulong snapshotInterval = 2)
    {
        return Options.Create(new SnapshotCreationConfig
        {
            SnapshotInterval = snapshotInterval,
            PeriodInHours = 24,
            StartupDelaySeconds = 0,
        });
    }

    private class FakeIndexProvider<T> : ICurrentIndexProvider<T>
    {
        public ImmutableDatasetState? CurrentDatasetState { get; set; }
        public ImmutableDatasetState? MostRecentSnapshotState { get; set; }
    }

    /// <summary>Marker type for tests — never actually deserialized.</summary>
    public class TestEntity;

    [Fact]
    public async Task Creates_snapshot_from_borrowed_index_with_uncompressed_names()
    {
        // Arrange: store snapshot v1 with 3 entities
        var storage = CreateStorageProvider();
        var payload1 = "entity-one-data"u8.ToArray();
        var payload2 = "entity-two-data"u8.ToArray();
        var payload3 = "entity-three-data"u8.ToArray();

        var (snapshotData, positions) = BuildSnapshotData(
            ("stores/abc/items/001", payload1),
            ("stores/abc/items/002", payload2),
            ("stores/def/items/003", payload3));

        var snapshotIndex = BuildSnapshotIndex(1, "", positions);
        await storage.StoreSnapshot(1, snapshotIndex, snapshotData);

        // Store delta v2 and v3 so gap condition (interval=2) triggers
        await storage.StoreDelta(2, Array.Empty<byte>(), Array.Empty<byte>());
        await storage.StoreDelta(3, Array.Empty<byte>(), Array.Empty<byte>());

        // Build the ImmutableDatasetState the writer would have
        var entities = positions.ToDictionary(
            kvp => kvp.Key,
            kvp => MakeEntity(version: 1, position: kvp.Value.position, length: kvp.Value.length));
        var currentIndex = new ImmutableDatasetState(
            EntityTrie.FromDictionary(entities), SnapshotVersion: 1, LatestVersion: 3);

        var indexProvider = new FakeIndexProvider<TestEntity>
        {
            CurrentDatasetState = currentIndex,
            MostRecentSnapshotState = currentIndex,
        };

        var config = CreateConfig(snapshotInterval: 2);
        var plugin = new SimpleSnapshotCreationPlugin<TestEntity>(
            NullLogger<SimpleSnapshotCreationPlugin<TestEntity>>.Instance,
            storage, config, Scheduler.Immediate, indexProvider);

        // Act
        await plugin.HandleNewDeltas(CancellationToken.None);

        // Assert: new snapshot v3 should exist
        var (_, _, snapshots) = await storage.ListVersions();
        snapshots.Should().Contain(s => s.Version == 3);

        // Verify the new snapshot index is parseable and has the same entities
        var newIndexBytes = await storage.ReadSnapshotIndex(3);
        var newIndex = SnapshotIndex.Parser.ParseFrom(newIndexBytes.Span);
        newIndex.SnapshotVersion.Should().Be(3);

        var restored = ImmutableDatasetState.FromProto(newIndex);
        restored.Entities.Count.Should().Be(3);
        restored.Entities.ContainsKey("stores/abc/items/001").Should().BeTrue();
        restored.Entities.ContainsKey("stores/abc/items/002").Should().BeTrue();
        restored.Entities.ContainsKey("stores/def/items/003").Should().BeTrue();

        // Verify entity data is readable from new snapshot
        var entity1 = restored.Entities["stores/abc/items/001"];
        var data1 = await storage.ReadDataFromSources(
            [(ObjectNameTypeEnum.Snapshot, 3, entity1.Position)], entity1.Length);
        data1.ToArray().Should().BeEquivalentTo(payload1);

        var entity3 = restored.Entities["stores/def/items/003"];
        var data3 = await storage.ReadDataFromSources(
            [(ObjectNameTypeEnum.Snapshot, 3, entity3.Position)], entity3.Length);
        data3.ToArray().Should().BeEquivalentTo(payload3);
    }

    [Fact]
    public async Task Creates_snapshot_with_name_compression()
    {
        // Arrange: use compressed names with pattern
        var compressor = NameCompressor.FromPattern("stores/*/items/*");
        var storage = CreateStorageProvider();

        var payload1 = "compressed-entity-1"u8.ToArray();
        var payload2 = "compressed-entity-2"u8.ToArray();

        var (snapshotData, positions) = BuildSnapshotData(
            ("stores/abc/items/001", payload1),
            ("stores/abc/items/002", payload2));

        var snapshotIndex = BuildSnapshotIndex(1, compressor!.Pattern, positions);
        await storage.StoreSnapshot(1, snapshotIndex, snapshotData);
        await storage.StoreDelta(2, Array.Empty<byte>(), Array.Empty<byte>());
        await storage.StoreDelta(3, Array.Empty<byte>(), Array.Empty<byte>());

        // Build current index with compressed names
        var entities = positions.ToDictionary(
            kvp => kvp.Key,
            kvp => MakeEntity(version: 1, position: kvp.Value.position, length: kvp.Value.length));
        var currentIndex = new ImmutableDatasetState(
            EntityTrie.FromDictionary(entities, compressor), SnapshotVersion: 1, LatestVersion: 3);

        var indexProvider = new FakeIndexProvider<TestEntity>
        {
            CurrentDatasetState = currentIndex,
            MostRecentSnapshotState = currentIndex,
        };

        var plugin = new SimpleSnapshotCreationPlugin<TestEntity>(
            NullLogger<SimpleSnapshotCreationPlugin<TestEntity>>.Instance,
            storage, CreateConfig(snapshotInterval: 2), Scheduler.Immediate, indexProvider,
            compressor);

        // Act
        await plugin.HandleNewDeltas(CancellationToken.None);

        // Assert
        var newIndexBytes = await storage.ReadSnapshotIndex(3);
        var newIndex = SnapshotIndex.Parser.ParseFrom(newIndexBytes.Span);
        newIndex.SnapshotVersion.Should().Be(3);
        newIndex.IdPattern.Should().Be(compressor.Pattern);

        var restored = ImmutableDatasetState.FromProto(newIndex, compressor);
        restored.Entities.Count.Should().Be(2);
        restored.Entities.ContainsKey("stores/abc/items/001").Should().BeTrue();
        restored.Entities.ContainsKey("stores/abc/items/002").Should().BeTrue();

        // Verify data roundtrip
        var e1 = restored.Entities["stores/abc/items/001"];
        var d1 = await storage.ReadDataFromSources(
            [(ObjectNameTypeEnum.Snapshot, 3, e1.Position)], e1.Length);
        d1.ToArray().Should().BeEquivalentTo(payload1);
    }

    [Fact]
    public async Task Reads_delta_entities_from_delta_source()
    {
        // Arrange: snapshot v1 with one entity, then delta v2 adds another
        var storage = CreateStorageProvider();
        var snapshotPayload = "snapshot-entity"u8.ToArray();
        var deltaPayload = "delta-entity"u8.ToArray();

        var (snapshotData, snapshotPositions) = BuildSnapshotData(
            ("items/old", snapshotPayload));
        var snapshotIndex = BuildSnapshotIndex(1, "", snapshotPositions);
        await storage.StoreSnapshot(1, snapshotIndex, snapshotData);

        // Delta v2: store the delta entity data
        var (deltaData, deltaPositions) = BuildSnapshotData(
            ("items/new", deltaPayload));
        await storage.StoreDelta(2, Array.Empty<byte>(), deltaData);

        // Delta v3 (empty) — to trigger gap condition
        await storage.StoreDelta(3, Array.Empty<byte>(), Array.Empty<byte>());

        // Build the current index with both entities
        var allEntities = new Dictionary<string, IndexEntity>
        {
            // Old entity: from snapshot v1
            ["items/old"] = MakeEntity(version: 1,
                position: snapshotPositions["items/old"].position,
                length: snapshotPositions["items/old"].length),
            // New entity: from delta v2
            ["items/new"] = MakeEntity(version: 2,
                position: deltaPositions["items/new"].position,
                length: deltaPositions["items/new"].length),
        };

        var currentIndex = new ImmutableDatasetState(
            EntityTrie.FromDictionary(allEntities), SnapshotVersion: 1, LatestVersion: 3);

        var indexProvider = new FakeIndexProvider<TestEntity>
        {
            CurrentDatasetState = currentIndex,
            MostRecentSnapshotState = currentIndex,
        };

        var plugin = new SimpleSnapshotCreationPlugin<TestEntity>(
            NullLogger<SimpleSnapshotCreationPlugin<TestEntity>>.Instance,
            storage, CreateConfig(snapshotInterval: 2), Scheduler.Immediate, indexProvider);

        // Act
        await plugin.HandleNewDeltas(CancellationToken.None);

        // Assert: new snapshot has both entities
        var newIndexBytes = await storage.ReadSnapshotIndex(3);
        var newIndex = SnapshotIndex.Parser.ParseFrom(newIndexBytes.Span);
        var restored = ImmutableDatasetState.FromProto(newIndex);
        restored.Entities.Count.Should().Be(2);

        // Verify both entities' data
        var eOld = restored.Entities["items/old"];
        var dOld = await storage.ReadDataFromSources(
            [(ObjectNameTypeEnum.Snapshot, 3, eOld.Position)], eOld.Length);
        dOld.ToArray().Should().BeEquivalentTo(snapshotPayload);

        var eNew = restored.Entities["items/new"];
        var dNew = await storage.ReadDataFromSources(
            [(ObjectNameTypeEnum.Snapshot, 3, eNew.Position)], eNew.Length);
        dNew.ToArray().Should().BeEquivalentTo(deltaPayload);
    }

    [Fact]
    public async Task Skips_snapshot_when_no_current_index()
    {
        var storage = CreateStorageProvider();
        await storage.StoreDelta(1, Array.Empty<byte>(), Array.Empty<byte>());
        await storage.StoreDelta(5, Array.Empty<byte>(), Array.Empty<byte>());

        var indexProvider = new FakeIndexProvider<TestEntity>
        {
            CurrentDatasetState = null,
        };

        var plugin = new SimpleSnapshotCreationPlugin<TestEntity>(
            NullLogger<SimpleSnapshotCreationPlugin<TestEntity>>.Instance,
            storage, CreateConfig(snapshotInterval: 2), Scheduler.Immediate, indexProvider);

        await plugin.HandleNewDeltas(CancellationToken.None);

        var (_, _, snapshots) = await storage.ListVersions();
        snapshots.Should().BeEmpty();
    }

    [Fact]
    public async Task Skips_when_gap_condition_not_met()
    {
        var storage = CreateStorageProvider();

        var (snapshotData, positions) = BuildSnapshotData(("items/1", "data"u8.ToArray()));
        var snapshotIndex = BuildSnapshotIndex(1, "", positions);
        await storage.StoreSnapshot(1, snapshotIndex, snapshotData);
        await storage.StoreDelta(2, Array.Empty<byte>(), Array.Empty<byte>());

        var entities = positions.ToDictionary(
            kvp => kvp.Key,
            kvp => MakeEntity(version: 1, position: kvp.Value.position, length: kvp.Value.length));
        var currentIndex = new ImmutableDatasetState(
            EntityTrie.FromDictionary(entities), SnapshotVersion: 1, LatestVersion: 2);

        var indexProvider = new FakeIndexProvider<TestEntity>
        {
            CurrentDatasetState = currentIndex,
            MostRecentSnapshotState = currentIndex,
        };

        // snapshotInterval=100: gap of 1 delta won't trigger.
        // Use Scheduler.Default so the deferred re-check doesn't execute synchronously.
        var plugin = new SimpleSnapshotCreationPlugin<TestEntity>(
            NullLogger<SimpleSnapshotCreationPlugin<TestEntity>>.Instance,
            storage, CreateConfig(snapshotInterval: 100), Scheduler.Default, indexProvider);

        await plugin.HandleNewDeltas(CancellationToken.None);

        var (_, _, snapshots) = await storage.ListVersions();
        // Only the original snapshot v1 should exist, no new one
        snapshots.Should().HaveCount(1);
        snapshots.Should().Contain(s => s.Version == 1);
    }

    [Fact]
    public async Task Handles_empty_dataset()
    {
        var storage = CreateStorageProvider();
        await storage.StoreDelta(1, Array.Empty<byte>(), Array.Empty<byte>());
        await storage.StoreDelta(5, Array.Empty<byte>(), Array.Empty<byte>());

        var currentIndex = new ImmutableDatasetState(
            EntityTrie.FromDictionary(new Dictionary<string, IndexEntity>()),
            SnapshotVersion: 0, LatestVersion: 5);

        var indexProvider = new FakeIndexProvider<TestEntity>
        {
            CurrentDatasetState = currentIndex,
            MostRecentSnapshotState = null,
        };

        var plugin = new SimpleSnapshotCreationPlugin<TestEntity>(
            NullLogger<SimpleSnapshotCreationPlugin<TestEntity>>.Instance,
            storage, CreateConfig(snapshotInterval: 2), Scheduler.Immediate, indexProvider);

        await plugin.HandleNewDeltas(CancellationToken.None);

        var (_, _, snapshots) = await storage.ListVersions();
        snapshots.Should().Contain(s => s.Version == 5);

        var newIndexBytes = await storage.ReadSnapshotIndex(5);
        var newIndex = SnapshotIndex.Parser.ParseFrom(newIndexBytes.Span);
        var restored = ImmutableDatasetState.FromProto(newIndex);
        restored.Entities.Count.Should().Be(0);
    }

    [Fact]
    public async Task Uses_snapshot_index_positions_when_available()
    {
        // Scenario: currentIndex built on snapshot v1, but snapshotIndex references v5
        // (a newer snapshot). Entity unchanged since v1, so both point to it.
        // The plugin should prefer snapshotIndex's v5 source.
        var storage = CreateStorageProvider();
        var payload = "the-entity-data"u8.ToArray();

        // Store snapshot v1
        var (snapData1, pos1) = BuildSnapshotData(("items/x", payload));
        await storage.StoreSnapshot(1, BuildSnapshotIndex(1, "", pos1), snapData1);

        // Store snapshot v5 (same entity, different position due to different packing)
        var (snapData5, pos5) = BuildSnapshotData(("items/x", payload));
        await storage.StoreSnapshot(5, BuildSnapshotIndex(5, "", pos5), snapData5);

        // Delta v6 + v7 to trigger
        await storage.StoreDelta(6, Array.Empty<byte>(), Array.Empty<byte>());
        await storage.StoreDelta(7, Array.Empty<byte>(), Array.Empty<byte>());

        // currentIndex based on snapshot v1 (entity version=1, position from snap v1)
        var currentEntities = new Dictionary<string, IndexEntity>
        {
            ["items/x"] = MakeEntity(version: 1, position: pos1["items/x"].position, length: pos1["items/x"].length),
        };
        var currentIndex = new ImmutableDatasetState(
            EntityTrie.FromDictionary(currentEntities), SnapshotVersion: 1, LatestVersion: 7);

        // snapshotIndex based on snapshot v5 (same entity but position from snap v5)
        var snapshotEntities = new Dictionary<string, IndexEntity>
        {
            ["items/x"] = MakeEntity(version: 1, position: pos5["items/x"].position, length: pos5["items/x"].length),
        };
        var snapshotIndex = new ImmutableDatasetState(
            EntityTrie.FromDictionary(snapshotEntities), SnapshotVersion: 5, LatestVersion: 5);

        var indexProvider = new FakeIndexProvider<TestEntity>
        {
            CurrentDatasetState = currentIndex,
            MostRecentSnapshotState = snapshotIndex,
        };

        var plugin = new SimpleSnapshotCreationPlugin<TestEntity>(
            NullLogger<SimpleSnapshotCreationPlugin<TestEntity>>.Instance,
            storage, CreateConfig(snapshotInterval: 2), Scheduler.Immediate, indexProvider);

        // Act
        await plugin.HandleNewDeltas(CancellationToken.None);

        // Assert: new snapshot created and data is correct
        var newIndexBytes = await storage.ReadSnapshotIndex(7);
        var newIndex = SnapshotIndex.Parser.ParseFrom(newIndexBytes.Span);
        var restored = ImmutableDatasetState.FromProto(newIndex);
        restored.Entities.Count.Should().Be(1);

        var e = restored.Entities["items/x"];
        var d = await storage.ReadDataFromSources(
            [(ObjectNameTypeEnum.Snapshot, 7, e.Position)], e.Length);
        d.ToArray().Should().BeEquivalentTo(payload);
    }
}
