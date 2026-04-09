using FluentAssertions;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using RxDatasets.Models;

namespace RxDatasets.Tests;

public class NestedIndexHelperTests
{
    private static IndexEntity MakeEntity(ulong version = 1, long position = 100, int length = 50)
        => new(version, position, length, 42);

    /// <summary>
    /// Builds a SnapshotIndex proto from entities by placing them directly in the nested SnapshotBucket.
    /// </summary>
    private static SnapshotIndex BuildSnapshotProto(ulong snapshotVersion, params (string name, SnapshotEntity entity)[] entries)
    {
        var proto = new SnapshotIndex { SnapshotVersion = snapshotVersion };
        var root = new SnapshotBucket();

        foreach (var (name, entity) in entries)
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
            current.Leaves[segments[^1]] = entity;
        }

        proto.Nested = root;
        return proto;
    }

    private static SnapshotEntity MakeProtoEntity(ulong insertedVersion = 1, long position = 100, int length = 50, uint crc32 = 42)
    {
        var e = new SnapshotEntity { Position = position, Length = length, Crc32 = crc32 };
        e.LastInsertedVersion.Add(insertedVersion);
        return e;
    }

    private static SnapshotEntity MakeDeletedProtoEntity()
    {
        var e = new SnapshotEntity { Position = 0, Length = 0, Crc32 = 0 };
        e.LastInsertedVersion.Add(1);
        e.LastDeletedVersion.Add(2);
        return e;
    }

    #region SnapshotIndex: nested roundtrip

    [Fact]
    public void SnapshotIndex_Roundtrip_PreservesAllEntities()
    {
        var proto = BuildSnapshotProto(5,
            ("tenants/abc/stores/def/items/1", MakeProtoEntity(1, 0, 10)),
            ("tenants/abc/stores/def/items/2", MakeProtoEntity(2, 10, 20)),
            ("tenants/abc/stores/ghi/items/3", MakeProtoEntity(3, 30, 30)),
            ("other/xyz/data/4", MakeProtoEntity(4, 60, 40)));

        // Flat entities should be empty (nested-only format)
        proto.Entities.Should().BeEmpty();
        proto.Nested.Should().NotBeNull();

        // Deserialize into slim index
        var restored = ImmutableDatasetState.FromProto(proto);

        restored.SnapshotVersion.Should().Be(5);
        restored.Entities.Count.Should().Be(4);
        restored.Entities["tenants/abc/stores/def/items/1"].Should().BeEquivalentTo(MakeEntity(1, 0, 10));
        restored.Entities["tenants/abc/stores/def/items/2"].Should().BeEquivalentTo(MakeEntity(2, 10, 20));
        restored.Entities["tenants/abc/stores/ghi/items/3"].Should().BeEquivalentTo(MakeEntity(3, 30, 30));
        restored.Entities["other/xyz/data/4"].Should().BeEquivalentTo(MakeEntity(4, 60, 40));
    }

    [Fact]
    public void SnapshotIndex_Roundtrip_ThroughBytes()
    {
        var proto = BuildSnapshotProto(3,
            ("a/b/c", MakeProtoEntity(1, 0, 10)),
            ("a/b/d", MakeProtoEntity(2, 10, 20)));

        var bytes = proto.ToByteArray();

        // Parse from raw bytes (simulates reading from storage)
        var parsed = SnapshotIndex.Parser.ParseFrom(bytes);
        var restored = ImmutableDatasetState.FromProto(parsed);

        restored.Entities.Count.Should().Be(2);
        restored.Entities["a/b/c"].Should().BeEquivalentTo(MakeEntity(1, 0, 10));
        restored.Entities["a/b/d"].Should().BeEquivalentTo(MakeEntity(2, 10, 20));
    }

    [Fact]
    public void SnapshotIndex_EmptyTrie_Roundtrips()
    {
        var proto = new SnapshotIndex { SnapshotVersion = 0 };
        var restored = ImmutableDatasetState.FromProto(proto);

        restored.Entities.Count.Should().Be(0);
    }

    [Fact]
    public void SnapshotIndex_WithDeletedEntities_SkipsDeletedInTrie()
    {
        var proto = BuildSnapshotProto(2,
            ("a/live", MakeProtoEntity(1, 0, 10)),
            ("a/dead", MakeDeletedProtoEntity()));

        var restored = ImmutableDatasetState.FromProto(proto);

        // Deleted entities should NOT be in the trie
        restored.Entities.Count.Should().Be(1);
        restored.Entities.ContainsKey("a/live").Should().BeTrue();
        restored.Entities.ContainsKey("a/dead").Should().BeFalse();
    }

    [Fact]
    public void SnapshotIndex_SingleSegmentName_Roundtrips()
    {
        var proto = BuildSnapshotProto(1, ("root", MakeProtoEntity(1, 0, 10)));

        var restored = ImmutableDatasetState.FromProto(proto);

        restored.Entities.Count.Should().Be(1);
        restored.Entities["root"].Should().BeEquivalentTo(MakeEntity(1, 0, 10));
    }

    #endregion

    #region SnapshotIndex: reading legacy flat format

    [Fact]
    public void SnapshotIndex_ReadsFlatFormat_WhenNoNestedPresent()
    {
        // Build a legacy flat SnapshotIndex (no nested field)
        var proto = new SnapshotIndex { SnapshotVersion = 5 };
        proto.Entities.Add("a/b/c", new SnapshotEntity
        {
            LastInsertedVersion = { 1 },
            Position = 0,
            Length = 10,
            Crc32 = 42
        });
        proto.Entities.Add("x/y/z", new SnapshotEntity
        {
            LastInsertedVersion = { 2 },
            Position = 10,
            Length = 20,
            Crc32 = 99
        });

        var state = ImmutableDatasetState.FromProto(proto);

        state.SnapshotVersion.Should().Be(5);
        state.Entities.Count.Should().Be(2);
        state.Entities["a/b/c"].Position.Should().Be(0);
        state.Entities["x/y/z"].Position.Should().Be(10);
    }

    [Fact]
    public void SnapshotIndex_PrefersNested_OverFlat()
    {
        // Build a SnapshotIndex with BOTH nested and flat (shouldn't happen normally)
        var proto = new SnapshotIndex { SnapshotVersion = 5 };

        // Flat: has entity "flat/only" at position 999
        proto.Entities.Add("flat/only", new SnapshotEntity
        {
            LastInsertedVersion = { 1 },
            Position = 999,
            Length = 10,
            Crc32 = 1
        });

        // Nested: has entity "nested/only" at position 100
        var bucket = new SnapshotBucket();
        var nestedChild = new SnapshotBucket();
        nestedChild.Leaves.Add("only", new SnapshotEntity
        {
            LastInsertedVersion = { 2 },
            Position = 100,
            Length = 20,
            Crc32 = 2
        });
        bucket.Children.Add("nested", nestedChild);
        proto.Nested = bucket;

        var state = ImmutableDatasetState.FromProto(proto);

        // Should use nested, not flat
        state.Entities.Count.Should().Be(1);
        state.Entities.ContainsKey("nested/only").Should().BeTrue();
        state.Entities.ContainsKey("flat/only").Should().BeFalse();
    }

    #endregion

    #region SnapshotIndex: EnsureFlat

    [Fact]
    public void EnsureFlat_PopulatesFlatFromNested()
    {
        var proto = BuildSnapshotProto(1,
            ("a/b/c", MakeProtoEntity(1, 0, 10)),
            ("x/y", MakeProtoEntity(2, 10, 20)));

        // flat is empty
        proto.Entities.Should().BeEmpty();

        NestedIndexHelper.EnsureFlat(proto);

        proto.Entities.Should().HaveCount(2);
        proto.Entities.Should().ContainKey("a/b/c");
        proto.Entities.Should().ContainKey("x/y");
        proto.Entities["a/b/c"].Position.Should().Be(0);
        proto.Entities["x/y"].Position.Should().Be(10);
    }

    [Fact]
    public void EnsureFlat_NoOpIfAlreadyFlat()
    {
        var proto = new SnapshotIndex { SnapshotVersion = 1 };
        proto.Entities.Add("a/b", new SnapshotEntity { Position = 42 });

        NestedIndexHelper.EnsureFlat(proto);

        // Should not modify
        proto.Entities.Should().HaveCount(1);
        proto.Entities["a/b"].Position.Should().Be(42);
    }

    #endregion

    #region DeltaIndex: nested roundtrip

    [Fact]
    public void DeltaIndex_SerializeAndParse_Roundtrips_NewEntities()
    {
        var delta = new DeltaIndex
        {
            DeltaVersion = 10,
            Timestamp = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow)
        };
        delta.NewEntities.Add(new DeltaEntity { Name = "t/a/items/1", Position = 0, Length = 10, Crc32 = 1, PreviousVersion = 0 });
        delta.NewEntities.Add(new DeltaEntity { Name = "t/a/items/2", Position = 10, Length = 20, Crc32 = 2, PreviousVersion = 0 });

        var bytes = NestedIndexHelper.SerializeDeltaIndex(delta);
        var restored = NestedIndexHelper.ParseDeltaIndex(bytes);

        restored.DeltaVersion.Should().Be(10);
        restored.NewEntities.Should().HaveCount(2);
        restored.NewEntities.Should().Contain(e => e.Name == "t/a/items/1" && e.Position == 0 && e.Length == 10 && e.Crc32 == 1);
        restored.NewEntities.Should().Contain(e => e.Name == "t/a/items/2" && e.Position == 10 && e.Length == 20 && e.Crc32 == 2);
    }

    [Fact]
    public void DeltaIndex_SerializeAndParse_Roundtrips_ChangedEntities()
    {
        var delta = new DeltaIndex { DeltaVersion = 5 };
        delta.ChangedEntities.Add(new DeltaEntity { Name = "a/b/c", Position = 50, Length = 30, Crc32 = 99, PreviousVersion = 3 });

        var bytes = NestedIndexHelper.SerializeDeltaIndex(delta);
        var restored = NestedIndexHelper.ParseDeltaIndex(bytes);

        restored.ChangedEntities.Should().HaveCount(1);
        var entity = restored.ChangedEntities[0];
        entity.Name.Should().Be("a/b/c");
        entity.Position.Should().Be(50);
        entity.Length.Should().Be(30);
        entity.Crc32.Should().Be(99u);
        entity.PreviousVersion.Should().Be(3u);
    }

    [Fact]
    public void DeltaIndex_SerializeAndParse_Roundtrips_DeletedEntities()
    {
        var delta = new DeltaIndex { DeltaVersion = 7 };
        delta.DeletedEntities.Add(new DeletedEntity { Name = "p/q/r/s", PreviousVersion = 5 });

        var bytes = NestedIndexHelper.SerializeDeltaIndex(delta);
        var restored = NestedIndexHelper.ParseDeltaIndex(bytes);

        restored.DeletedEntities.Should().HaveCount(1);
        restored.DeletedEntities[0].Name.Should().Be("p/q/r/s");
        restored.DeletedEntities[0].PreviousVersion.Should().Be(5u);
    }

    [Fact]
    public void DeltaIndex_SerializeAndParse_Roundtrips_DeletedNames()
    {
        var delta = new DeltaIndex { DeltaVersion = 3 };
        delta.DeletedNames.Add("old/legacy/name");

        var bytes = NestedIndexHelper.SerializeDeltaIndex(delta);
        var restored = NestedIndexHelper.ParseDeltaIndex(bytes);

        restored.DeletedNames.Should().HaveCount(1);
        restored.DeletedNames[0].Should().Be("old/legacy/name");
    }

    [Fact]
    public void DeltaIndex_SerializeAndParse_MixedEntityTypes()
    {
        var delta = new DeltaIndex { DeltaVersion = 20 };
        delta.NewEntities.Add(new DeltaEntity { Name = "prefix/a/new1", Position = 0, Length = 10, Crc32 = 1, PreviousVersion = 0 });
        delta.ChangedEntities.Add(new DeltaEntity { Name = "prefix/a/changed1", Position = 10, Length = 15, Crc32 = 2, PreviousVersion = 18 });
        delta.DeletedEntities.Add(new DeletedEntity { Name = "prefix/b/deleted1", PreviousVersion = 15 });
        delta.DeletedNames.Add("prefix/c/legacy_deleted");

        var bytes = NestedIndexHelper.SerializeDeltaIndex(delta);
        var restored = NestedIndexHelper.ParseDeltaIndex(bytes);

        restored.DeltaVersion.Should().Be(20);
        restored.NewEntities.Should().HaveCount(1);
        restored.NewEntities[0].Name.Should().Be("prefix/a/new1");
        restored.ChangedEntities.Should().HaveCount(1);
        restored.ChangedEntities[0].Name.Should().Be("prefix/a/changed1");
        restored.DeletedEntities.Should().HaveCount(1);
        restored.DeletedEntities[0].Name.Should().Be("prefix/b/deleted1");
        restored.DeletedNames.Should().HaveCount(1);
        restored.DeletedNames[0].Should().Be("prefix/c/legacy_deleted");
    }

    [Fact]
    public void DeltaIndex_EmptyDelta_Roundtrips()
    {
        var delta = new DeltaIndex { DeltaVersion = 1 };

        var bytes = NestedIndexHelper.SerializeDeltaIndex(delta);
        var restored = NestedIndexHelper.ParseDeltaIndex(bytes);

        restored.DeltaVersion.Should().Be(1);
        restored.NewEntities.Should().BeEmpty();
        restored.ChangedEntities.Should().BeEmpty();
        restored.DeletedEntities.Should().BeEmpty();
        restored.DeletedNames.Should().BeEmpty();
    }

    [Fact]
    public void SerializeDeltaIndex_DoesNotMutateOriginal()
    {
        var delta = new DeltaIndex { DeltaVersion = 5 };
        delta.NewEntities.Add(new DeltaEntity { Name = "a/b", Position = 0, Length = 10 });
        delta.DeletedEntities.Add(new DeletedEntity { Name = "c/d", PreviousVersion = 3 });

        NestedIndexHelper.SerializeDeltaIndex(delta);

        // Original should still have its flat fields intact
        delta.NewEntities.Should().HaveCount(1);
        delta.NewEntities[0].Name.Should().Be("a/b");
        delta.DeletedEntities.Should().HaveCount(1);
        delta.DeletedEntities[0].Name.Should().Be("c/d");
    }

    #endregion

    #region DeltaIndex: reading legacy flat format

    [Fact]
    public void ParseDeltaIndex_ReadsFlatFormat_WhenNoNestedPresent()
    {
        // Simulate a legacy flat DeltaIndex (serialized without nesting)
        var legacyDelta = new DeltaIndex { DeltaVersion = 3 };
        legacyDelta.NewEntities.Add(new DeltaEntity { Name = "flat/entity", Position = 0, Length = 10 });
        legacyDelta.DeletedNames.Add("flat/deleted");

        // Serialize without nesting (raw proto)
        var bytes = legacyDelta.ToByteArray();

        // Parse with the new helper
        var restored = NestedIndexHelper.ParseDeltaIndex(bytes);

        restored.DeltaVersion.Should().Be(3);
        restored.NewEntities.Should().HaveCount(1);
        restored.NewEntities[0].Name.Should().Be("flat/entity");
        restored.DeletedNames.Should().HaveCount(1);
        restored.DeletedNames[0].Should().Be("flat/deleted");
    }

    #endregion

    #region DeltaIndex: single-segment entity names

    [Fact]
    public void DeltaIndex_SingleSegmentNames_Roundtrip()
    {
        var delta = new DeltaIndex { DeltaVersion = 1 };
        delta.NewEntities.Add(new DeltaEntity { Name = "simple", Position = 0, Length = 5 });

        var bytes = NestedIndexHelper.SerializeDeltaIndex(delta);
        var restored = NestedIndexHelper.ParseDeltaIndex(bytes);

        restored.NewEntities.Should().HaveCount(1);
        restored.NewEntities[0].Name.Should().Be("simple");
    }

    #endregion
}
