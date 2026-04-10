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

    #region On-disk ID pattern compression: DeltaIndex

    [Fact]
    public void DeltaIndex_WithCompressor_CompressesNamesOnDisk()
    {
        var compressor = NameCompressor.FromPattern("t/*/s/*/items/*/values")!;
        var delta = new DeltaIndex { DeltaVersion = 10 };
        delta.NewEntities.Add(new DeltaEntity { Name = "t/abc/s/def/items/001/values", Position = 0, Length = 10 });
        delta.ChangedEntities.Add(new DeltaEntity { Name = "t/abc/s/def/items/002/values", Position = 10, Length = 20, PreviousVersion = 5 });
        delta.DeletedEntities.Add(new DeletedEntity { Name = "t/abc/s/ghi/items/003/values", PreviousVersion = 7 });
        delta.DeletedNames.Add("t/abc/s/ghi/items/004/values");

        var bytes = NestedIndexHelper.SerializeDeltaIndex(delta, compressor);

        // Verify the on-disk proto has compressed names and id_pattern
        var rawParsed = DeltaIndex.Parser.ParseFrom(bytes);
        rawParsed.IdPattern.Should().Be("t/*/s/*/items/*/values");
        // Nested format — flatten to check names
        NestedIndexHelper.FlattenDeltaIndex(rawParsed);
        rawParsed.NewEntities[0].Name.Should().Be("abc/def/001");
        rawParsed.ChangedEntities[0].Name.Should().Be("abc/def/002");
        rawParsed.DeletedEntities[0].Name.Should().Be("abc/ghi/003");
        rawParsed.DeletedNames[0].Should().Be("abc/ghi/004");
    }

    [Fact]
    public void DeltaIndex_WithCompressor_RoundtripsToFullNames()
    {
        var compressor = NameCompressor.FromPattern("t/{tenantid}/s/{storeid}/items/{itemid}/values")!;
        var delta = new DeltaIndex { DeltaVersion = 10 };
        delta.NewEntities.Add(new DeltaEntity { Name = "t/abc/s/def/items/001/values", Position = 0, Length = 10, Crc32 = 1 });
        delta.ChangedEntities.Add(new DeltaEntity { Name = "t/abc/s/def/items/002/values", Position = 10, Length = 20, Crc32 = 2, PreviousVersion = 5 });
        delta.DeletedEntities.Add(new DeletedEntity { Name = "t/abc/s/ghi/items/003/values", PreviousVersion = 7 });
        delta.DeletedNames.Add("t/abc/s/ghi/items/004/values");

        // Serialize with compression, then parse back (ParseDeltaIndex expands)
        var bytes = NestedIndexHelper.SerializeDeltaIndex(delta, compressor);
        var restored = NestedIndexHelper.ParseDeltaIndex(bytes);

        restored.DeltaVersion.Should().Be(10);
        restored.NewEntities.Should().HaveCount(1);
        restored.NewEntities[0].Name.Should().Be("t/abc/s/def/items/001/values");
        restored.NewEntities[0].Position.Should().Be(0);
        restored.NewEntities[0].Crc32.Should().Be(1u);
        restored.ChangedEntities.Should().HaveCount(1);
        restored.ChangedEntities[0].Name.Should().Be("t/abc/s/def/items/002/values");
        restored.DeletedEntities.Should().HaveCount(1);
        restored.DeletedEntities[0].Name.Should().Be("t/abc/s/ghi/items/003/values");
        restored.DeletedNames.Should().HaveCount(1);
        restored.DeletedNames[0].Should().Be("t/abc/s/ghi/items/004/values");
    }

    [Fact]
    public void DeltaIndex_WithCompressor_DoesNotMutateOriginal()
    {
        var compressor = NameCompressor.FromPattern("t/*/items/*/values")!;
        var delta = new DeltaIndex { DeltaVersion = 5 };
        delta.NewEntities.Add(new DeltaEntity { Name = "t/abc/items/001/values", Position = 0, Length = 10 });

        NestedIndexHelper.SerializeDeltaIndex(delta, compressor);

        // Original delta should still have full names
        delta.NewEntities[0].Name.Should().Be("t/abc/items/001/values");
    }

    [Fact]
    public void DeltaIndex_WithoutCompressor_HasNoIdPattern()
    {
        var delta = new DeltaIndex { DeltaVersion = 5 };
        delta.NewEntities.Add(new DeltaEntity { Name = "a/b/c", Position = 0, Length = 10 });

        var bytes = NestedIndexHelper.SerializeDeltaIndex(delta);
        var rawParsed = DeltaIndex.Parser.ParseFrom(bytes);

        rawParsed.IdPattern.Should().BeEmpty();
    }

    #endregion

    #region On-disk ID pattern compression: SnapshotIndex

    [Fact]
    public void SnapshotIndex_WithCompressor_RoundtripsViaFromProto()
    {
        // Build a snapshot proto with compressed names (simulating what SnapshotCreationPlugin produces)
        var compressor = NameCompressor.FromPattern("t/*/s/*/items/*/values")!;
        var proto = BuildSnapshotProto(10,
            ("abc/def/001", MakeProtoEntity(1, 0, 10)),
            ("abc/def/002", MakeProtoEntity(2, 10, 20)),
            ("abc/ghi/003", MakeProtoEntity(3, 30, 30)));
        proto.IdPattern = compressor.Pattern;

        // Serialize to bytes and parse back (simulates disk roundtrip)
        var bytes = proto.ToByteArray();
        var parsed = SnapshotIndex.Parser.ParseFrom(bytes);

        // FromProto should auto-detect the compressor and produce full names
        var state = ImmutableDatasetState.FromProto(parsed);

        state.Entities.Count.Should().Be(3);
        state.Entities.ContainsKey("t/abc/s/def/items/001/values").Should().BeTrue();
        state.Entities.ContainsKey("t/abc/s/def/items/002/values").Should().BeTrue();
        state.Entities.ContainsKey("t/abc/s/ghi/items/003/values").Should().BeTrue();
    }

    [Fact]
    public void SnapshotIndex_WithCompressor_FromProtoUsesExplicitCompressor()
    {
        // Build with compressed names
        var compressor = NameCompressor.FromPattern("t/*/s/*/items/*/values")!;
        var proto = BuildSnapshotProto(5,
            ("abc/def/001", MakeProtoEntity(1, 0, 10)));
        proto.IdPattern = compressor.Pattern;

        // Pass an explicit compressor (same pattern) — should use it
        var state = ImmutableDatasetState.FromProto(proto, compressor);

        state.Entities.Count.Should().Be(1);
        state.Entities.ContainsKey("t/abc/s/def/items/001/values").Should().BeTrue();
    }

    [Fact]
    public void SnapshotIndex_WithCompressor_LegacyFlatFormat()
    {
        // Build a flat snapshot with compressed names + id_pattern
        var compressor = NameCompressor.FromPattern("t/*/items/*/values")!;
        var proto = new SnapshotIndex { SnapshotVersion = 5, IdPattern = compressor.Pattern };
        proto.Entities.Add("abc/001", MakeProtoEntity(1, 0, 10));
        proto.Entities.Add("abc/002", MakeProtoEntity(2, 10, 20));

        var state = ImmutableDatasetState.FromProto(proto);

        state.Entities.Count.Should().Be(2);
        state.Entities.ContainsKey("t/abc/items/001/values").Should().BeTrue();
        state.Entities.ContainsKey("t/abc/items/002/values").Should().BeTrue();
    }

    [Fact]
    public void SnapshotIndex_WithoutCompressor_LegacyFullNames()
    {
        // No id_pattern — full names in proto, no compressor auto-created
        var proto = BuildSnapshotProto(5,
            ("t/abc/s/def/items/001/values", MakeProtoEntity(1, 0, 10)));

        var state = ImmutableDatasetState.FromProto(proto);

        state.Entities.Count.Should().Be(1);
        state.Entities.ContainsKey("t/abc/s/def/items/001/values").Should().BeTrue();
    }

    [Fact]
    public void SnapshotIndex_PatternTransition_CompressedToNewPattern()
    {
        // Snapshot written with pattern A, now reading with pattern B.
        // Proto has compressed names under pattern A — must expand with A, re-compress with B.
        var patternA = NameCompressor.FromPattern("t/*/items/*/values")!;
        var patternB = NameCompressor.FromPattern("region/*/store/*/data")!;

        // Simulate a snapshot written with pattern A: compressed names are "abc/001"
        var proto = BuildSnapshotProto(5,
            ("abc/001", MakeProtoEntity(1, 0, 10)),
            ("abc/002", MakeProtoEntity(2, 10, 20)));
        proto.IdPattern = patternA.Pattern;

        var state = ImmutableDatasetState.FromProto(proto, patternB);

        // The trie should contain full names expanded from pattern A
        state.Entities.Count.Should().Be(2);
        state.Entities.ContainsKey("t/abc/items/001/values").Should().BeTrue();
        state.Entities.ContainsKey("t/abc/items/002/values").Should().BeTrue();
    }

    [Fact]
    public void SnapshotIndex_PatternTransition_CompressedToNoCompressor()
    {
        // Snapshot written with compression, but runtime compressor is explicitly null.
        // Auto-detect at line 9-10 creates compressor from proto → same pattern → direct path.
        var compressor = NameCompressor.FromPattern("t/*/items/*/values")!;

        var proto = BuildSnapshotProto(5,
            ("abc/001", MakeProtoEntity(1, 0, 10)));
        proto.IdPattern = compressor.Pattern;

        // Pass null compressor — auto-detect should kick in
        var state = ImmutableDatasetState.FromProto(proto, null);

        state.Entities.Count.Should().Be(1);
        state.Entities.ContainsKey("t/abc/items/001/values").Should().BeTrue();
    }

    [Fact]
    public void SnapshotIndex_PatternTransition_UncompressedToNewCompressor()
    {
        // Snapshot written without compression (full names in nested), now reading with a compressor.
        var compressor = NameCompressor.FromPattern("t/*/items/*/values")!;

        var proto = BuildSnapshotProto(5,
            ("t/abc/items/001/values", MakeProtoEntity(1, 0, 10)),
            ("t/abc/items/002/values", MakeProtoEntity(2, 10, 20)));
        // No IdPattern set — uncompressed nested

        var state = ImmutableDatasetState.FromProto(proto, compressor);

        state.Entities.Count.Should().Be(2);
        state.Entities.ContainsKey("t/abc/items/001/values").Should().BeTrue();
        state.Entities.ContainsKey("t/abc/items/002/values").Should().BeTrue();
    }

    [Fact]
    public void DeltaIndex_PatternTransition_CompressedDelta_UncompressedTrie()
    {
        // Delta written with compression, applied to trie without compression.
        // ParseDeltaIndex expands flat names → flat fallback should work.
        var compressor = NameCompressor.FromPattern("t/*/items/*/values")!;

        var delta = new DeltaIndex { DeltaVersion = 2 };
        delta.NewEntities.Add(new DeltaEntity { Name = "t/abc/items/001/values", Position = 0, Length = 10 });

        var bytes = NestedIndexHelper.SerializeDeltaIndex(delta, compressor);
        var parsed = NestedIndexHelper.ParseDeltaIndex(bytes);

        // Flat fields should have expanded names
        parsed.NewEntities[0].Name.Should().Be("t/abc/items/001/values");

        // Apply to an uncompressed trie
        var state = new ImmutableDatasetState(
            EntityTrie.FromDictionary(new Dictionary<string, IndexEntity>
            {
                ["t/abc/items/000/values"] = new(1, 0, 5)
            }), 1, 1);

        var newState = state.ApplyDelta(parsed);
        newState.Entities.Count.Should().Be(2);
        newState.Entities.ContainsKey("t/abc/items/000/values").Should().BeTrue();
        newState.Entities.ContainsKey("t/abc/items/001/values").Should().BeTrue();
    }

    [Fact]
    public void DeltaIndex_PatternTransition_UncompressedDelta_CompressedTrie()
    {
        // Legacy delta without compression, applied to a compressed trie.
        var compressor = NameCompressor.FromPattern("t/*/items/*/values")!;

        // Serialize WITHOUT compression (legacy format)
        var delta = new DeltaIndex { DeltaVersion = 2 };
        delta.NewEntities.Add(new DeltaEntity { Name = "t/abc/items/001/values", Position = 0, Length = 10 });
        var bytes = NestedIndexHelper.SerializeDeltaIndex(delta); // no compressor
        var parsed = NestedIndexHelper.ParseDeltaIndex(bytes);

        // Apply to a compressed trie
        var state = new ImmutableDatasetState(
            EntityTrie.FromDictionary(new Dictionary<string, IndexEntity>
            {
                ["t/abc/items/000/values"] = new(1, 0, 5)
            }, compressor), 1, 1);

        var newState = state.ApplyDelta(parsed);
        newState.Entities.Count.Should().Be(2);
        newState.Entities.ContainsKey("t/abc/items/000/values").Should().BeTrue();
        newState.Entities.ContainsKey("t/abc/items/001/values").Should().BeTrue();
    }

    [Fact]
    public void DeltaIndex_PatternTransition_DifferentPatterns()
    {
        // Delta compressed with pattern A, trie compressed with pattern B.
        // Patterns don't match → flat fallback with expanded names.
        var patternA = NameCompressor.FromPattern("t/*/items/*/values")!;
        var patternB = NameCompressor.FromPattern("region/*/store/*/data")!;

        var delta = new DeltaIndex { DeltaVersion = 2 };
        delta.NewEntities.Add(new DeltaEntity { Name = "t/abc/items/001/values", Position = 0, Length = 10 });
        var bytes = NestedIndexHelper.SerializeDeltaIndex(delta, patternA);
        var parsed = NestedIndexHelper.ParseDeltaIndex(bytes);

        // Flat names should be expanded with pattern A
        parsed.NewEntities[0].Name.Should().Be("t/abc/items/001/values");

        // Apply to a trie with pattern B (which doesn't match)
        var state = new ImmutableDatasetState(
            EntityTrie.FromDictionary([], patternB), 1, 1);

        var newState = state.ApplyDelta(parsed);
        newState.Entities.Count.Should().Be(1);
        // The trie stores with full names since pattern B can't compress pattern A names
        newState.Entities.ContainsKey("t/abc/items/001/values").Should().BeTrue();
    }

    #endregion

    #region On-disk ID pattern compression: Snapshot position via trie

    [Fact]
    public void SnapshotState_WithCompressor_ExpandsNestedNames()
    {
        var compressor = NameCompressor.FromPattern("t/*/s/*/items/*/values")!;
        var proto = BuildSnapshotProto(5,
            ("abc/def/001", MakeProtoEntity(1, 0, 10)),
            ("abc/ghi/002", MakeProtoEntity(2, 10, 20)));
        proto.IdPattern = compressor.Pattern;

        var state = ImmutableDatasetState.FromProto(proto, compressor);

        state.Entities.TryGetValue("t/abc/s/def/items/001/values", out var e1).Should().BeTrue();
        e1.Position.Should().Be(0);
        state.Entities.TryGetValue("t/abc/s/ghi/items/002/values", out var e2).Should().BeTrue();
        e2.Position.Should().Be(10);
    }

    [Fact]
    public void SnapshotState_WithCompressor_ExpandsFlatNames()
    {
        var compressor = NameCompressor.FromPattern("t/*/items/*/values")!;
        var proto = new SnapshotIndex { SnapshotVersion = 5, IdPattern = compressor.Pattern };
        proto.Entities.Add("abc/001", MakeProtoEntity(1, 0, 10));

        var state = ImmutableDatasetState.FromProto(proto, compressor);

        state.Entities.TryGetValue("t/abc/items/001/values", out var entity).Should().BeTrue();
        entity.Position.Should().Be(0);
    }

    [Fact]
    public void SnapshotState_WithoutCompressor_UsesFullNames()
    {
        var proto = BuildSnapshotProto(5,
            ("t/abc/s/def/items/001/values", MakeProtoEntity(1, 0, 10)));

        var state = ImmutableDatasetState.FromProto(proto);

        state.Entities.TryGetValue("t/abc/s/def/items/001/values", out var entity).Should().BeTrue();
        entity.Position.Should().Be(0);
    }

    #endregion

    #region On-disk ID pattern compression: EnsureFlat + ExpandSnapshotNames

    [Fact]
    public void EnsureFlat_ThenExpand_ProducesFullNames()
    {
        var compressor = NameCompressor.FromPattern("t/*/s/*/items/*/values")!;
        var proto = BuildSnapshotProto(5,
            ("abc/def/001", MakeProtoEntity(1, 0, 10)),
            ("abc/ghi/002", MakeProtoEntity(2, 10, 20)));
        proto.IdPattern = compressor.Pattern;

        NestedIndexHelper.EnsureFlat(proto);
        NestedIndexHelper.ExpandSnapshotNames(proto);

        proto.Entities.Should().HaveCount(2);
        proto.Entities.Should().ContainKey("t/abc/s/def/items/001/values");
        proto.Entities.Should().ContainKey("t/abc/s/ghi/items/002/values");
    }

    [Fact]
    public void ExpandSnapshotNames_NoOpWithoutIdPattern()
    {
        var proto = new SnapshotIndex { SnapshotVersion = 1 };
        proto.Entities.Add("a/b", MakeProtoEntity(1, 0, 10));

        NestedIndexHelper.ExpandSnapshotNames(proto);

        proto.Entities.Should().HaveCount(1);
        proto.Entities.Should().ContainKey("a/b");
    }

    #endregion
}
