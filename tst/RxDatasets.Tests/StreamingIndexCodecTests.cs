using FluentAssertions;
using Google.Protobuf;
using RxDatasets.Models;

namespace RxDatasets.Tests;

/// <summary>
/// Tests for <see cref="StreamingIndexCodec"/> — the span-based streaming protobuf reader/writer.
/// Verifies that:
/// 1. StreamingIndexCodec.WriteSnapshot produces bytes that proto can parse back
/// 2. StreamingIndexCodec.ReadSnapshot produces identical results to proto-based parsing
/// 3. id_pattern is written first in the byte stream
/// 4. Legacy flat and nested formats both work
/// 5. Deleted entities are excluded from the trie
/// 6. Position lookup streaming reader produces correct results
/// 7. Delta writer puts id_pattern first
/// </summary>
public class StreamingIndexCodecTests
{
    private const string Pattern = "t/*/s/*/items/*/values";

    #region ── Helpers ──

    private static SnapshotBucket BuildNestedBucket(Dictionary<string, SnapshotEntity> entities)
    {
        var root = new SnapshotBucket();
        foreach (var (name, entity) in entities)
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
        return root;
    }

    private static SnapshotEntity MakeEntity(ulong insertedVersion, long position, int length, uint crc32 = 0)
    {
        var entity = new SnapshotEntity { Position = position, Length = length, Crc32 = crc32 };
        entity.LastInsertedVersion.Add(insertedVersion);
        return entity;
    }

    private static SnapshotEntity MakeDeletedEntity(ulong insertedVersion, ulong deletedVersion, long position, int length)
    {
        var entity = new SnapshotEntity { Position = position, Length = length };
        entity.LastInsertedVersion.Add(insertedVersion);
        entity.LastDeletedVersion.Add(deletedVersion);
        return entity;
    }

    private static byte[] WriteProtoSnapshot(ulong version, string idPattern, SnapshotBucket nested)
    {
        var proto = new SnapshotIndex
        {
            SnapshotVersion = version,
            Nested = nested,
            IdPattern = idPattern
        };
        return proto.ToByteArray();
    }

    private static byte[] WriteLegacyFlatSnapshot(ulong version, Dictionary<string, SnapshotEntity> entities)
    {
        var proto = new SnapshotIndex { SnapshotVersion = version };
        foreach (var (name, entity) in entities)
            proto.Entities[name] = entity;
        return proto.ToByteArray();
    }

    #endregion

    #region ── Snapshot Writer Tests ──

    [Fact]
    public void WriteSnapshot_ProtoCanParseBack()
    {
        var compressor = NameCompressor.FromPattern(Pattern)!;
        var entities = new Dictionary<string, SnapshotEntity>
        {
            ["abc/def/001"] = MakeEntity(10, 0, 100),
            ["abc/def/002"] = MakeEntity(10, 100, 200),
        };
        var nested = BuildNestedBucket(entities);

        var bytes = StreamingIndexCodec.WriteSnapshot(10, compressor.Pattern, nested);
        var parsed = SnapshotIndex.Parser.ParseFrom(bytes);

        parsed.SnapshotVersion.Should().Be(10);
        parsed.IdPattern.Should().Be(Pattern);
        parsed.Nested.Should().NotBeNull();
        parsed.Nested.Children.Should().ContainKey("abc");
    }

    [Fact]
    public void WriteSnapshot_IdPatternIsFirstField()
    {
        var compressor = NameCompressor.FromPattern(Pattern)!;
        var entities = new Dictionary<string, SnapshotEntity>
        {
            ["abc/def/001"] = MakeEntity(10, 0, 100),
        };
        var nested = BuildNestedBucket(entities);

        var bytes = StreamingIndexCodec.WriteSnapshot(10, compressor.Pattern, nested);

        // First byte should be the tag for field 34, wire type 2 (LEN)
        // tag = (34 << 3) | 2 = 274 → varint: 0x92 0x02
        bytes[0].Should().Be(0x92);
        bytes[1].Should().Be(0x02);
    }

    [Fact]
    public void WriteSnapshot_NoPattern_VersionIsFirstField()
    {
        var entities = new Dictionary<string, SnapshotEntity>
        {
            ["a/b"] = MakeEntity(5, 0, 50),
        };
        var nested = BuildNestedBucket(entities);

        var bytes = StreamingIndexCodec.WriteSnapshot(5, "", nested);

        // Without pattern, version (field 32, varint) should be first
        // tag = (32 << 3) | 0 = 256 → varint: 0x80 0x02
        bytes[0].Should().Be(0x80);
        bytes[1].Should().Be(0x02);
    }

    [Fact]
    public void WriteSnapshot_EmptySnapshot()
    {
        var bytes = StreamingIndexCodec.WriteSnapshot(0, "", null);
        bytes.Should().BeEmpty();
    }

    #endregion

    #region ── Snapshot Reader Tests ──

    [Fact]
    public void ReadSnapshot_CompressedNested_MatchesProtoBasedParsing()
    {
        var compressor = NameCompressor.FromPattern(Pattern)!;
        var entities = new Dictionary<string, SnapshotEntity>
        {
            ["abc/def/001"] = MakeEntity(10, 0, 100, 0xAA),
            ["abc/def/002"] = MakeEntity(10, 100, 200, 0xBB),
            ["abc/ghi/003"] = MakeEntity(10, 300, 150, 0xCC),
        };
        var nested = BuildNestedBucket(entities);

        // Write using streaming writer (id_pattern first)
        var bytes = StreamingIndexCodec.WriteSnapshot(10, compressor.Pattern, nested);

        // Read using streaming reader
        var result = StreamingIndexCodec.ReadSnapshot(bytes, compressor);

        result.Version.Should().Be(10);
        result.State.SnapshotVersion.Should().Be(10);
        result.State.Entities.Count.Should().Be(3);

        // Verify entities are accessible by full expanded names
        result.State.Entities.TryGetValue("t/abc/s/def/items/001/values", out var e1).Should().BeTrue();
        e1!.Position.Should().Be(0);
        e1.Length.Should().Be(100);
        e1.Crc32.Should().Be(0xAA);

        result.State.Entities.TryGetValue("t/abc/s/ghi/items/003/values", out var e3).Should().BeTrue();
        e3!.Position.Should().Be(300);
    }

    [Fact]
    public void ReadSnapshot_CompressedNested_MaxEndPosition()
    {
        var compressor = NameCompressor.FromPattern(Pattern)!;
        var entities = new Dictionary<string, SnapshotEntity>
        {
            ["abc/def/001"] = MakeEntity(10, 0, 100),
            ["abc/def/002"] = MakeEntity(10, 100, 200),
        };
        var nested = BuildNestedBucket(entities);
        var bytes = StreamingIndexCodec.WriteSnapshot(10, compressor.Pattern, nested);

        var result = StreamingIndexCodec.ReadSnapshot(bytes, compressor);

        result.MaxEndPosition.Should().Be(300); // 100 + 200
    }

    [Fact]
    public void ReadSnapshot_DeletedEntitiesExcluded()
    {
        var compressor = NameCompressor.FromPattern(Pattern)!;
        var entities = new Dictionary<string, SnapshotEntity>
        {
            ["abc/def/001"] = MakeEntity(10, 0, 100),
            ["abc/def/002"] = MakeDeletedEntity(8, 10, 100, 200), // deleted at v10
        };
        var nested = BuildNestedBucket(entities);
        var bytes = StreamingIndexCodec.WriteSnapshot(10, compressor.Pattern, nested);

        var result = StreamingIndexCodec.ReadSnapshot(bytes, compressor);

        result.State.Entities.Count.Should().Be(1);
        result.State.Entities.TryGetValue("t/abc/s/def/items/001/values", out _).Should().BeTrue();
        result.State.Entities.TryGetValue("t/abc/s/def/items/002/values", out _).Should().BeFalse();
    }

    [Fact]
    public void ReadSnapshot_AutoDetectsCompressorFromIdPattern()
    {
        var compressor = NameCompressor.FromPattern(Pattern)!;
        var entities = new Dictionary<string, SnapshotEntity>
        {
            ["abc/def/001"] = MakeEntity(10, 0, 100),
        };
        var nested = BuildNestedBucket(entities);
        var bytes = StreamingIndexCodec.WriteSnapshot(10, compressor.Pattern, nested);

        // Pass null compressor — should auto-detect from id_pattern in the file
        var result = StreamingIndexCodec.ReadSnapshot(bytes, null);

        result.State.Entities.Count.Should().Be(1);
        result.State.Entities.TryGetValue("t/abc/s/def/items/001/values", out _).Should().BeTrue();
    }

    [Fact]
    public void ReadSnapshot_ProtoWrittenBytes_MatchesStreamingReader()
    {
        // Write with proto's own serializer (id_pattern NOT first)
        var compressor = NameCompressor.FromPattern(Pattern)!;
        var entities = new Dictionary<string, SnapshotEntity>
        {
            ["abc/def/001"] = MakeEntity(10, 0, 100, 0xAA),
            ["xyz/ghi/002"] = MakeEntity(10, 100, 200, 0xBB),
        };
        var nested = BuildNestedBucket(entities);
        var protoBytes = WriteProtoSnapshot(10, Pattern, nested);

        // Read with streaming reader
        var result = StreamingIndexCodec.ReadSnapshot(protoBytes, compressor);

        result.State.Entities.Count.Should().Be(2);
        result.State.Entities.TryGetValue("t/abc/s/def/items/001/values", out var e1).Should().BeTrue();
        e1!.Crc32.Should().Be(0xAA);
        result.State.Entities.TryGetValue("t/xyz/s/ghi/items/002/values", out var e2).Should().BeTrue();
        e2!.Crc32.Should().Be(0xBB);
    }

    [Fact]
    public void ReadSnapshot_LegacyFlat_NoCompressor()
    {
        var entities = new Dictionary<string, SnapshotEntity>
        {
            ["t/abc/s/def/items/001/values"] = MakeEntity(5, 0, 50),
            ["t/abc/s/def/items/002/values"] = MakeEntity(5, 50, 60),
        };
        var bytes = WriteLegacyFlatSnapshot(5, entities);

        var result = StreamingIndexCodec.ReadSnapshot(bytes, null);

        result.Version.Should().Be(5);
        result.State.Entities.Count.Should().Be(2);
        result.State.Entities.TryGetValue("t/abc/s/def/items/001/values", out var e).Should().BeTrue();
        e!.Position.Should().Be(0);
    }

    [Fact]
    public void ReadSnapshot_LegacyFlat_WithCompressor()
    {
        var compressor = NameCompressor.FromPattern(Pattern)!;
        var entities = new Dictionary<string, SnapshotEntity>
        {
            ["t/abc/s/def/items/001/values"] = MakeEntity(5, 0, 50),
        };
        var bytes = WriteLegacyFlatSnapshot(5, entities);

        var result = StreamingIndexCodec.ReadSnapshot(bytes, compressor);

        // Legacy flat names should be imported and compressed into the trie
        result.State.Entities.Count.Should().Be(1);
        result.State.Entities.TryGetValue("t/abc/s/def/items/001/values", out _).Should().BeTrue();
    }

    [Fact]
    public void ReadSnapshot_VersionLegacyField()
    {
        // Write with legacy version field (field 1) only
        var proto = new SnapshotIndex { SnapshotVersionLegacy = 42 };
        proto.Entities["foo/bar"] = MakeEntity(42, 0, 10);
        var bytes = proto.ToByteArray();

        var result = StreamingIndexCodec.ReadSnapshot(bytes, null);

        result.VersionLegacy.Should().Be(42);
        result.State.SnapshotVersion.Should().Be(42);
    }

    [Fact]
    public void ReadSnapshot_EmptySnapshot()
    {
        var bytes = new SnapshotIndex().ToByteArray();
        var result = StreamingIndexCodec.ReadSnapshot(bytes, null);

        result.State.Entities.Count.Should().Be(0);
    }

    [Fact]
    public void ReadSnapshot_RoundTrip_StreamingWriterThenStreamingReader()
    {
        var compressor = NameCompressor.FromPattern(Pattern)!;
        var entities = new Dictionary<string, SnapshotEntity>
        {
            ["abc/def/001"] = MakeEntity(10, 0, 100, 0x11),
            ["abc/def/002"] = MakeEntity(10, 100, 200, 0x22),
            ["xyz/ghi/003"] = MakeEntity(10, 300, 300, 0x33),
        };
        var nested = BuildNestedBucket(entities);

        // Write → Read round trip
        var written = StreamingIndexCodec.WriteSnapshot(10, compressor.Pattern, nested);
        var result = StreamingIndexCodec.ReadSnapshot(written, compressor);

        result.Version.Should().Be(10);
        result.State.Entities.Count.Should().Be(3);
        result.MaxEndPosition.Should().Be(600); // 300 + 300

        // Verify all entities
        result.State.Entities.TryGetValue("t/abc/s/def/items/001/values", out var e1).Should().BeTrue();
        e1!.Crc32.Should().Be(0x11);
        result.State.Entities.TryGetValue("t/abc/s/def/items/002/values", out var e2).Should().BeTrue();
        e2!.Crc32.Should().Be(0x22);
        result.State.Entities.TryGetValue("t/xyz/s/ghi/items/003/values", out var e3).Should().BeTrue();
        e3!.Crc32.Should().Be(0x33);
    }

    #endregion

    #region ── Snapshot Position Trie Tests ──

    [Fact]
    public void ReadSnapshot_PositionLookup_Nested()
    {
        var compressor = NameCompressor.FromPattern(Pattern)!;
        var entities = new Dictionary<string, SnapshotEntity>
        {
            ["abc/def/001"] = MakeEntity(10, 0, 100),
            ["abc/def/002"] = MakeEntity(10, 100, 200),
            ["abc/def/003"] = MakeDeletedEntity(8, 10, 300, 50), // deleted, should be excluded
        };
        var nested = BuildNestedBucket(entities);
        var bytes = StreamingIndexCodec.WriteSnapshot(10, compressor.Pattern, nested);

        var result = StreamingIndexCodec.ReadSnapshot(bytes, compressor);

        result.State.SnapshotVersion.Should().Be(10);
        result.State.Entities.TryGetValue("t/abc/s/def/items/001/values", out var e1).Should().BeTrue();
        e1.Position.Should().Be(0);
        result.State.Entities.TryGetValue("t/abc/s/def/items/002/values", out var e2).Should().BeTrue();
        e2.Position.Should().Be(100);
        result.State.Entities.TryGetValue("t/abc/s/def/items/003/values", out _).Should().BeFalse();
    }

    [Fact]
    public void ReadSnapshot_PositionLookup_LegacyFlat()
    {
        var entities = new Dictionary<string, SnapshotEntity>
        {
            ["foo/bar"] = MakeEntity(5, 42, 10),
        };
        var bytes = WriteLegacyFlatSnapshot(5, entities);

        var result = StreamingIndexCodec.ReadSnapshot(bytes);

        result.State.SnapshotVersion.Should().Be(5);
        result.State.Entities.TryGetValue("foo/bar", out var entity).Should().BeTrue();
        entity.Position.Should().Be(42);
    }

    [Fact]
    public void ReadSnapshot_PositionLookup_MatchesProtoBasedState()
    {
        var compressor = NameCompressor.FromPattern(Pattern)!;
        var entities = new Dictionary<string, SnapshotEntity>
        {
            ["abc/def/001"] = MakeEntity(10, 0, 100),
            ["abc/def/002"] = MakeEntity(10, 100, 200),
        };
        var nested = BuildNestedBucket(entities);
        var protoBytes = WriteProtoSnapshot(10, Pattern, nested);

        // Proto-based state
        var proto = SnapshotIndex.Parser.ParseFrom(protoBytes);
        var protoState = ImmutableDatasetState.FromProto(proto, compressor);

        // Streaming state
        var streamingState = StreamingIndexCodec.ReadSnapshot(protoBytes, compressor).State;

        // Both should return the same positions
        protoState.Entities.TryGetValue("t/abc/s/def/items/001/values", out var p1a);
        streamingState.Entities.TryGetValue("t/abc/s/def/items/001/values", out var p1b);
        p1a.Position.Should().Be(p1b.Position);

        protoState.Entities.TryGetValue("t/abc/s/def/items/002/values", out var p2a);
        streamingState.Entities.TryGetValue("t/abc/s/def/items/002/values", out var p2b);
        p2a.Position.Should().Be(p2b.Position);
    }

    #endregion

    #region ── Delta Writer Tests ──

    [Fact]
    public void WriteDelta_IdPatternIsFirst()
    {
        var delta = new DeltaIndex { DeltaVersion = 10, IdPattern = "a/*/b" };
        delta.NewEntities.Add(new DeltaEntity { Name = "x", Position = 0, Length = 10 });

        var bytes = StreamingIndexCodec.WriteDelta(delta);

        // First byte should be the tag for field 34 (id_pattern), wire type 2 (LEN)
        // tag = (34 << 3) | 2 = 274 → varint: 0x92 0x02
        bytes[0].Should().Be(0x92);
        bytes[1].Should().Be(0x02);

        // Verify proto can still parse it
        var parsed = DeltaIndex.Parser.ParseFrom(bytes);
        parsed.DeltaVersion.Should().Be(10);
        parsed.IdPattern.Should().Be("a/*/b");
        parsed.NewEntities.Should().HaveCount(1);
    }

    [Fact]
    public void WriteDelta_NoPattern_NormalFieldOrder()
    {
        var delta = new DeltaIndex { DeltaVersion = 10 };
        delta.NewEntities.Add(new DeltaEntity { Name = "x", Position = 0, Length = 10 });

        var bytes = StreamingIndexCodec.WriteDelta(delta);

        // Without id_pattern, should NOT start with field 34
        var parsed = DeltaIndex.Parser.ParseFrom(bytes);
        parsed.DeltaVersion.Should().Be(10);
        parsed.IdPattern.Should().BeEmpty();
    }

    [Fact]
    public void WriteDelta_OriginalUnmodified()
    {
        var delta = new DeltaIndex { DeltaVersion = 10, IdPattern = "a/*/b" };
        delta.NewEntities.Add(new DeltaEntity { Name = "x", Position = 0, Length = 10 });

        StreamingIndexCodec.WriteDelta(delta);

        // Original delta should still have its IdPattern
        delta.IdPattern.Should().Be("a/*/b");
    }

    #endregion

    #region ── Packed / Unpacked Repeated Fields ──

    [Fact]
    public void ReadSnapshot_PackedRepeatedVersionArrays()
    {
        // SnapshotEntity with multiple inserted versions (packed encoding)
        var entity = new SnapshotEntity { Position = 0, Length = 10 };
        entity.LastInsertedVersion.Add(5);
        entity.LastInsertedVersion.Add(8);
        entity.LastInsertedVersion.Add(12);

        var entities = new Dictionary<string, SnapshotEntity> { ["a/b/c"] = entity };
        var nested = BuildNestedBucket(entities);
        var bytes = StreamingIndexCodec.WriteSnapshot(12, "", nested);

        var result = StreamingIndexCodec.ReadSnapshot(bytes, null);

        result.State.Entities.Count.Should().Be(1);
        result.State.Entities.TryGetValue("a/b/c", out var e).Should().BeTrue();
        e!.LatestVersion.Should().Be(12);
    }

    [Fact]
    public void ReadSnapshot_MultipleDeletedVersions_CorrectlyIdentifiesDeleted()
    {
        var entity = new SnapshotEntity { Position = 0, Length = 10 };
        entity.LastInsertedVersion.Add(5);
        entity.LastDeletedVersion.Add(8);
        entity.LastInsertedVersion.Add(12);
        entity.LastDeletedVersion.Add(15); // deleted at v15

        var entities = new Dictionary<string, SnapshotEntity> { ["a/b"] = entity };
        var nested = BuildNestedBucket(entities);
        var bytes = StreamingIndexCodec.WriteSnapshot(15, "", nested);

        var result = StreamingIndexCodec.ReadSnapshot(bytes, null);

        // Entity with max deleted 15 == max overall 15 → deleted
        result.State.Entities.Count.Should().Be(0);
    }

    [Fact]
    public void ReadSnapshot_MultipleDeletedVersions_NotDeletedWhenReinserted()
    {
        var entity = new SnapshotEntity { Position = 0, Length = 10 };
        entity.LastInsertedVersion.Add(5);
        entity.LastDeletedVersion.Add(8);
        entity.LastInsertedVersion.Add(15); // reinserted at v15

        var entities = new Dictionary<string, SnapshotEntity> { ["a/b"] = entity };
        var nested = BuildNestedBucket(entities);
        var bytes = StreamingIndexCodec.WriteSnapshot(15, "", nested);

        var result = StreamingIndexCodec.ReadSnapshot(bytes, null);

        // max inserted = 15 > max deleted = 8 → alive
        result.State.Entities.Count.Should().Be(1);
    }

    #endregion

    #region ── Cross-validation: Streaming vs Proto-based ──

    [Fact]
    public void ReadSnapshot_StreamingMatchesProtoBased_ForAllEntities()
    {
        var compressor = NameCompressor.FromPattern(Pattern)!;
        var entities = new Dictionary<string, SnapshotEntity>
        {
            ["abc/def/001"] = MakeEntity(10, 0, 100, 0x11),
            ["abc/def/002"] = MakeEntity(10, 100, 200, 0x22),
            ["abc/ghi/003"] = MakeEntity(10, 300, 150, 0x33),
            ["xyz/jkl/004"] = MakeEntity(10, 450, 50, 0x44),
        };
        var nested = BuildNestedBucket(entities);

        // Use proto's own serializer
        var protoBytes = WriteProtoSnapshot(10, Pattern, nested);

        // Proto-based path
        var proto = SnapshotIndex.Parser.ParseFrom(protoBytes);
        var protoState = ImmutableDatasetState.FromProto(proto, compressor);

        // Streaming path
        var streamResult = StreamingIndexCodec.ReadSnapshot(protoBytes, compressor);
        var streamState = streamResult.State;

        // Compare
        streamState.SnapshotVersion.Should().Be(protoState.SnapshotVersion);
        streamState.Entities.Count.Should().Be(protoState.Entities.Count);

        foreach (var (name, protoEntity) in protoState.Entities)
        {
            streamState.Entities.TryGetValue(name, out var streamEntity).Should().BeTrue(
                $"entity '{name}' should exist in streaming result");
            streamEntity!.Position.Should().Be(protoEntity.Position);
            streamEntity.Length.Should().Be(protoEntity.Length);
            streamEntity.Crc32.Should().Be(protoEntity.Crc32);
            streamEntity.LatestVersion.Should().Be(protoEntity.LatestVersion);
        }
    }

    #endregion

    #region ── Bucket-by-Bucket Scanning Tests ──

    [Fact]
    public void ScanLeafBuckets_FindsOneLeafBucketPerStore_WithCompressedNames()
    {
        // With pattern t/*/s/*/items/*/values, compressed names are <tenant>/<store>/<itemid>
        // BuildNestedBucket creates: root → tenant1 → store1 (leaf bucket with items as leaves)
        var entities = new Dictionary<string, SnapshotEntity>
        {
            ["tenant1/store1/0001"] = MakeEntity(1, 0, 100),
            ["tenant1/store1/0002"] = MakeEntity(1, 100, 100),
            ["tenant1/store2/0001"] = MakeEntity(1, 200, 100),
            ["tenant1/store2/0002"] = MakeEntity(1, 300, 100),
            ["tenant1/store3/0001"] = MakeEntity(1, 400, 100),
        };
        var nested = BuildNestedBucket(entities);
        byte[] data = WriteProtoSnapshot(1, Pattern, nested);

        var (nestedOffset, nestedLength, buckets) = StreamingIndexCodec.ScanLeafBuckets(data);

        nestedOffset.Should().BeGreaterThan(0);
        nestedLength.Should().BeGreaterThan(0);

        // 3 stores → 3 leaf buckets
        buckets.Should().HaveCount(3);
        buckets.Select(b => b.Prefix).Should().BeEquivalentTo(
            ["tenant1/store1", "tenant1/store2", "tenant1/store3"]);

        // All buckets should have valid byte ranges within the nested span
        foreach (var bucket in buckets)
        {
            bucket.BucketOffset.Should().BeGreaterOrEqualTo(nestedOffset);
            bucket.BucketLength.Should().BeGreaterThan(0);
            (bucket.BucketOffset + bucket.BucketLength).Should().BeLessOrEqualTo(nestedOffset + nestedLength);
        }
    }

    [Fact]
    public void ScanLeafBuckets_ReturnsEmptyForLegacyFlatSnapshot()
    {
        var entities = new Dictionary<string, SnapshotEntity>
        {
            ["entity1"] = MakeEntity(1, 0, 100),
        };
        byte[] data = WriteLegacyFlatSnapshot(1, entities);

        var (nestedOffset, nestedLength, buckets) = StreamingIndexCodec.ScanLeafBuckets(data);

        nestedOffset.Should().Be(0);
        nestedLength.Should().Be(0);
        buckets.Should().BeEmpty();
    }

    [Fact]
    public void ReadBucketProtoEntities_ReturnsFullProtoEntitiesWithVersionArrays()
    {
        // Compressed names: <tenant>/<store>/<itemid>
        var entities = new Dictionary<string, SnapshotEntity>
        {
            ["t1/store1/0001"] = MakeEntity(1, 0, 100, 111),
            ["t1/store1/0002"] = MakeEntity(2, 100, 200, 222),
            ["t1/store2/0001"] = MakeEntity(3, 300, 300, 333),
        };
        var nested = BuildNestedBucket(entities);
        byte[] data = WriteProtoSnapshot(1, Pattern, nested);

        var (_, _, buckets) = StreamingIndexCodec.ScanLeafBuckets(data);
        buckets.Should().HaveCount(2); // store1, store2

        // Parse all leaf buckets and collect their entities
        var allParsed = new Dictionary<string, SnapshotEntity>();
        foreach (var bucket in buckets)
        {
            var bucketEntities = StreamingIndexCodec.ReadBucketProtoEntities(data, bucket);
            foreach (var (key, entity) in bucketEntities)
            {
                var fullName = bucket.Prefix.Length > 0 ? $"{bucket.Prefix}/{key}" : key;
                allParsed[fullName] = entity;
            }
        }

        allParsed.Should().HaveCount(3);
        allParsed.Keys.Should().BeEquivalentTo(entities.Keys);

        foreach (var (name, expected) in entities)
        {
            var actual = allParsed[name];
            actual.Position.Should().Be(expected.Position);
            actual.Length.Should().Be(expected.Length);
            actual.Crc32.Should().Be(expected.Crc32);
            actual.LastInsertedVersion.Should().BeEquivalentTo(expected.LastInsertedVersion);
        }
    }

    [Fact]
    public void ReadBucketProtoEntities_PreservesDeletedVersionArrays()
    {
        // Compressed: <tenant>/<store>/<itemid>
        var entities = new Dictionary<string, SnapshotEntity>
        {
            ["t1/s1/001"] = MakeEntity(1, 0, 100),
            ["t1/s1/002"] = MakeDeletedEntity(1, 5, 100, 200),
        };
        var nested = BuildNestedBucket(entities);
        byte[] data = WriteProtoSnapshot(1, Pattern, nested);

        var (_, _, buckets) = StreamingIndexCodec.ScanLeafBuckets(data);
        buckets.Should().HaveCount(1); // single store "s1"

        var bucketEntities = StreamingIndexCodec.ReadBucketProtoEntities(data, buckets[0]);
        bucketEntities.Should().HaveCount(2);
        bucketEntities["002"].LastDeletedVersion.Should().Contain(5UL);
        bucketEntities["002"].LastInsertedVersion.Should().Contain(1UL);
    }

    [Fact]
    public void ScanLeafBuckets_LargerDataset_MatchesStoreCount()
    {
        // Simulate compressed names: 1 tenant, 10 stores, 50 items each = 500 entities
        var entities = new Dictionary<string, SnapshotEntity>();
        int pos = 0;
        for (int store = 0; store < 10; store++)
        {
            for (int item = 0; item < 50; item++)
            {
                var name = $"tenant1/store{store:D4}/{item:D10}";
                entities[name] = MakeEntity(1, pos, 100);
                pos += 100;
            }
        }

        var nested = BuildNestedBucket(entities);
        byte[] data = WriteProtoSnapshot(1, Pattern, nested);

        var (_, _, buckets) = StreamingIndexCodec.ScanLeafBuckets(data);

        // 10 stores → 10 leaf buckets, each with 50 items
        buckets.Should().HaveCount(10);

        // Round-trip: collect all entities from all leaf buckets
        var allParsed = new Dictionary<string, SnapshotEntity>();
        foreach (var bucket in buckets)
        {
            var bucketEntities = StreamingIndexCodec.ReadBucketProtoEntities(data, bucket);
            bucketEntities.Should().HaveCount(50, $"bucket '{bucket.Prefix}' should have 50 items");
            foreach (var (key, entity) in bucketEntities)
            {
                var fullName = $"{bucket.Prefix}/{key}";
                allParsed[fullName] = entity;
            }
        }

        allParsed.Should().HaveCount(500);
        allParsed.Keys.Should().BeEquivalentTo(entities.Keys);
    }

    #endregion
}
