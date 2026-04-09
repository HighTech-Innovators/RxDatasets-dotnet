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

    #region ── Position Lookup Tests ──

    [Fact]
    public void ReadSnapshotPositionLookup_Nested()
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

        var lookup = StreamingIndexCodec.ReadSnapshotPositionLookup(bytes);

        lookup.SnapshotVersion.Should().Be(10);
        lookup.TryGetPosition("t/abc/s/def/items/001/values", out var pos1).Should().BeTrue();
        pos1.Should().Be(0);
        lookup.TryGetPosition("t/abc/s/def/items/002/values", out var pos2).Should().BeTrue();
        pos2.Should().Be(100);
        lookup.TryGetPosition("t/abc/s/def/items/003/values", out _).Should().BeFalse();
    }

    [Fact]
    public void ReadSnapshotPositionLookup_LegacyFlat()
    {
        var entities = new Dictionary<string, SnapshotEntity>
        {
            ["foo/bar"] = MakeEntity(5, 42, 10),
        };
        var bytes = WriteLegacyFlatSnapshot(5, entities);

        var lookup = StreamingIndexCodec.ReadSnapshotPositionLookup(bytes);

        lookup.SnapshotVersion.Should().Be(5);
        lookup.TryGetPosition("foo/bar", out var pos).Should().BeTrue();
        pos.Should().Be(42);
    }

    [Fact]
    public void ReadSnapshotPositionLookup_ProtoWritten_MatchesProtoBasedLookup()
    {
        var compressor = NameCompressor.FromPattern(Pattern)!;
        var entities = new Dictionary<string, SnapshotEntity>
        {
            ["abc/def/001"] = MakeEntity(10, 0, 100),
            ["abc/def/002"] = MakeEntity(10, 100, 200),
        };
        var nested = BuildNestedBucket(entities);
        var protoBytes = WriteProtoSnapshot(10, Pattern, nested);

        // Proto-based lookup
        var proto = SnapshotIndex.Parser.ParseFrom(protoBytes);
        var protoLookup = SnapshotPositionLookup.FromProto(proto);

        // Streaming lookup
        var streamingLookup = StreamingIndexCodec.ReadSnapshotPositionLookup(protoBytes);

        // Both should return the same positions
        protoLookup.TryGetPosition("t/abc/s/def/items/001/values", out var p1a);
        streamingLookup.TryGetPosition("t/abc/s/def/items/001/values", out var p1b);
        p1a.Should().Be(p1b);

        protoLookup.TryGetPosition("t/abc/s/def/items/002/values", out var p2a);
        streamingLookup.TryGetPosition("t/abc/s/def/items/002/values", out var p2b);
        p2a.Should().Be(p2b);
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
}
