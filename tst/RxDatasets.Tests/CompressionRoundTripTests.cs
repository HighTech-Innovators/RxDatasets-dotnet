using FluentAssertions;
using Google.Protobuf;
using RxDatasets.Models;

namespace RxDatasets.Tests;

/// <summary>
/// End-to-end round-trip tests that serialize index data to bytes, inspect the raw proto
/// structure (IdPattern, Nested tree shape, flat fields), then parse and load into
/// ImmutableDatasetState — covering every combination of compression/no-compression
/// for snapshots and deltas.
/// </summary>
public class CompressionRoundTripTests
{
    #region Helpers

    /// <summary>
    /// Simulates the snapshot write path: takes full entity names, compresses if compressor
    /// is provided, builds nested SnapshotBucket, sets IdPattern. Returns raw bytes.
    /// </summary>
    private static byte[] WriteSnapshotBytes(
        ulong version, NameCompressor? compressor, params (string fullName, long position, int length, uint crc32)[] entities)
    {
        var protoEntities = new Dictionary<string, SnapshotEntity>();
        foreach (var (fullName, position, length, crc32) in entities)
        {
            var key = compressor != null ? compressor.Compress(fullName) : fullName;
            var entity = new SnapshotEntity { Position = position, Length = length, Crc32 = crc32 };
            entity.LastInsertedVersion.Add(version);
            protoEntities[key] = entity;
        }

        var snapshot = new SnapshotIndex
        {
            SnapshotVersion = version,
            Nested = BuildNestedBucket(protoEntities),
            IdPattern = compressor?.Pattern ?? ""
        };
        return snapshot.ToByteArray();
    }

    /// <summary>
    /// Writes a legacy flat snapshot (no Nested field) to bytes.
    /// </summary>
    private static byte[] WriteLegacyFlatSnapshotBytes(
        ulong version, params (string name, long position, int length, uint crc32)[] entities)
    {
        var snapshot = new SnapshotIndex { SnapshotVersion = version };
        foreach (var (name, position, length, crc32) in entities)
        {
            var entity = new SnapshotEntity { Position = position, Length = length, Crc32 = crc32 };
            entity.LastInsertedVersion.Add(version);
            snapshot.Entities[name] = entity;
        }
        return snapshot.ToByteArray();
    }

    /// <summary>
    /// Writes a delta via SerializeDeltaIndex (produces nested format, optionally compressed).
    /// </summary>
    private static byte[] WriteDeltaBytes(
        ulong version, NameCompressor? compressor,
        (string name, long position, int length, uint crc32)[]? newEntities = null,
        (string name, long position, int length, uint crc32)[]? changedEntities = null,
        string[]? deletedNames = null)
    {
        var delta = new DeltaIndex { DeltaVersion = version };
        foreach (var (name, position, length, crc32) in newEntities ?? [])
            delta.NewEntities.Add(new DeltaEntity { Name = name, Position = position, Length = length, Crc32 = crc32 });
        foreach (var (name, position, length, crc32) in changedEntities ?? [])
            delta.ChangedEntities.Add(new DeltaEntity { Name = name, Position = position, Length = length, Crc32 = crc32 });
        foreach (var name in deletedNames ?? [])
            delta.DeletedNames.Add(name);
        return NestedIndexHelper.SerializeDeltaIndex(delta, compressor);
    }

    /// <summary>
    /// Writes a legacy flat delta (no nesting) to bytes.
    /// </summary>
    private static byte[] WriteLegacyFlatDeltaBytes(
        ulong version,
        (string name, long position, int length, uint crc32)[]? newEntities = null,
        string[]? deletedNames = null)
    {
        var delta = new DeltaIndex { DeltaVersion = version };
        foreach (var (name, position, length, crc32) in newEntities ?? [])
            delta.NewEntities.Add(new DeltaEntity { Name = name, Position = position, Length = length, Crc32 = crc32 });
        foreach (var name in deletedNames ?? [])
            delta.DeletedNames.Add(name);
        // Serialize directly without nesting
        return delta.ToByteArray();
    }

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

    /// <summary>
    /// Collects all leaf names from a SnapshotBucket tree (for structural inspection).
    /// </summary>
    private static List<string> CollectNestedLeafNames(SnapshotBucket bucket, string prefix = "")
    {
        var result = new List<string>();
        foreach (var (segment, _) in bucket.Leaves)
            result.Add(prefix.Length == 0 ? segment : prefix + "/" + segment);
        foreach (var (segment, child) in bucket.Children)
            result.Add(prefix.Length == 0 ? segment : prefix + "/" + segment);
        foreach (var (segment, child) in bucket.Children)
        {
            var childPrefix = prefix.Length == 0 ? segment : prefix + "/" + segment;
            result.AddRange(CollectNestedLeafNames(child, childPrefix));
        }
        return result;
    }

    /// <summary>
    /// Collects all entity full names from a SnapshotBucket tree by flattening.
    /// </summary>
    private static List<string> FlattenNestedNames(SnapshotBucket bucket, string prefix = "")
    {
        var result = new List<string>();
        foreach (var (segment, _) in bucket.Leaves)
            result.Add(prefix.Length == 0 ? segment : prefix + "/" + segment);
        foreach (var (segment, child) in bucket.Children)
        {
            var childPrefix = prefix.Length == 0 ? segment : prefix + "/" + segment;
            result.AddRange(FlattenNestedNames(child, childPrefix));
        }
        return result;
    }

    /// <summary>
    /// Collects all entity full names from a DeltaBucket tree by flattening.
    /// </summary>
    private static List<string> FlattenDeltaNestedNames(DeltaBucket bucket, string prefix = "")
    {
        var result = new List<string>();
        foreach (var e in bucket.NewEntities)
            result.Add(prefix.Length == 0 ? e.Name : prefix + "/" + e.Name);
        foreach (var e in bucket.ChangedEntities)
            result.Add(prefix.Length == 0 ? e.Name : prefix + "/" + e.Name);
        foreach (var e in bucket.DeletedEntities)
            result.Add(prefix.Length == 0 ? e.Name : prefix + "/" + e.Name);
        foreach (var name in bucket.DeletedNames)
            result.Add(prefix.Length == 0 ? name : prefix + "/" + name);
        foreach (var (segment, child) in bucket.Children)
        {
            var childPrefix = prefix.Length == 0 ? segment : prefix + "/" + segment;
            result.AddRange(FlattenDeltaNestedNames(child, childPrefix));
        }
        return result;
    }

    #endregion

    #region Snapshot proto structure inspection

    [Fact]
    public void CompressedSnapshot_ProtoStructure_HasCompressedTreeAndIdPattern()
    {
        var compressor = NameCompressor.FromPattern("t/{tenantid}/s/{storeid}/items/{itemid}/values")!;
        var bytes = WriteSnapshotBytes(5, compressor,
            ("t/abc/s/def/items/001/values", 0, 10, 1),
            ("t/abc/s/def/items/002/values", 10, 20, 2),
            ("t/abc/s/ghi/items/003/values", 30, 30, 3));

        // Inspect raw proto without any expansion
        var raw = SnapshotIndex.Parser.ParseFrom(bytes);

        raw.IdPattern.Should().Be("t/{tenantid}/s/{storeid}/items/{itemid}/values");
        raw.Entities.Should().BeEmpty("compressed snapshots use Nested, not flat Entities");
        raw.Nested.Should().NotBeNull();

        // The nested tree should contain only variable segments (abc, def, 001, etc.)
        // NOT the fixed segments (t, s, items, values)
        var flatNames = FlattenNestedNames(raw.Nested);
        flatNames.Should().BeEquivalentTo(["abc/def/001", "abc/def/002", "abc/ghi/003"]);

        // Verify tree shape: root → "abc" → {"def" → leaves "001","002", "ghi" → leaf "003"}
        raw.Nested.Children.Should().ContainKey("abc");
        var abcNode = raw.Nested.Children["abc"];
        abcNode.Children.Should().ContainKey("def");
        abcNode.Children.Should().ContainKey("ghi");
        abcNode.Children["def"].Leaves.Should().ContainKey("001");
        abcNode.Children["def"].Leaves.Should().ContainKey("002");
        abcNode.Children["ghi"].Leaves.Should().ContainKey("003");
    }

    [Fact]
    public void UncompressedNestedSnapshot_ProtoStructure_HasFullNameTreeAndNoIdPattern()
    {
        var bytes = WriteSnapshotBytes(5, null,
            ("t/abc/items/001/values", 0, 10, 1),
            ("t/abc/items/002/values", 10, 20, 2));

        var raw = SnapshotIndex.Parser.ParseFrom(bytes);

        raw.IdPattern.Should().BeEmpty();
        raw.Entities.Should().BeEmpty("nested snapshots don't populate flat map");
        raw.Nested.Should().NotBeNull();

        // Full-name segments in the tree
        var flatNames = FlattenNestedNames(raw.Nested);
        flatNames.Should().BeEquivalentTo(["t/abc/items/001/values", "t/abc/items/002/values"]);

        // Tree has the fixed segments as nodes
        raw.Nested.Children.Should().ContainKey("t");
        raw.Nested.Children["t"].Children.Should().ContainKey("abc");
    }

    [Fact]
    public void LegacyFlatSnapshot_ProtoStructure_HasFlatEntitiesAndNoNested()
    {
        var bytes = WriteLegacyFlatSnapshotBytes(5,
            ("items/a", 0, 10, 1),
            ("items/b", 10, 20, 2));

        var raw = SnapshotIndex.Parser.ParseFrom(bytes);

        raw.IdPattern.Should().BeEmpty();
        raw.Entities.Should().HaveCount(2);
        raw.Entities.Should().ContainKey("items/a");
        raw.Entities.Should().ContainKey("items/b");
        // Nested should not be populated
        (raw.Nested == null || (raw.Nested.Children.Count == 0 && raw.Nested.Leaves.Count == 0))
            .Should().BeTrue("legacy flat snapshots don't have nested structure");
    }

    #endregion

    #region Delta proto structure inspection

    [Fact]
    public void CompressedDelta_ProtoStructure_HasCompressedTreeAndIdPattern()
    {
        var compressor = NameCompressor.FromPattern("t/*/items/*/values")!;
        var bytes = WriteDeltaBytes(10, compressor,
            newEntities: [("t/abc/items/001/values", 0, 10, 1)],
            changedEntities: [("t/abc/items/002/values", 10, 20, 2)],
            deletedNames: ["t/abc/items/003/values"]);

        // Parse RAW proto (no expansion)
        var raw = DeltaIndex.Parser.ParseFrom(bytes);

        raw.IdPattern.Should().Be("t/*/items/*/values");
        raw.NewEntities.Should().BeEmpty("SerializeDeltaIndex moves entities into Nested");
        raw.ChangedEntities.Should().BeEmpty();
        raw.DeletedNames.Should().BeEmpty();
        raw.Nested.Should().NotBeNull();

        // Flattened nested names should be compressed
        var flatNames = FlattenDeltaNestedNames(raw.Nested);
        flatNames.Should().BeEquivalentTo(["abc/001", "abc/002", "abc/003"]);
    }

    [Fact]
    public void UncompressedNestedDelta_ProtoStructure_HasFullNameTreeAndNoIdPattern()
    {
        var bytes = WriteDeltaBytes(10, null,
            newEntities: [("t/abc/items/001/values", 0, 10, 1)]);

        var raw = DeltaIndex.Parser.ParseFrom(bytes);

        raw.IdPattern.Should().BeEmpty();
        raw.Nested.Should().NotBeNull();
        raw.NewEntities.Should().BeEmpty("SerializeDeltaIndex moves entities into Nested");

        var flatNames = FlattenDeltaNestedNames(raw.Nested);
        flatNames.Should().BeEquivalentTo(["t/abc/items/001/values"]);
    }

    [Fact]
    public void LegacyFlatDelta_ProtoStructure_HasFlatFieldsAndNoNested()
    {
        var bytes = WriteLegacyFlatDeltaBytes(10,
            newEntities: [("items/a", 0, 10, 1)],
            deletedNames: ["items/b"]);

        var raw = DeltaIndex.Parser.ParseFrom(bytes);

        raw.IdPattern.Should().BeEmpty();
        raw.NewEntities.Should().HaveCount(1);
        raw.NewEntities[0].Name.Should().Be("items/a");
        raw.DeletedNames.Should().ContainSingle().Which.Should().Be("items/b");
        (raw.Nested == null || (raw.Nested.Children.Count == 0
            && raw.Nested.NewEntities.Count == 0 && raw.Nested.ChangedEntities.Count == 0
            && raw.Nested.DeletedEntities.Count == 0 && raw.Nested.DeletedNames.Count == 0))
            .Should().BeTrue("legacy deltas don't have nested structure");
    }

    #endregion

    #region Snapshot loading round-trips (bytes → parse → FromProto → verify full names)

    [Fact]
    public void CompressedSnapshot_RoundTrip_SameCompressor()
    {
        var compressor = NameCompressor.FromPattern("t/*/s/*/items/*/values")!;
        var bytes = WriteSnapshotBytes(5, compressor,
            ("t/abc/s/def/items/001/values", 0, 10, 1),
            ("t/abc/s/ghi/items/002/values", 10, 20, 2));

        var parsed = SnapshotIndex.Parser.ParseFrom(bytes);
        parsed.IdPattern.Should().Be(compressor.Pattern);

        var state = ImmutableDatasetState.FromProto(parsed, compressor);

        state.SnapshotVersion.Should().Be(5);
        state.Entities.Count.Should().Be(2);
        state.Entities["t/abc/s/def/items/001/values"].Should()
            .Be(new IndexEntity(5, 0, 10, 1));
        state.Entities["t/abc/s/ghi/items/002/values"].Should()
            .Be(new IndexEntity(5, 10, 20, 2));
    }

    [Fact]
    public void CompressedSnapshot_RoundTrip_AutoDetectCompressor()
    {
        var compressor = NameCompressor.FromPattern("t/*/items/*/values")!;
        var bytes = WriteSnapshotBytes(8, compressor,
            ("t/abc/items/001/values", 0, 10, 42));

        var parsed = SnapshotIndex.Parser.ParseFrom(bytes);

        // No compressor passed — auto-detect from IdPattern
        var state = ImmutableDatasetState.FromProto(parsed);

        state.Entities.Count.Should().Be(1);
        state.Entities.ContainsKey("t/abc/items/001/values").Should().BeTrue();
        state.Entities["t/abc/items/001/values"].Crc32.Should().Be(42u);
    }

    [Fact]
    public void CompressedSnapshot_RoundTrip_DifferentCompressor()
    {
        // Written with pattern A
        var patternA = NameCompressor.FromPattern("t/*/items/*/values")!;
        var bytes = WriteSnapshotBytes(5, patternA,
            ("t/abc/items/001/values", 0, 10, 1),
            ("t/def/items/002/values", 10, 20, 2));

        var parsed = SnapshotIndex.Parser.ParseFrom(bytes);
        parsed.IdPattern.Should().Be(patternA.Pattern);

        // Inspect: compressed names in nested tree
        FlattenNestedNames(parsed.Nested).Should().BeEquivalentTo(["abc/001", "def/002"]);

        // Read with pattern B — transition path: expand A, re-compress with B
        var patternB = NameCompressor.FromPattern("region/*/store/*/data")!;
        var state = ImmutableDatasetState.FromProto(parsed, patternB);

        state.Entities.Count.Should().Be(2);
        // Entities have full names (expanded from pattern A)
        state.Entities.ContainsKey("t/abc/items/001/values").Should().BeTrue();
        state.Entities.ContainsKey("t/def/items/002/values").Should().BeTrue();
        state.Entities["t/abc/items/001/values"].Should().Be(new IndexEntity(5, 0, 10, 1));
    }

    [Fact]
    public void UncompressedSnapshot_RoundTrip_WithNewCompressor()
    {
        // Written without compression
        var bytes = WriteSnapshotBytes(5, null,
            ("t/abc/items/001/values", 0, 10, 1),
            ("t/abc/items/002/values", 10, 20, 2));

        var parsed = SnapshotIndex.Parser.ParseFrom(bytes);
        parsed.IdPattern.Should().BeEmpty();

        // Read with a compressor (transition: uncompressed → new compressor)
        var compressor = NameCompressor.FromPattern("t/*/items/*/values")!;
        var state = ImmutableDatasetState.FromProto(parsed, compressor);

        state.Entities.Count.Should().Be(2);
        state.Entities.ContainsKey("t/abc/items/001/values").Should().BeTrue();
        state.Entities.ContainsKey("t/abc/items/002/values").Should().BeTrue();
    }

    [Fact]
    public void UncompressedSnapshot_RoundTrip_NoCompressor()
    {
        var bytes = WriteSnapshotBytes(5, null,
            ("channels/123/events/1", 0, 10, 1),
            ("channels/456/events/2", 10, 20, 2));

        var parsed = SnapshotIndex.Parser.ParseFrom(bytes);
        var state = ImmutableDatasetState.FromProto(parsed);

        state.Entities.Count.Should().Be(2);
        state.Entities.ContainsKey("channels/123/events/1").Should().BeTrue();
        state.Entities.ContainsKey("channels/456/events/2").Should().BeTrue();
    }

    [Fact]
    public void LegacyFlatSnapshot_RoundTrip_WithCompressor()
    {
        // Legacy flat snapshot with full names
        var bytes = WriteLegacyFlatSnapshotBytes(5,
            ("t/abc/items/001/values", 0, 10, 1),
            ("t/abc/items/002/values", 10, 20, 2));

        var parsed = SnapshotIndex.Parser.ParseFrom(bytes);
        parsed.IdPattern.Should().BeEmpty();
        parsed.Entities.Should().HaveCount(2);

        // Read with a compressor — flat legacy path, but compressor compresses in builder
        var compressor = NameCompressor.FromPattern("t/*/items/*/values")!;
        var state = ImmutableDatasetState.FromProto(parsed, compressor);

        state.Entities.Count.Should().Be(2);
        state.Entities.ContainsKey("t/abc/items/001/values").Should().BeTrue();
        state.Entities.ContainsKey("t/abc/items/002/values").Should().BeTrue();
    }

    [Fact]
    public void LegacyFlatSnapshot_RoundTrip_NoCompressor()
    {
        var bytes = WriteLegacyFlatSnapshotBytes(5,
            ("data/a", 0, 10, 1));

        var parsed = SnapshotIndex.Parser.ParseFrom(bytes);
        var state = ImmutableDatasetState.FromProto(parsed);

        state.Entities.Count.Should().Be(1);
        state.Entities["data/a"].Should().Be(new IndexEntity(5, 0, 10, 1));
    }

    #endregion

    #region Delta loading round-trips (bytes → ParseDeltaIndex → verify flat + nested)

    [Fact]
    public void CompressedDelta_RoundTrip_ExpandedFlatAndPreservedNested()
    {
        var compressor = NameCompressor.FromPattern("t/*/items/*/values")!;
        var bytes = WriteDeltaBytes(10, compressor,
            newEntities: [("t/abc/items/001/values", 0, 10, 1)],
            changedEntities: [("t/abc/items/002/values", 10, 20, 2)],
            deletedNames: ["t/abc/items/003/values"]);

        var parsed = NestedIndexHelper.ParseDeltaIndex(bytes);

        // Flat fields should have EXPANDED (full) names
        parsed.IdPattern.Should().Be(compressor.Pattern);
        parsed.NewEntities.Should().HaveCount(1);
        parsed.NewEntities[0].Name.Should().Be("t/abc/items/001/values");
        parsed.ChangedEntities[0].Name.Should().Be("t/abc/items/002/values");
        parsed.DeletedNames[0].Should().Be("t/abc/items/003/values");

        // Nested should be PRESERVED with raw (compressed) proto names
        parsed.Nested.Should().NotBeNull();
        var nestedNames = FlattenDeltaNestedNames(parsed.Nested!);
        nestedNames.Should().BeEquivalentTo(["abc/001", "abc/002", "abc/003"]);
    }

    [Fact]
    public void UncompressedDelta_RoundTrip_FlatNamesAndPreservedNested()
    {
        var bytes = WriteDeltaBytes(10, null,
            newEntities: [("a/b/c", 0, 10, 1)],
            deletedNames: ["x/y/z"]);

        var parsed = NestedIndexHelper.ParseDeltaIndex(bytes);

        parsed.IdPattern.Should().BeEmpty();
        parsed.NewEntities[0].Name.Should().Be("a/b/c");
        parsed.DeletedNames[0].Should().Be("x/y/z");

        // Nested preserved with full-name segments
        parsed.Nested.Should().NotBeNull();
        var nestedNames = FlattenDeltaNestedNames(parsed.Nested!);
        nestedNames.Should().BeEquivalentTo(["a/b/c", "x/y/z"]);
    }

    [Fact]
    public void LegacyFlatDelta_RoundTrip_NoNested()
    {
        var bytes = WriteLegacyFlatDeltaBytes(10,
            newEntities: [("items/a", 0, 10, 1)],
            deletedNames: ["items/b"]);

        var parsed = NestedIndexHelper.ParseDeltaIndex(bytes);

        parsed.NewEntities[0].Name.Should().Be("items/a");
        parsed.DeletedNames[0].Should().Be("items/b");
        // Legacy flat deltas have no Nested (or it's the empty restored bucket)
        // In any case, CanUseNestedMerge should return false for these
        parsed.IdPattern.Should().BeEmpty();
    }

    #endregion

    #region Cross-format application: snapshot + delta → bytes → parse → apply → verify

    [Fact]
    public void CompressedSnapshot_CompressedDelta_SamePattern_EndToEnd()
    {
        var compressor = NameCompressor.FromPattern("t/*/items/*/values")!;

        var snapshotBytes = WriteSnapshotBytes(5, compressor,
            ("t/abc/items/001/values", 0, 10, 1),
            ("t/abc/items/002/values", 10, 20, 2));

        var deltaBytes = WriteDeltaBytes(6, compressor,
            newEntities: [("t/abc/items/003/values", 30, 30, 3)],
            changedEntities: [("t/abc/items/001/values", 40, 15, 11)],
            deletedNames: ["t/abc/items/002/values"]);

        // Parse both from bytes
        var snapshot = SnapshotIndex.Parser.ParseFrom(snapshotBytes);
        var delta = NestedIndexHelper.ParseDeltaIndex(deltaBytes);

        // Both should have same IdPattern
        snapshot.IdPattern.Should().Be(compressor.Pattern);
        delta.IdPattern.Should().Be(compressor.Pattern);

        // Load snapshot and apply delta
        var state = ImmutableDatasetState.FromProto(snapshot, compressor);
        state.Entities.Count.Should().Be(2);

        var newState = state.ApplyDelta(delta);
        newState.LatestVersion.Should().Be(6);
        newState.Entities.Count.Should().Be(2); // 001 changed, 002 deleted, 003 added
        newState.Entities["t/abc/items/001/values"].Should().Be(new IndexEntity(6, 40, 15, 11));
        newState.Entities["t/abc/items/003/values"].Should().Be(new IndexEntity(6, 30, 30, 3));
        newState.Entities.ContainsKey("t/abc/items/002/values").Should().BeFalse();
    }

    [Fact]
    public void CompressedSnapshot_UncompressedDelta_EndToEnd()
    {
        var compressor = NameCompressor.FromPattern("t/*/items/*/values")!;

        var snapshotBytes = WriteSnapshotBytes(5, compressor,
            ("t/abc/items/001/values", 0, 10, 1));

        // Delta written without compression (legacy nested)
        var deltaBytes = WriteDeltaBytes(6, null,
            newEntities: [("t/abc/items/002/values", 10, 20, 2)]);

        var snapshot = SnapshotIndex.Parser.ParseFrom(snapshotBytes);
        var delta = NestedIndexHelper.ParseDeltaIndex(deltaBytes);

        // Patterns don't match: snapshot has pattern, delta has none
        snapshot.IdPattern.Should().Be(compressor.Pattern);
        delta.IdPattern.Should().BeEmpty();

        // Load and apply — delta should use flat fallback
        var state = ImmutableDatasetState.FromProto(snapshot, compressor);
        var newState = state.ApplyDelta(delta);

        newState.Entities.Count.Should().Be(2);
        newState.Entities.ContainsKey("t/abc/items/001/values").Should().BeTrue();
        newState.Entities.ContainsKey("t/abc/items/002/values").Should().BeTrue();
    }

    [Fact]
    public void UncompressedSnapshot_CompressedDelta_EndToEnd()
    {
        var compressor = NameCompressor.FromPattern("t/*/items/*/values")!;

        // Snapshot written without compression
        var snapshotBytes = WriteSnapshotBytes(5, null,
            ("t/abc/items/001/values", 0, 10, 1));

        // Delta written with compression
        var deltaBytes = WriteDeltaBytes(6, compressor,
            newEntities: [("t/abc/items/002/values", 10, 20, 2)]);

        var snapshot = SnapshotIndex.Parser.ParseFrom(snapshotBytes);
        var delta = NestedIndexHelper.ParseDeltaIndex(deltaBytes);

        // No compressor provided — snapshot has no pattern, delta has one.
        // Delta flat fields are expanded, so flat fallback works.
        snapshot.IdPattern.Should().BeEmpty();
        delta.IdPattern.Should().Be(compressor.Pattern);
        delta.NewEntities[0].Name.Should().Be("t/abc/items/002/values", "flat names expanded by ParseDeltaIndex");

        var state = ImmutableDatasetState.FromProto(snapshot);
        var newState = state.ApplyDelta(delta);

        newState.Entities.Count.Should().Be(2);
        newState.Entities.ContainsKey("t/abc/items/001/values").Should().BeTrue();
        newState.Entities.ContainsKey("t/abc/items/002/values").Should().BeTrue();
    }

    [Fact]
    public void CompressedSnapshotA_CompressedDeltaB_DifferentPatterns_EndToEnd()
    {
        var patternA = NameCompressor.FromPattern("t/*/items/*/values")!;
        var patternB = NameCompressor.FromPattern("region/*/store/*/data")!;

        // Snapshot compressed with pattern A
        var snapshotBytes = WriteSnapshotBytes(5, patternA,
            ("t/abc/items/001/values", 0, 10, 1));

        // Delta compressed with pattern B (completely different structure)
        // Note: entity names don't match pattern B, so compression is a no-op
        // More realistic: delta has names that match pattern B
        var deltaBytes = WriteDeltaBytes(6, patternB,
            newEntities: [("region/eu/store/main/data", 10, 20, 2)]);

        var snapshot = SnapshotIndex.Parser.ParseFrom(snapshotBytes);
        var delta = NestedIndexHelper.ParseDeltaIndex(deltaBytes);

        // Verify raw proto patterns
        snapshot.IdPattern.Should().Be(patternA.Pattern);
        delta.IdPattern.Should().Be(patternB.Pattern);

        // Delta flat names should be expanded with pattern B
        delta.NewEntities[0].Name.Should().Be("region/eu/store/main/data");

        // Load snapshot (auto-detect creates compressor from pattern A)
        var state = ImmutableDatasetState.FromProto(snapshot);
        var newState = state.ApplyDelta(delta);

        // Both entities should be present with full names
        newState.Entities.Count.Should().Be(2);
        newState.Entities.ContainsKey("t/abc/items/001/values").Should().BeTrue();
        newState.Entities.ContainsKey("region/eu/store/main/data").Should().BeTrue();
    }

    [Fact]
    public void LegacyFlatSnapshot_CompressedDelta_EndToEnd()
    {
        var compressor = NameCompressor.FromPattern("t/*/items/*/values")!;

        var snapshotBytes = WriteLegacyFlatSnapshotBytes(5,
            ("t/abc/items/001/values", 0, 10, 1));

        var deltaBytes = WriteDeltaBytes(6, compressor,
            newEntities: [("t/abc/items/002/values", 10, 20, 2)]);

        var snapshot = SnapshotIndex.Parser.ParseFrom(snapshotBytes);
        var delta = NestedIndexHelper.ParseDeltaIndex(deltaBytes);

        // Snapshot is legacy flat, delta is compressed
        snapshot.IdPattern.Should().BeEmpty();
        snapshot.Entities.Should().HaveCount(1);
        delta.IdPattern.Should().Be(compressor.Pattern);

        var state = ImmutableDatasetState.FromProto(snapshot);
        var newState = state.ApplyDelta(delta);

        newState.Entities.Count.Should().Be(2);
        newState.Entities.ContainsKey("t/abc/items/001/values").Should().BeTrue();
        newState.Entities.ContainsKey("t/abc/items/002/values").Should().BeTrue();
    }

    [Fact]
    public void CompressedSnapshot_LegacyFlatDelta_EndToEnd()
    {
        var compressor = NameCompressor.FromPattern("t/*/items/*/values")!;

        var snapshotBytes = WriteSnapshotBytes(5, compressor,
            ("t/abc/items/001/values", 0, 10, 1));

        var deltaBytes = WriteLegacyFlatDeltaBytes(6,
            newEntities: [("t/abc/items/002/values", 10, 20, 2)]);

        var snapshot = SnapshotIndex.Parser.ParseFrom(snapshotBytes);
        var delta = NestedIndexHelper.ParseDeltaIndex(deltaBytes);

        snapshot.IdPattern.Should().Be(compressor.Pattern);
        delta.IdPattern.Should().BeEmpty();

        // Load snapshot with its compressor, apply legacy flat delta (flat fallback)
        var state = ImmutableDatasetState.FromProto(snapshot, compressor);
        var newState = state.ApplyDelta(delta);

        newState.Entities.Count.Should().Be(2);
        newState.Entities.ContainsKey("t/abc/items/001/values").Should().BeTrue();
        newState.Entities.ContainsKey("t/abc/items/002/values").Should().BeTrue();
    }

    #endregion

    #region Bulk delta application with mixed formats

    [Fact]
    public void CompressedSnapshot_MixedDeltas_BulkApply_EndToEnd()
    {
        var compressor = NameCompressor.FromPattern("t/*/items/*/values")!;

        var snapshotBytes = WriteSnapshotBytes(5, compressor,
            ("t/abc/items/001/values", 0, 10, 1));

        // Delta 1: compressed (same pattern) — should use nested merge
        var delta1Bytes = WriteDeltaBytes(6, compressor,
            newEntities: [("t/abc/items/002/values", 10, 20, 2)]);

        // Delta 2: legacy flat (no nesting) — should use flat fallback
        var delta2Bytes = WriteLegacyFlatDeltaBytes(7,
            newEntities: [("t/abc/items/003/values", 30, 30, 3)]);

        // Delta 3: uncompressed nested — should use flat fallback (pattern mismatch)
        var delta3Bytes = WriteDeltaBytes(8, null,
            changedEntities: [("t/abc/items/001/values", 40, 15, 11)],
            deletedNames: ["t/abc/items/002/values"]);

        var snapshot = SnapshotIndex.Parser.ParseFrom(snapshotBytes);
        var deltas = new[]
        {
            NestedIndexHelper.ParseDeltaIndex(delta1Bytes),
            NestedIndexHelper.ParseDeltaIndex(delta2Bytes),
            NestedIndexHelper.ParseDeltaIndex(delta3Bytes)
        };

        var state = ImmutableDatasetState.FromProto(snapshot, compressor);
        var newState = state.ApplyDeltas(deltas);

        newState.LatestVersion.Should().Be(8);
        newState.Entities.Count.Should().Be(2); // 001 changed, 002 deleted by d3, 003 added by d2
        newState.Entities["t/abc/items/001/values"].Should().Be(new IndexEntity(8, 40, 15, 11));
        newState.Entities.ContainsKey("t/abc/items/002/values").Should().BeFalse();
        newState.Entities["t/abc/items/003/values"].Should().Be(new IndexEntity(7, 30, 30, 3));
    }

    [Fact]
    public void UncompressedSnapshot_MixedDeltas_BulkApply_EndToEnd()
    {
        // No compression anywhere in the snapshot
        var snapshotBytes = WriteSnapshotBytes(1, null,
            ("data/a", 0, 10, 1),
            ("data/b", 10, 20, 2));

        // Delta 1: compressed (trie has no compressor → flat fallback)
        var compressor = NameCompressor.FromPattern("data/{id}")!;
        var delta1Bytes = WriteDeltaBytes(2, compressor,
            newEntities: [("data/c", 20, 30, 3)]);

        // Delta 2: uncompressed nested
        var delta2Bytes = WriteDeltaBytes(3, null,
            deletedNames: ["data/a"]);

        var snapshot = SnapshotIndex.Parser.ParseFrom(snapshotBytes);
        var deltas = new[]
        {
            NestedIndexHelper.ParseDeltaIndex(delta1Bytes),
            NestedIndexHelper.ParseDeltaIndex(delta2Bytes)
        };

        var state = ImmutableDatasetState.FromProto(snapshot);
        var newState = state.ApplyDeltas(deltas);

        newState.LatestVersion.Should().Be(3);
        newState.Entities.Count.Should().Be(2); // a deleted, b unchanged, c added
        newState.Entities.ContainsKey("data/a").Should().BeFalse();
        newState.Entities["data/b"].Should().Be(new IndexEntity(1, 10, 20, 2));
        newState.Entities["data/c"].Should().Be(new IndexEntity(2, 20, 30, 3));
    }

    #endregion

    #region Proto byte-level size verification

    [Fact]
    public void CompressedSnapshot_IsSmallerThanUncompressed()
    {
        var compressor = NameCompressor.FromPattern("t/{tenantid}/s/{storeid}/items/{itemid}/values")!;
        var entities = Enumerable.Range(0, 100).Select(i =>
            ($"t/abc/s/def/items/{i:D5}/values", (long)i * 10, 10, (uint)i)
        ).ToArray();

        var compressedBytes = WriteSnapshotBytes(5, compressor, entities);
        var uncompressedBytes = WriteSnapshotBytes(5, null, entities);

        compressedBytes.Length.Should().BeLessThan(uncompressedBytes.Length,
            "compressed format strips fixed path segments, reducing proto size");
    }

    [Fact]
    public void CompressedDelta_IsSmallerThanUncompressed()
    {
        var compressor = NameCompressor.FromPattern("t/{tenantid}/s/{storeid}/items/{itemid}/values")!;
        var newEntities = Enumerable.Range(0, 50).Select(i =>
            ($"t/abc/s/def/items/{i:D5}/values", (long)i * 10, 10, (uint)i)
        ).ToArray();

        var compressedBytes = WriteDeltaBytes(10, compressor, newEntities: newEntities);
        var uncompressedBytes = WriteDeltaBytes(10, null, newEntities: newEntities);

        compressedBytes.Length.Should().BeLessThan(uncompressedBytes.Length,
            "compressed format strips fixed path segments, reducing proto size");
    }

    #endregion

    #region Snapshot position trie cross-format

    [Fact]
    public void SnapshotPositionTrie_CompressedSnapshot_RoundTrip()
    {
        var compressor = NameCompressor.FromPattern("t/*/items/*/values")!;
        var bytes = WriteSnapshotBytes(5, compressor,
            ("t/abc/items/001/values", 0, 10, 1),
            ("t/abc/items/002/values", 100, 20, 2));

        var parsed = SnapshotIndex.Parser.ParseFrom(bytes);

        // Verify proto has compressed names
        FlattenNestedNames(parsed.Nested).Should().BeEquivalentTo(["abc/001", "abc/002"]);

        // Trie-based state should expand and provide full-name lookups
        var state = ImmutableDatasetState.FromProto(parsed, compressor);
        state.Entities.TryGetValue("t/abc/items/001/values", out var e1).Should().BeTrue();
        e1.Position.Should().Be(0);
        state.Entities.TryGetValue("t/abc/items/002/values", out var e2).Should().BeTrue();
        e2.Position.Should().Be(100);

        // Compressed name also works via the trie (unlike the old Dictionary-based lookup)
        state.Entities.TryGetValue("abc/001", out _).Should().BeTrue();
    }

    [Fact]
    public void SnapshotPositionTrie_LegacyFlatSnapshot_RoundTrip()
    {
        var bytes = WriteLegacyFlatSnapshotBytes(5,
            ("items/a", 0, 10, 1),
            ("items/b", 100, 20, 2));

        var parsed = SnapshotIndex.Parser.ParseFrom(bytes);
        var state = ImmutableDatasetState.FromProto(parsed);

        state.Entities.TryGetValue("items/a", out var e1).Should().BeTrue();
        e1.Position.Should().Be(0);
        state.Entities.TryGetValue("items/b", out var e2).Should().BeTrue();
        e2.Position.Should().Be(100);
    }

    #endregion
}
