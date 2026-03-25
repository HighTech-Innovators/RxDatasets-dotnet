using System.Collections.Immutable;
using FluentAssertions;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using RxDatasets.Abstractions;
using RxDatasets.Models;

namespace RxDatasets.Tests;

/// <summary>
/// Tests that verify old flat-format data can be read by new code,
/// new nested-format data roundtrips correctly, and mixed scenarios
/// (old snapshots + new deltas) work end-to-end.
/// </summary>
public class FormatMigrationTests
{
    private static ImmutableSnapshotEntity MakeEntity(ulong insertedVersion = 1, long position = 0, int length = 10, uint crc32 = 42)
        => new([], [insertedVersion], 0, position, length, crc32);

    #region Old flat snapshot bytes → new reader

    [Fact]
    public void OldFlatSnapshotBytes_CanBeReadByNewCode()
    {
        // Build a legacy flat SnapshotIndex and serialize to bytes
        var legacy = new SnapshotIndex { SnapshotVersion = 5 };
        legacy.Entities.Add("channels/64090/days/20240923/events/1", new SnapshotEntity
        {
            LastInsertedVersion = { 5 },
            Position = 0,
            Length = 100,
            Crc32 = 111
        });
        legacy.Entities.Add("channels/64090/days/20240923/events/2", new SnapshotEntity
        {
            LastInsertedVersion = { 5 },
            Position = 100,
            Length = 200,
            Crc32 = 222
        });
        legacy.Entities.Add("channels/99999/days/20240101/events/1", new SnapshotEntity
        {
            LastInsertedVersion = { 3 },
            LastDeletedVersion = { 4 },
            Position = 0,
            Length = 0,
            Crc32 = 333
        });

        var legacyBytes = legacy.ToByteArray();

        // Parse with new code
        var parsed = SnapshotIndex.Parser.ParseFrom(legacyBytes);
        var state = ImmutableDatasetState.FromProto(parsed);

        state.SnapshotVersion.Should().Be(5);
        state.Entities.Count.Should().Be(3);
        state.Entities["channels/64090/days/20240923/events/1"].Length.Should().Be(100);
        state.Entities["channels/64090/days/20240923/events/2"].Length.Should().Be(200);
        state.Entities["channels/99999/days/20240101/events/1"].GetLatestChangeType().Should().Be(ChangeType.Deleted);
    }

    #endregion

    #region Old flat delta bytes → new reader

    [Fact]
    public void OldFlatDeltaBytes_CanBeReadByNewCode()
    {
        var legacyDelta = new DeltaIndex
        {
            DeltaVersion = 10,
            Timestamp = Timestamp.FromDateTimeOffset(new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero))
        };
        legacyDelta.NewEntities.Add(new DeltaEntity { Name = "a/b/c", Position = 0, Length = 50, Crc32 = 11, PreviousVersion = 0 });
        legacyDelta.ChangedEntities.Add(new DeltaEntity { Name = "x/y/z", Position = 50, Length = 30, Crc32 = 22, PreviousVersion = 8 });
        legacyDelta.DeletedEntities.Add(new DeletedEntity { Name = "old/thing", PreviousVersion = 7 });
        legacyDelta.DeletedNames.Add("ancient/name");

        var legacyBytes = legacyDelta.ToByteArray();

        // Parse with new code
        var parsed = NestedIndexHelper.ParseDeltaIndex(legacyBytes);

        parsed.DeltaVersion.Should().Be(10);
        parsed.NewEntities.Should().HaveCount(1);
        parsed.NewEntities[0].Name.Should().Be("a/b/c");
        parsed.ChangedEntities.Should().HaveCount(1);
        parsed.ChangedEntities[0].Name.Should().Be("x/y/z");
        parsed.DeletedEntities.Should().HaveCount(1);
        parsed.DeletedEntities[0].Name.Should().Be("old/thing");
        parsed.DeletedNames.Should().HaveCount(1);
        parsed.DeletedNames[0].Should().Be("ancient/name");
    }

    #endregion

    #region New nested format → bytes → parse back

    [Fact]
    public void NewNestedSnapshotBytes_Roundtrip()
    {
        var builder = new EntityTrie.Builder();
        builder.Add("prov/bbc/pack/123/video/1", MakeEntity(1, 0, 100, 11));
        builder.Add("prov/bbc/pack/123/video/2", MakeEntity(2, 100, 200, 22));
        builder.Add("prov/bbc/pack/456/video/1", MakeEntity(3, 300, 50, 33));
        builder.Add("prov/nbc/pack/789/video/1", MakeEntity(4, 350, 75, 44));
        var trie = builder.Build();

        var state = new ImmutableDatasetState(trie, 10, 10);
        var bytes = state.ToProto().ToByteArray();

        // Parse from raw bytes
        var parsed = SnapshotIndex.Parser.ParseFrom(bytes);
        var restored = ImmutableDatasetState.FromProto(parsed);

        restored.Entities.Count.Should().Be(4);
        restored.Entities["prov/bbc/pack/123/video/1"].Crc32.Should().Be(11u);
        restored.Entities["prov/bbc/pack/123/video/2"].Crc32.Should().Be(22u);
        restored.Entities["prov/bbc/pack/456/video/1"].Crc32.Should().Be(33u);
        restored.Entities["prov/nbc/pack/789/video/1"].Crc32.Should().Be(44u);
    }

    [Fact]
    public void NewNestedDeltaBytes_Roundtrip()
    {
        var delta = new DeltaIndex
        {
            DeltaVersion = 15,
            Timestamp = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow)
        };
        delta.NewEntities.Add(new DeltaEntity { Name = "t/a/s/d/new1", Position = 0, Length = 10, Crc32 = 1 });
        delta.ChangedEntities.Add(new DeltaEntity { Name = "t/a/s/d/chg1", Position = 10, Length = 20, Crc32 = 2, PreviousVersion = 12 });
        delta.DeletedEntities.Add(new DeletedEntity { Name = "t/b/s/e/del1", PreviousVersion = 10 });

        var bytes = NestedIndexHelper.SerializeDeltaIndex(delta);
        var restored = NestedIndexHelper.ParseDeltaIndex(bytes);

        restored.DeltaVersion.Should().Be(15);
        restored.NewEntities.Should().ContainSingle(e => e.Name == "t/a/s/d/new1");
        restored.ChangedEntities.Should().ContainSingle(e => e.Name == "t/a/s/d/chg1" && e.PreviousVersion == 12);
        restored.DeletedEntities.Should().ContainSingle(e => e.Name == "t/b/s/e/del1" && e.PreviousVersion == 10);
    }

    #endregion

    #region Mixed: old flat snapshot + new nested deltas applied

    [Fact]
    public void OldSnapshot_PlusNewDeltas_ProducesCorrectState()
    {
        // Step 1: Build a legacy flat snapshot
        var legacySnapshot = new SnapshotIndex { SnapshotVersion = 5 };
        legacySnapshot.Entities.Add("items/a", new SnapshotEntity
        {
            LastInsertedVersion = { 5 },
            Position = 0,
            Length = 100,
            Crc32 = 11
        });
        legacySnapshot.Entities.Add("items/b", new SnapshotEntity
        {
            LastInsertedVersion = { 5 },
            Position = 100,
            Length = 200,
            Crc32 = 22
        });
        legacySnapshot.Entities.Add("items/c", new SnapshotEntity
        {
            LastInsertedVersion = { 3 },
            Position = 300,
            Length = 50,
            Crc32 = 33
        });

        // Deserialize snapshot with new code (flat fallback)
        var state = ImmutableDatasetState.FromProto(legacySnapshot);

        // Step 2: Build a new nested delta
        var delta = new DeltaIndex { DeltaVersion = 6 };
        delta.NewEntities.Add(new DeltaEntity { Name = "items/d", Position = 0, Length = 75, Crc32 = 44, PreviousVersion = 0 });
        delta.ChangedEntities.Add(new DeltaEntity { Name = "items/a", Position = 75, Length = 110, Crc32 = 55, PreviousVersion = 5 });
        delta.DeletedEntities.Add(new DeletedEntity { Name = "items/b", PreviousVersion = 5 });

        // Serialize with nesting and parse back (simulates storage roundtrip)
        var deltaBytes = NestedIndexHelper.SerializeDeltaIndex(delta);
        var parsedDelta = NestedIndexHelper.ParseDeltaIndex(deltaBytes);

        // Step 3: Apply delta to snapshot
        var newState = state.ApplyDelta(parsedDelta);

        newState.LatestVersion.Should().Be(6);
        newState.Entities.Count.Should().Be(4); // a, b (deleted), c, d

        // items/a was changed
        newState.Entities["items/a"].GetLatestChangeType().Should().Be(ChangeType.Changed);
        newState.Entities["items/a"].Length.Should().Be(110);
        newState.Entities["items/a"].Crc32.Should().Be(55u);

        // items/b was deleted
        newState.Entities["items/b"].GetLatestChangeType().Should().Be(ChangeType.Deleted);

        // items/c unchanged
        newState.Entities["items/c"].GetLatestChangeType().Should().Be(ChangeType.New);
        newState.Entities["items/c"].Crc32.Should().Be(33u);

        // items/d is new
        newState.Entities["items/d"].GetLatestChangeType().Should().Be(ChangeType.New);
        newState.Entities["items/d"].Length.Should().Be(75);
    }

    [Fact]
    public void NewSnapshot_PlusOldDeltas_ProducesCorrectState()
    {
        // Step 1: New nested snapshot
        var builder = new EntityTrie.Builder();
        builder.Add("items/a", MakeEntity(5, 0, 100, 11));
        builder.Add("items/b", MakeEntity(5, 100, 200, 22));
        var trie = builder.Build();
        var state = new ImmutableDatasetState(trie, 5, 5);

        // Roundtrip through bytes to ensure nested format
        var snapshotBytes = state.ToProto().ToByteArray();
        state = ImmutableDatasetState.FromProto(SnapshotIndex.Parser.ParseFrom(snapshotBytes));

        // Step 2: Old flat delta (not nested)
        var legacyDelta = new DeltaIndex { DeltaVersion = 6 };
        legacyDelta.NewEntities.Add(new DeltaEntity { Name = "items/c", Position = 0, Length = 50, Crc32 = 33 });
        legacyDelta.DeletedNames.Add("items/b"); // legacy format uses DeletedNames

        // Parse with new code (handles flat gracefully)
        var parsedDelta = NestedIndexHelper.ParseDeltaIndex(legacyDelta.ToByteArray());

        // Step 3: Apply
        var newState = state.ApplyDelta(parsedDelta);

        newState.LatestVersion.Should().Be(6);
        newState.Entities["items/a"].GetLatestChangeType().Should().Be(ChangeType.New); // unchanged
        newState.Entities["items/b"].GetLatestChangeType().Should().Be(ChangeType.Deleted);
        newState.Entities["items/c"].GetLatestChangeType().Should().Be(ChangeType.New);
    }

    #endregion

    #region Multiple deltas applied sequentially

    [Fact]
    public void MultipleNewDeltas_AppliedToOldSnapshot()
    {
        // Old flat snapshot with one entity
        var snapshot = new SnapshotIndex { SnapshotVersion = 1 };
        snapshot.Entities.Add("data/1", new SnapshotEntity
        {
            LastInsertedVersion = { 1 },
            Position = 0,
            Length = 10,
            Crc32 = 1
        });
        var state = ImmutableDatasetState.FromProto(snapshot);

        // Delta v2: add entity
        var delta2 = new DeltaIndex { DeltaVersion = 2 };
        delta2.NewEntities.Add(new DeltaEntity { Name = "data/2", Position = 0, Length = 20, Crc32 = 2 });
        var d2Bytes = NestedIndexHelper.SerializeDeltaIndex(delta2);

        // Delta v3: change entity, delete another
        var delta3 = new DeltaIndex { DeltaVersion = 3 };
        delta3.ChangedEntities.Add(new DeltaEntity { Name = "data/1", Position = 0, Length = 15, Crc32 = 3, PreviousVersion = 1 });
        delta3.DeletedEntities.Add(new DeletedEntity { Name = "data/2", PreviousVersion = 2 });
        var d3Bytes = NestedIndexHelper.SerializeDeltaIndex(delta3);

        // Delta v4: re-add deleted entity
        var delta4 = new DeltaIndex { DeltaVersion = 4 };
        delta4.NewEntities.Add(new DeltaEntity { Name = "data/2", Position = 0, Length = 25, Crc32 = 4, PreviousVersion = 3 });
        var d4Bytes = NestedIndexHelper.SerializeDeltaIndex(delta4);

        // Apply all deltas
        var parsedDeltas = new[]
        {
            NestedIndexHelper.ParseDeltaIndex(d2Bytes),
            NestedIndexHelper.ParseDeltaIndex(d3Bytes),
            NestedIndexHelper.ParseDeltaIndex(d4Bytes)
        };
        state = state.ApplyDeltas(parsedDeltas);

        state.LatestVersion.Should().Be(4);
        state.Entities["data/1"].GetLatestChangeType().Should().Be(ChangeType.Changed);
        state.Entities["data/1"].Crc32.Should().Be(3u);
        state.Entities["data/2"].GetLatestChangeType().Should().Be(ChangeType.New);
        state.Entities["data/2"].Crc32.Should().Be(4u);
    }

    #endregion

    #region GetChangedNames across format migration

    [Fact]
    public void GetChangedNames_WorksAcrossFormatBoundary()
    {
        // Old snapshot at v1
        var snapshot = new SnapshotIndex { SnapshotVersion = 1 };
        snapshot.Entities.Add("a/1", new SnapshotEntity
        {
            LastInsertedVersion = { 1 },
            Position = 0,
            Length = 10,
            Crc32 = 1
        });
        var state = ImmutableDatasetState.FromProto(snapshot);

        // New nested delta at v2
        var delta = new DeltaIndex { DeltaVersion = 2 };
        delta.NewEntities.Add(new DeltaEntity { Name = "a/2", Position = 0, Length = 20, Crc32 = 2 });
        delta.ChangedEntities.Add(new DeltaEntity { Name = "a/1", Position = 20, Length = 15, Crc32 = 3, PreviousVersion = 1 });

        var deltaBytes = NestedIndexHelper.SerializeDeltaIndex(delta);
        state = state.ApplyDelta(NestedIndexHelper.ParseDeltaIndex(deltaBytes));

        // Query changes from v1
        var changed = state.Entities
            .Where(x => x.Value.GetLatestEntityVersion() > 1 && x.Value.GetLatestEntityVersion() <= 2)
            .Select(x => (x.Key, x.Value.GetLatestChangeType()))
            .ToList();

        changed.Should().HaveCount(2);
        changed.Should().Contain(("a/1", ChangeType.Changed));
        changed.Should().Contain(("a/2", ChangeType.New));
    }

    #endregion

    #region Snapshot re-serialization (old → new format via read + write)

    [Fact]
    public void OldFlatSnapshot_ReadAndRewrite_ProducesNestedFormat()
    {
        // Old flat snapshot
        var legacy = new SnapshotIndex { SnapshotVersion = 5 };
        legacy.Entities.Add("a/b", new SnapshotEntity { LastInsertedVersion = { 5 }, Position = 0, Length = 10, Crc32 = 42 });
        legacy.Entities.Add("c/d", new SnapshotEntity { LastInsertedVersion = { 5 }, Position = 10, Length = 20, Crc32 = 99 });

        var legacyBytes = legacy.ToByteArray();

        // Read with new code
        var state = ImmutableDatasetState.FromProto(SnapshotIndex.Parser.ParseFrom(legacyBytes));

        // Re-serialize with new code (produces nested format)
        var newProto = state.ToProto();
        newProto.Entities.Should().BeEmpty("new format writes nested only");
        newProto.Nested.Should().NotBeNull();

        var newBytes = newProto.ToByteArray();

        // Verify the new bytes are readable
        var reparsed = ImmutableDatasetState.FromProto(SnapshotIndex.Parser.ParseFrom(newBytes));
        reparsed.Entities.Count.Should().Be(2);
        reparsed.Entities["a/b"].Crc32.Should().Be(42u);
        reparsed.Entities["c/d"].Crc32.Should().Be(99u);

        // New nested bytes should be smaller than flat bytes for shared prefixes
        // (In this small case, might not be smaller due to overhead, but should at least roundtrip)
    }

    #endregion

    #region ListDescendants/CountDescendants after format migration

    [Fact]
    public void ListDescendants_WorksAfterReadingOldFlatSnapshot()
    {
        var legacy = new SnapshotIndex { SnapshotVersion = 5 };
        legacy.Entities.Add("t/a/items/1", new SnapshotEntity { LastInsertedVersion = { 5 }, Position = 0, Length = 10 });
        legacy.Entities.Add("t/a/items/2", new SnapshotEntity { LastInsertedVersion = { 5 }, Position = 10, Length = 20 });
        legacy.Entities.Add("t/b/items/3", new SnapshotEntity { LastInsertedVersion = { 5 }, Position = 30, Length = 30 });

        var state = ImmutableDatasetState.FromProto(legacy);

        state.Entities.ListDescendants("t/a").Should().BeEquivalentTo(["t/a/items/1", "t/a/items/2"]);
        state.Entities.ListDescendants("t/b").Should().BeEquivalentTo(["t/b/items/3"]);
        state.Entities.CountDescendants("t/a").Should().Be(2);
        state.Entities.CountDescendants("t").Should().Be(3);
    }

    [Fact]
    public void ListDescendants_WorksAfterReadingNewNestedSnapshot()
    {
        var builder = new EntityTrie.Builder();
        builder.Add("t/a/items/1", MakeEntity(1, 0, 10));
        builder.Add("t/a/items/2", MakeEntity(2, 10, 20));
        builder.Add("t/b/items/3", MakeEntity(3, 30, 30));
        var state = new ImmutableDatasetState(builder.Build(), 5, 5);

        // Roundtrip through bytes
        var bytes = state.ToProto().ToByteArray();
        var restored = ImmutableDatasetState.FromProto(SnapshotIndex.Parser.ParseFrom(bytes));

        restored.Entities.ListDescendants("t/a").Should().BeEquivalentTo(["t/a/items/1", "t/a/items/2"]);
        restored.Entities.CountDescendants("t/a").Should().Be(2);
    }

    #endregion
}
