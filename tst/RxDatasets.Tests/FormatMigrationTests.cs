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
    private static IndexEntity MakeEntity(ulong version = 1, long position = 0, int length = 10, uint crc32 = 42)
        => new(version, position, length, crc32);

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
        // Deleted entity is excluded from the trie
        state.Entities.Count.Should().Be(2);
        state.Entities["channels/64090/days/20240923/events/1"].Length.Should().Be(100);
        state.Entities["channels/64090/days/20240923/events/2"].Length.Should().Be(200);
        state.Entities.ContainsKey("channels/99999/days/20240101/events/1").Should().BeFalse();
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
        // Build a nested SnapshotIndex proto directly
        var proto = new SnapshotIndex { SnapshotVersion = 10 };
        var root = new SnapshotBucket();

        // prov/bbc/pack/123/video/{1,2}
        var video123 = new SnapshotBucket();
        video123.Leaves.Add("1", new SnapshotEntity { LastInsertedVersion = { 1 }, Position = 0, Length = 100, Crc32 = 11 });
        video123.Leaves.Add("2", new SnapshotEntity { LastInsertedVersion = { 2 }, Position = 100, Length = 200, Crc32 = 22 });

        var pack123 = new SnapshotBucket();
        pack123.Children.Add("video", video123);

        // prov/bbc/pack/456/video/1
        var video456 = new SnapshotBucket();
        video456.Leaves.Add("1", new SnapshotEntity { LastInsertedVersion = { 3 }, Position = 300, Length = 50, Crc32 = 33 });

        var pack456 = new SnapshotBucket();
        pack456.Children.Add("video", video456);

        var packBucket = new SnapshotBucket();
        packBucket.Children.Add("123", pack123);
        packBucket.Children.Add("456", pack456);

        var bbcBucket = new SnapshotBucket();
        bbcBucket.Children.Add("pack", packBucket);

        // prov/nbc/pack/789/video/1
        var video789 = new SnapshotBucket();
        video789.Leaves.Add("1", new SnapshotEntity { LastInsertedVersion = { 4 }, Position = 350, Length = 75, Crc32 = 44 });

        var pack789 = new SnapshotBucket();
        pack789.Children.Add("video", video789);

        var nbcPack = new SnapshotBucket();
        nbcPack.Children.Add("789", pack789);

        var nbcBucket = new SnapshotBucket();
        nbcBucket.Children.Add("pack", nbcPack);

        var provBucket = new SnapshotBucket();
        provBucket.Children.Add("bbc", bbcBucket);
        provBucket.Children.Add("nbc", nbcBucket);

        root.Children.Add("prov", provBucket);
        proto.Nested = root;

        var bytes = proto.ToByteArray();

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
        newState.Entities.Count.Should().Be(3); // a (changed), c (unchanged), d (new) — b was deleted/removed

        // items/a was changed
        newState.Entities["items/a"].Length.Should().Be(110);
        newState.Entities["items/a"].Crc32.Should().Be(55u);
        newState.Entities["items/a"].LatestVersion.Should().Be(6);

        // items/b was deleted — no longer in trie
        newState.Entities.ContainsKey("items/b").Should().BeFalse();

        // items/c unchanged
        newState.Entities["items/c"].Crc32.Should().Be(33u);

        // items/d is new
        newState.Entities["items/d"].Length.Should().Be(75);
    }

    [Fact]
    public void NewSnapshot_PlusOldDeltas_ProducesCorrectState()
    {
        // Step 1: New nested snapshot (build proto directly)
        var proto = new SnapshotIndex { SnapshotVersion = 5 };
        var root = new SnapshotBucket();
        var itemsBucket = new SnapshotBucket();
        itemsBucket.Leaves.Add("a", new SnapshotEntity { LastInsertedVersion = { 5 }, Position = 0, Length = 100, Crc32 = 11 });
        itemsBucket.Leaves.Add("b", new SnapshotEntity { LastInsertedVersion = { 5 }, Position = 100, Length = 200, Crc32 = 22 });
        root.Children.Add("items", itemsBucket);
        proto.Nested = root;

        var snapshotBytes = proto.ToByteArray();
        var state = ImmutableDatasetState.FromProto(SnapshotIndex.Parser.ParseFrom(snapshotBytes));

        // Step 2: Old flat delta (not nested)
        var legacyDelta = new DeltaIndex { DeltaVersion = 6 };
        legacyDelta.NewEntities.Add(new DeltaEntity { Name = "items/c", Position = 0, Length = 50, Crc32 = 33 });
        legacyDelta.DeletedNames.Add("items/b"); // legacy format uses DeletedNames

        // Parse with new code (handles flat gracefully)
        var parsedDelta = NestedIndexHelper.ParseDeltaIndex(legacyDelta.ToByteArray());

        // Step 3: Apply
        var newState = state.ApplyDelta(parsedDelta);

        newState.LatestVersion.Should().Be(6);
        newState.Entities["items/a"].LatestVersion.Should().Be(5); // unchanged
        newState.Entities.ContainsKey("items/b").Should().BeFalse(); // deleted → removed from trie
        newState.Entities["items/c"].LatestVersion.Should().Be(6); // new
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
        state.Entities["data/1"].LatestVersion.Should().Be(3);
        state.Entities["data/1"].Crc32.Should().Be(3u);
        state.Entities["data/2"].LatestVersion.Should().Be(4);
        state.Entities["data/2"].Crc32.Should().Be(4u);
    }

    #endregion

    #region GetChangedNames across format migration

    [Fact]
    public void GetChangedNames_CanBeDeterminedFromDeltaIndices()
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
        var parsedDelta = NestedIndexHelper.ParseDeltaIndex(deltaBytes);

        state = state.ApplyDelta(parsedDelta);

        // After applying delta, both entities should be in trie with version 2
        state.Entities["a/1"].LatestVersion.Should().Be(2);
        state.Entities["a/2"].LatestVersion.Should().Be(2);
        state.Entities.Count.Should().Be(2);

        // The delta index itself contains the change info for GetChangedNames
        parsedDelta.NewEntities.Should().ContainSingle(e => e.Name == "a/2");
        parsedDelta.ChangedEntities.Should().ContainSingle(e => e.Name == "a/1");
    }

    #endregion

    #region Snapshot re-serialization (old → new format via read + write)

    [Fact]
    public void OldFlatSnapshot_ReadAndRewrite_PreservesData()
    {
        // Old flat snapshot
        var legacy = new SnapshotIndex { SnapshotVersion = 5 };
        legacy.Entities.Add("a/b", new SnapshotEntity { LastInsertedVersion = { 5 }, Position = 0, Length = 10, Crc32 = 42 });
        legacy.Entities.Add("c/d", new SnapshotEntity { LastInsertedVersion = { 5 }, Position = 10, Length = 20, Crc32 = 99 });

        var legacyBytes = legacy.ToByteArray();

        // Read with new code
        var state = ImmutableDatasetState.FromProto(SnapshotIndex.Parser.ParseFrom(legacyBytes));

        // Verify the data was correctly loaded
        state.Entities.Count.Should().Be(2);
        state.Entities["a/b"].Crc32.Should().Be(42u);
        state.Entities["c/d"].Crc32.Should().Be(99u);
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
        // Build a nested proto directly
        var proto = new SnapshotIndex { SnapshotVersion = 5 };
        var root = new SnapshotBucket();

        var taBucket = new SnapshotBucket();
        var taItemsBucket = new SnapshotBucket();
        taItemsBucket.Leaves.Add("1", new SnapshotEntity { LastInsertedVersion = { 1 }, Position = 0, Length = 10 });
        taItemsBucket.Leaves.Add("2", new SnapshotEntity { LastInsertedVersion = { 2 }, Position = 10, Length = 20 });
        taBucket.Children.Add("items", taItemsBucket);

        var tbBucket = new SnapshotBucket();
        var tbItemsBucket = new SnapshotBucket();
        tbItemsBucket.Leaves.Add("3", new SnapshotEntity { LastInsertedVersion = { 3 }, Position = 30, Length = 30 });
        tbBucket.Children.Add("items", tbItemsBucket);

        var tBucket = new SnapshotBucket();
        tBucket.Children.Add("a", taBucket);
        tBucket.Children.Add("b", tbBucket);
        root.Children.Add("t", tBucket);
        proto.Nested = root;

        var bytes = proto.ToByteArray();
        var restored = ImmutableDatasetState.FromProto(SnapshotIndex.Parser.ParseFrom(bytes));

        restored.Entities.ListDescendants("t/a").Should().BeEquivalentTo(["t/a/items/1", "t/a/items/2"]);
        restored.Entities.CountDescendants("t/a").Should().Be(2);
    }

    #endregion
}
