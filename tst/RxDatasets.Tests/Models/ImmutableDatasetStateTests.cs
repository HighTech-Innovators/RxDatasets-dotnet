using Google.Protobuf;
using RxDatasets.Models;

namespace RxDatasets.Tests.Models;

public class ImmutableDatasetStateTests
{
    private static readonly string[] Names =
    [
        "esltypes/E", "t/T/tenant", "t/T/s/S/eccparameters", "t/T/s/S/items/I1/values", "t/T/s/S/items/I2/values",
        "t/T/s/S/esls/B/pages/1/studioimage",
    ];

    private static SnapshotEntity Entity(int i) =>
        new() { LastInsertedVersion = { 1 }, Position = i * 10, Length = 10, Crc32 = (uint)i + 1 };

    private static SnapshotIndex FlatSnapshot()
    {
        var index = new SnapshotIndex { SnapshotVersion = 1 };
        for (int i = 0; i < Names.Length; i++)
            index.Entities.Add(Names[i], Entity(i));
        return index;
    }

    private static Dictionary<string, ImmutableSnapshotEntity> Entities(ImmutableDatasetState state) =>
        state.EnumerateAll().ToDictionary(e => e.Key, e => e.Value);

    [Fact]
    public void FromProto_ReadsLegacyFlatSnapshot()
    {
        var state = ImmutableDatasetState.FromProto(FlatSnapshot());

        state.SnapshotVersion.Should().Be(1);
        state.LatestVersion.Should().Be(1);
        Entities(state).Keys.Should().BeEquivalentTo(Names);
        state.GetEntity("t/T/s/S/items/I2/values").Position.Should().Be(40);
        state.Root.LiveCount.Should().Be(Names.Length);
    }

    [Fact]
    public void ToProto_WritesOnlyTheTree_AndRoundTrips()
    {
        var state = ImmutableDatasetState.FromProto(FlatSnapshot());

        var proto = state.ToProto();
        var reread = ImmutableDatasetState.FromProto(SnapshotIndex.Parser.ParseFrom(proto.ToByteArray()));

        proto.Entities.Should().BeEmpty();
        proto.Root.Leaves.Keys.Should().BeEquivalentTo("esltypes/{}", "t/{}/tenant");
        Entities(reread).Should().BeEquivalentTo(Entities(state));
        reread.SnapshotVersion.Should().Be(1);
    }

    [Fact]
    public void FromProto_ReadsTreeAndFlatEntriesTogether()
    {
        var proto = ImmutableDatasetState.FromProto(FlatSnapshot()).ToProto();
        proto.Entities.Add("palettes/P", Entity(9));

        var state = ImmutableDatasetState.FromProto(proto);

        Entities(state).Keys.Should().BeEquivalentTo(Names.Append("palettes/P"));
    }

    [Fact]
    public void FromProto_LeavesOutTheTombstonesOfTheEarlierFormat_AndTakesTheLatestVersion()
    {
        var proto = FlatSnapshot();
        proto.Entities["t/T/tenant"] = new SnapshotEntity { LastInsertedVersion = { 1 }, LastDeletedVersion = { 2 } };
        proto.Entities["esltypes/E"] = new SnapshotEntity
        {
            LastInsertedVersion = { 1, 4 }, LastDeletedVersion = { 3 }, LastUpdatedVersion = 5, Position = 7, Length = 2
        };

        var state = ImmutableDatasetState.FromProto(proto);

        state.TryGetEntity("t/T/tenant", out _).Should().BeFalse();
        state.GetEntity("esltypes/E").Should().BeEquivalentTo(new ImmutableSnapshotEntity(5, 7, 2));
        state.Root.LiveCount.Should().Be(Names.Length - 1);
    }

    [Fact]
    public void ToProto_WritesTheVersionAsTheOnlyCreation_SoAnEarlierReaderSeesALiveEntity()
    {
        var state = ImmutableDatasetState.FromProto(FlatSnapshot()).ApplyDelta(FlatDelta());

        var entity = state.ToProto().EnumerateEntities().Single(e => e.Name == "t/T/s/S/items/I1/values").Entity;

        entity.LastInsertedVersion.Should().Equal(2UL);
        entity.LastDeletedVersion.Should().BeEmpty();
        entity.LastUpdatedVersion.Should().Be(0);
        entity.GetLatestChangeType().Should().Be(ChangeType.New);
        entity.GetLatestEntityVersion().Should().Be(2);
    }

    private static DeltaIndex FlatDelta()
    {
        var delta = new DeltaIndex { DeltaVersion = 2 };
        delta.NewEntities.Add(
            new DeltaEntity { Name = "t/T/s/S/items/I3/values", Position = 1, Length = 5, Crc32 = 7, PreviousVersion = 0 });
        delta.ChangedEntities.Add(
            new DeltaEntity { Name = "t/T/s/S/items/I1/values", Position = 6, Length = 5, Crc32 = 8, PreviousVersion = 1 });
        delta.DeletedEntities.Add(new DeletedEntity { Name = "t/T/tenant", PreviousVersion = 1 });
        delta.DeletedNames.Add("esltypes/E");
        return delta;
    }

    private static void ShouldHaveAppliedFlatDelta(ImmutableDatasetState state)
    {
        state.LatestVersion.Should().Be(2);
        state.SnapshotVersion.Should().Be(1);
        state.GetEntity("t/T/s/S/items/I3/values").Should().BeEquivalentTo(new ImmutableSnapshotEntity(2, 1, 5, 7));
        state.GetEntity("t/T/s/S/items/I1/values").Should().BeEquivalentTo(new ImmutableSnapshotEntity(2, 6, 5, 8));
        // A deleted entity leaves the index; that it was deleted is in the delta
        state.TryGetEntity("t/T/tenant", out _).Should().BeFalse();
        state.TryGetEntity("esltypes/E", out _).Should().BeFalse();
        state.Root.LiveCount.Should().Be(Names.Length + 1 - 2);
        state.Root.CountLive("t/T/s/S/items").Should().Be(3);
    }

    [Fact]
    public void ApplyDelta_AppliesLegacyFlatDelta()
    {
        ShouldHaveAppliedFlatDelta(ImmutableDatasetState.FromProto(FlatSnapshot()).ApplyDelta(FlatDelta()));
    }

    [Fact]
    public void ApplyDelta_AppliesTreeDelta_AsWrittenAndReadBack()
    {
        var delta = FlatDelta();
        delta.ConvertToTree();
        var reread = DeltaIndex.Parser.ParseFrom(delta.ToByteArray());

        ShouldHaveAppliedFlatDelta(ImmutableDatasetState.FromProto(FlatSnapshot()).ApplyDelta(reread));
    }

    [Fact]
    public void ApplyDelta_LeavesTheOriginalStateUntouched()
    {
        var original = ImmutableDatasetState.FromProto(FlatSnapshot());

        original.ApplyDelta(FlatDelta());

        Entities(original).Should().BeEquivalentTo(Entities(ImmutableDatasetState.FromProto(FlatSnapshot())));
        original.Root.LiveCount.Should().Be(Names.Length);
    }

    [Fact]
    public void ApplyDelta_RecreatesADeletedEntity()
    {
        var recreate = new DeltaIndex { DeltaVersion = 3 };
        recreate.NewEntities.Add(new DeltaEntity { Name = "t/T/tenant", Position = 11, Length = 4, Crc32 = 9 });
        recreate.ConvertToTree();

        var state = ImmutableDatasetState.FromProto(FlatSnapshot()).ApplyDelta(FlatDelta()).ApplyDelta(recreate);

        state.Root.LiveCount.Should().Be(Names.Length + 1 - 2 + 1);
        state.Root.CountLive("t/T").Should().Be(6);
        state.TryGetEntity("t/T/tenant", out var entity).Should().BeTrue();
        entity.Should().BeEquivalentTo(new ImmutableSnapshotEntity(3, 11, 4, 9));
    }

    [Fact]
    public void ApplyDelta_Throws_WhenAChangedEntityIsUnknown()
    {
        var delta = new DeltaIndex { DeltaVersion = 2 };
        delta.ChangedEntities.Add(new DeltaEntity { Name = "t/T/s/S/items/unknown/values" });
        delta.ConvertToTree();

        var act = () => ImmutableDatasetState.FromProto(FlatSnapshot()).ApplyDelta(delta);

        act.Should().Throw<KeyNotFoundException>().WithMessage("*items/unknown/values*");
    }

    [Fact]
    public void ConvertToTree_EmptiesNamesAndFlatLists()
    {
        var delta = FlatDelta();

        delta.ConvertToTree();

        delta.HasFlatEntries().Should().BeFalse();
        delta.EnumerateNewEntities().Concat(delta.EnumerateChangedEntities())
            .Should().AllSatisfy(e => e.Entity.Name.Should().BeEmpty());
        delta.EnumerateDeletedEntities().Should().AllSatisfy(e => e.Entity.Name.Should().BeEmpty());
    }
}
