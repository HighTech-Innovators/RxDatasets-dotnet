using Google.Protobuf;
using RxDatasets.Models;

namespace RxDatasets.Tests.Models;

public class DeltaIndexEnumerationTests
{
    private static DeltaIndex FlatDelta()
    {
        var delta = new DeltaIndex { DeltaVersion = 5, Timestamp = Timestamp.FromDateTimeOffset(DateTimeOffset.UnixEpoch) };
        delta.NewEntities.Add(new DeltaEntity { Name = "t/T/s/S/items/I1/values", PreviousVersion = 0, TrackingId = "new", Position = 1 });
        delta.ChangedEntities.Add(new DeltaEntity { Name = "t/T/s/S/eccparameters", PreviousVersion = 3, TrackingId = "changed" });
        delta.DeletedEntities.Add(new DeletedEntity { Name = "esltypes/E", PreviousVersion = 4, TrackingId = "deleted" });
        delta.DeletedNames.Add("esltypes/E");
        delta.DeletedNames.Add("t/T/s/S/items/I2/values");
        return delta;
    }

    private static DeltaIndex TreeDelta()
    {
        var delta = FlatDelta();
        delta.ConvertToTree();
        return DeltaIndex.Parser.ParseFrom(delta.ToByteArray());
    }

    public static readonly TheoryData<bool> Formats = [false, true];

    private static DeltaIndex Delta(bool tree) => tree ? TreeDelta() : FlatDelta();

    [Theory]
    [MemberData(nameof(Formats))]
    public void Enumerate_ReturnsFullNamesAndTrackingInfo(bool tree)
    {
        var delta = Delta(tree);

        delta.EnumerateNewEntities().Select(e => (e.Name, e.Entity.TrackingId)).Should().Equal(("t/T/s/S/items/I1/values", "new"));
        delta.EnumerateChangedEntities().Select(e => (e.Name, e.Entity.TrackingId)).Should().Equal(("t/T/s/S/eccparameters", "changed"));
        var expectedDeleted = tree
            ? [("esltypes/E", "deleted"), ("t/T/s/S/items/I2/values", "")] // a legacy deleted name becomes a deleted entity
            : new[] { ("esltypes/E", "deleted") };
        delta.EnumerateDeletedEntities().Select(e => (e.Name, e.Entity.TrackingId)).Should().BeEquivalentTo(expectedDeleted);
    }

    [Fact]
    public void Enumerate_ReturnsFlatEntities_ForLegacyDelta()
    {
        var delta = FlatDelta();

        delta.EnumerateNewEntities().Select(e => e.Name).Should().Equal("t/T/s/S/items/I1/values");
        delta.DeletedNames.Should().HaveCount(2);
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public void FindChange_FindsEachKindOfChange(bool tree)
    {
        var delta = Delta(tree);

        delta.FindChange("t/T/s/S/items/I1/values").Should().Be((ChangeType.New, 0UL, delta.EnumerateNewEntities().Single().Entity));
        delta.FindChange("t/T/s/S/eccparameters").Change.Should().Be(ChangeType.Changed);
        delta.FindChange("t/T/s/S/eccparameters").PreviousVersion.Should().Be(3);
        delta.FindChange("esltypes/E").Should().Be((ChangeType.Deleted, 4UL, (DeltaEntity?)null));
        delta.FindChange("t/T/s/S/items/I2/values").Should().Be((ChangeType.Deleted, 0UL, (DeltaEntity?)null));
        delta.FindChange("t/T/s/S/items/I3/values").Change.Should().Be(ChangeType.None);
        delta.FindChange("t/T").Change.Should().Be(ChangeType.None);
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public void EnumerateHistoryEntries_ListsNewChangedDeleted_WithoutDuplicates(bool tree)
    {
        var delta = Delta(tree);

        delta.EnumerateHistoryEntries(5, "").Select(h => (h.Id, h.ChangeType, h.PreviousVersion)).Should().Equal(
            ("t/T/s/S/items/I1/values", ChangeType.New, 0UL),
            ("t/T/s/S/eccparameters", ChangeType.Changed, 3UL),
            ("esltypes/E", ChangeType.Deleted, 4UL),
            ("t/T/s/S/items/I2/values", ChangeType.Deleted, 0UL));
        delta.EnumerateHistoryEntries(5, "t/T/s/S/items").Select(h => h.Id).Should().Equal(
            "t/T/s/S/items/I1/values", "t/T/s/S/items/I2/values");
        delta.EnumerateHistoryEntries(5, "").Should().AllSatisfy(h =>
        {
            h.Version.Should().Be(5);
            h.Timestamp.Should().Be(DateTimeOffset.UnixEpoch);
        });
    }

    [Fact]
    public void EnumerateEntities_ReadsTreeAndLegacySnapshots()
    {
        var legacy = new SnapshotIndex { SnapshotVersion = 1 };
        legacy.Entities.Add("t/T/s/S/items/I1/values", new SnapshotEntity { Position = 3 });
        legacy.Entities.Add("esltypes/E", new SnapshotEntity { Position = 4 });
        var tree = ImmutableDatasetState.FromProto(legacy).ToProto();

        legacy.EnumerateEntities().Select(e => (e.Name, e.Entity.Position)).Should().BeEquivalentTo(
            [("t/T/s/S/items/I1/values", 3L), ("esltypes/E", 4L)]);
        tree.EnumerateEntities().Select(e => (e.Name, e.Entity.Position)).Should().BeEquivalentTo(
            [("t/T/s/S/items/I1/values", 3L), ("esltypes/E", 4L)]);
    }
}
