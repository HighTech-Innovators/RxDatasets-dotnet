namespace RxDatasets.Tests.Models;

public class DeltaIndexEnumerationTests
{
    [Fact]
    public void Enumerate_ShouldReturnFullNamesAndTrackingInfo_ForWriterProducedHierarchicalDelta()
    {
        // Arrange: build the delta the way the writer does - flat lists converted to Root, then cleared
        var sla = Timestamp.FromDateTimeOffset(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var deltaIndex = new DeltaIndex { DeltaVersion = 5 };
        deltaIndex.NewEntities.Add(new DeltaEntity { Name = "a/1/b/2", Position = 1, Length = 3, TrackingId = "t-new", SlaTimestamp = sla });
        deltaIndex.ChangedEntities.Add(new DeltaEntity { Name = "top", Position = 4, Length = 5, TrackingId = "t-changed" });
        deltaIndex.DeletedEntities.Add(new DeletedEntity { Name = "a/1/c/3", PreviousVersion = 2, TrackingId = "t-deleted" });

        deltaIndex.Root = deltaIndex.ToDeltaNode();
        deltaIndex.NewEntities.Clear();
        deltaIndex.ChangedEntities.Clear();
        deltaIndex.DeletedEntities.Clear();

        // Act
        var newEntities = deltaIndex.EnumerateNewEntities().ToList();
        var changedEntities = deltaIndex.EnumerateChangedEntities().ToList();
        var deletedEntities = deltaIndex.EnumerateDeletedEntities().ToList();

        // Assert
        deltaIndex.HasHierarchicalEntries().Should().BeTrue();
        newEntities.Should().ContainSingle().Which.Should().Match<(string Name, DeltaEntity Entity)>(e =>
            e.Name == "a/1/b/2" && e.Entity.TrackingId == "t-new" && e.Entity.SlaTimestamp == sla && e.Entity.Position == 1);
        changedEntities.Should().ContainSingle().Which.Name.Should().Be("top");
        deletedEntities.Should().ContainSingle().Which.Should().Match<(string Name, DeletedEntity Entity)>(e =>
            e.Name == "a/1/c/3" && e.Entity.TrackingId == "t-deleted");
    }

    [Fact]
    public void Enumerate_ShouldUseMapKeys_WhenEntityNameHoldsOnlyTheLeaf()
    {
        // Arrange: migrated deltas store only the leaf name in the entity
        var leafNode = new DeltaNode();
        leafNode.NewEntities.Add("2", new DeltaEntity { Name = "2" });
        var root = new DeltaNode();
        root.Children.Add("a", new DeltaNode { Children = { { "1", new DeltaNode { Children = { { "b", leafNode } } } } } });
        var deltaIndex = new DeltaIndex { DeltaVersion = 1, Root = root };

        // Act & Assert
        deltaIndex.EnumerateNewEntities().Select(e => e.Name).Should().Equal("a/1/b/2");
    }

    [Fact]
    public void Enumerate_ShouldReturnFlatEntities_ForLegacyDelta()
    {
        // Arrange
        var deltaIndex = new DeltaIndex { DeltaVersion = 1 };
        deltaIndex.NewEntities.Add(new DeltaEntity { Name = "x/1" });
        deltaIndex.DeletedEntities.Add(new DeletedEntity { Name = "y/2" });

        // Act & Assert
        deltaIndex.HasHierarchicalEntries().Should().BeFalse();
        deltaIndex.EnumerateNewEntities().Select(e => e.Name).Should().Equal("x/1");
        deltaIndex.EnumerateChangedEntities().Should().BeEmpty();
        deltaIndex.EnumerateDeletedEntities().Select(e => e.Name).Should().Equal("y/2");
    }
}
