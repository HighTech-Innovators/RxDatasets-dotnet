using System.Collections.Generic;
using System.Collections.Immutable;
using RxDatasets.Models;
using Xunit;

namespace RxDatasets.Tests.Models;

public class ImmutableDatasetStateTests
{
    private ImmutableSnapshotEntity CreateTestEntity()
    {
        return new ImmutableSnapshotEntity(
            ImmutableArray<ulong>.Empty,
            ImmutableArray<ulong>.Empty,
            1,
            0,
            100);
    }

    [Fact]
    public void Constructor_FromFlatEntities_BuildsHierarchicalIndex()
    {
        var entities = new Dictionary<string, ImmutableSnapshotEntity>
        {
            { "a/b/c", CreateTestEntity() },
            { "a/d", CreateTestEntity() },
            { "e", CreateTestEntity() }
        }.ToImmutableDictionary();

        var state = new ImmutableDatasetState(entities, 1, 1);

        // Verify IndexNode is populated
        Assert.NotNull(state.IndexNode);
        Assert.False(state.IndexNode.Children.IsEmpty);
        Assert.False(state.IndexNode.Entities.IsEmpty);

        // Check structure
        Assert.True(state.IndexNode.Entities.ContainsKey("e"));
        Assert.True(state.IndexNode.Children.ContainsKey("a"));
        
        var nodeA = state.IndexNode.Children["a"];
        Assert.True(nodeA.Entities.ContainsKey("d"));
        Assert.True(nodeA.Children.ContainsKey("b"));

        var nodeB = nodeA.Children["b"];
        Assert.True(nodeB.Entities.ContainsKey("c"));
    }

    [Fact]
    public void FromProto_FromFlatSnapshot_BuildsHierarchicalIndex()
    {
        var snapshotIndex = new SnapshotIndex
        {
            SnapshotVersion = 1
        };
        snapshotIndex.Entities.Add("a/b", CreateTestEntity().ToProto());

        var state = ImmutableDatasetState.FromProto(snapshotIndex);

        // Verify IndexNode is populated
        Assert.NotNull(state.IndexNode);
        Assert.True(state.IndexNode.Children.ContainsKey("a"));
        Assert.True(state.IndexNode.Children["a"].Entities.ContainsKey("b"));
    }

    [Fact]
    public void GetAllEntities_ReturnsCorrectDictionary()
    {
        var entities = new Dictionary<string, ImmutableSnapshotEntity>
        {
            { "a/b", CreateTestEntity() },
            { "c", CreateTestEntity() }
        }.ToImmutableDictionary();

        var state = new ImmutableDatasetState(entities, 1, 1);

        var flat = state.IndexNode.GetAllEntities().ToDictionary(x => x.Key, x => x.Value);

        Assert.Equal(2, flat.Count);
        Assert.True(flat.ContainsKey("a/b"));
        Assert.True(flat.ContainsKey("c"));
    }

    [Fact]
    public void ToProto_SerializesHierarchicalIndex()
    {
        var entities = new Dictionary<string, ImmutableSnapshotEntity>
        {
            { "a/b", CreateTestEntity() }
        }.ToImmutableDictionary();

        var state = new ImmutableDatasetState(entities, 1, 1);
        var proto = state.ToProto();

        Assert.NotNull(proto.Root);
        Assert.True(proto.Root.Children.ContainsKey("a"));
    }
}
