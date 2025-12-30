using System.Collections.Immutable;
using RxDatasets.Models;

namespace RxDatasets.Tests.Storage;

/// <summary>
/// Tests to verify snapshot and delta file structure uses the new hierarchical format.
/// </summary>
public class SnapshotFormatTests
{
    private ImmutableSnapshotEntity CreateTestEntity(long position = 0, int length = 100)
    {
        return new ImmutableSnapshotEntity(
            ImmutableArray<ulong>.Empty,
            ImmutableArray<ulong>.Empty,
            1,
            position,
            length);
    }

    [Fact]
    public void SnapshotIndex_CanSerializeHierarchicalRoot()
    {
        // Create a hierarchical index
        var entity = CreateTestEntity();
        var child = ImmutableIndexNode.Empty.AddEntity("file.txt", entity);
        var root = ImmutableIndexNode.Empty.SetChild("subdir", child);

        // Serialize to proto
        var snapshotIndex = new SnapshotIndex
        {
            SnapshotVersion = 1,
            Root = root.ToProto()
        };

        // Verify the root is present and populated
        Assert.NotNull(snapshotIndex.Root);
        Assert.NotEmpty(snapshotIndex.Root.Children);
        Assert.True(snapshotIndex.Root.Children.ContainsKey("subdir"));
    }

    [Fact]
    public void SnapshotIndex_BackwardCompatibility_PreservesLegacyEntities()
    {
        // Create a snapshot with both legacy and new structures
        var snapshotIndex = new SnapshotIndex
        {
            SnapshotVersion = 1
        };

        // Add legacy flat entities
        snapshotIndex.Entities.Add("dir/file.txt", new SnapshotEntity
        {
            LastDeletedVersion = { },
            LastInsertedVersion = { },
            LastUpdatedVersion = 1,
            Position = 0,
            Length = 100,
            Crc32 = 0
        });

        // Verify legacy structure is preserved
        Assert.NotEmpty(snapshotIndex.Entities);
        Assert.Contains("dir/file.txt", snapshotIndex.Entities.Keys);
    }

    [Fact]
    public void DeltaIndex_CanSerializeHierarchicalRoot()
    {
        // Create a hierarchical delta
        var deltaNode = new DeltaNode();
        deltaNode.ChangedEntities.Add("file.txt", new DeltaEntity
        {
            Name = "file.txt",
            Position = 100,
            Length = 50,
            Crc32 = 123
        });

        var deltaIndex = new DeltaIndex
        {
            DeltaVersion = 2,
            Root = deltaNode
        };

        // Verify the root is present and populated
        Assert.NotNull(deltaIndex.Root);
        Assert.NotEmpty(deltaIndex.Root.ChangedEntities);
    }

    [Fact]
    public void DeltaIndex_BackwardCompatibility_PreservesLegacyStructure()
    {
        var deltaIndex = new DeltaIndex
        {
            DeltaVersion = 1
        };

        // Add legacy changed entities
        deltaIndex.ChangedEntities.Add(new DeltaEntity
        {
            Name = "file.txt",
            Position = 0,
            Length = 100,
            Crc32 = 0
        });

        // Add legacy deleted names
        deltaIndex.DeletedNames.Add("oldfile.txt");

        // Verify legacy structure is preserved
        Assert.NotEmpty(deltaIndex.ChangedEntities);
        Assert.NotEmpty(deltaIndex.DeletedNames);
    }

    [Fact]
    public void ImmutableDatasetState_SerializesHierarchicalStructure()
    {
        // Create a hierarchical index
        var entity = CreateTestEntity();
        var child = ImmutableIndexNode.Empty.AddEntity("file.txt", entity);
        var indexNode = ImmutableIndexNode.Empty.SetChild("subdir", child);

        // Create an ImmutableDatasetState with the hierarchical index
        var state = new ImmutableDatasetState(
            ImmutableDictionary<string, ImmutableSnapshotEntity>.Empty,
            1,  // snapshotVersion
            1,  // latestVersion
            indexNode);

        // Convert to proto
        var protoIndex = state.ToProto();

        // Verify hierarchical structure is in the root
        Assert.NotNull(protoIndex.Root);
        Assert.NotEmpty(protoIndex.Root.Children);

        // Verify legacy flat entities are NOT populated anymore
        Assert.Empty(protoIndex.Entities);
    }

    [Fact]
    public void ImmutableDatasetState_DeserializesHierarchicalStructure()
    {
        // Create a protobuf snapshot with hierarchical root
        var protoChild = new SnapshotNode();
        protoChild.Entities.Add("file.txt", new SnapshotEntity
        {
            LastDeletedVersion = { },
            LastInsertedVersion = { },
            LastUpdatedVersion = 1,
            Position = 0,
            Length = 100,
            Crc32 = 0
        });

        var protoParent = new SnapshotNode();
        protoParent.Children.Add("subdir", protoChild);

        var snapshotIndex = new SnapshotIndex
        {
            SnapshotVersion = 1,
            Root = protoParent
        };

        // Deserialize
        var state = ImmutableDatasetState.FromProto(snapshotIndex);

        // Verify hierarchical index is loaded
        Assert.NotNull(state.IndexNode);
        Assert.NotEmpty(state.IndexNode.Children);

        // Verify flat view is also available
        Assert.NotEmpty(state.GetFlatEntities());
    }

    [Fact]
    public void RoundTrip_HierarchicalStructurePreserved()
    {
        // Create a hierarchical structure
        var entity1 = CreateTestEntity(position: 0);
        var entity2 = CreateTestEntity(position: 100);

        var level2 = ImmutableIndexNode.Empty.AddEntity("file.txt", entity1);
        var level1 = ImmutableIndexNode.Empty.SetChild("level2", level2);
        var root = ImmutableIndexNode.Empty.SetChild("level1", level1).AddEntity("root.txt", entity2);

        var state = new ImmutableDatasetState(
            ImmutableDictionary<string, ImmutableSnapshotEntity>.Empty,
            1,  // snapshotVersion
            1,  // latestVersion
            root);

        // Round-trip through proto
        var protoIndex = state.ToProto();
        var restoredState = ImmutableDatasetState.FromProto(protoIndex);

        // Verify structure
        Assert.NotNull(restoredState.IndexNode);
        Assert.NotEmpty(restoredState.IndexNode.Children);
        Assert.Contains("level1", restoredState.IndexNode.Children.Keys);

        var level1Child = restoredState.IndexNode.GetChild("level1");
        Assert.NotNull(level1Child);
        Assert.NotEmpty(level1Child.Children);

        var level2Child = level1Child.GetChild("level2");
        Assert.NotNull(level2Child);
        Assert.NotEmpty(level2Child.Entities);
    }

    [Fact]
    public void LegacySnapshot_CanBeUpgradedWithHierarchicalRoot()
    {
        // Simulate a legacy snapshot (flat only)
        var legacySnapshot = new SnapshotIndex
        {
            SnapshotVersion = 1
        };
        legacySnapshot.Entities.Add("path/to/file.txt", new SnapshotEntity
        {
            LastDeletedVersion = { },
            LastInsertedVersion = { },
            LastUpdatedVersion = 1,
            Position = 0,
            Length = 100,
            Crc32 = 0
        });

        // Load legacy snapshot
        var state = ImmutableDatasetState.FromProto(legacySnapshot);

        // Verify it loads correctly
        Assert.NotEmpty(state.Entities);
        Assert.Contains("path/to/file.txt", state.Entities.Keys);
    }

    [Fact]
    public void PreferenceForRoot_WhenBothLegacyAndNewPresent()
    {
        // Create a snapshot with both legacy and new structures
        var newRootChild = new SnapshotNode();
        newRootChild.Entities.Add("new_file.txt", new SnapshotEntity
        {
            LastDeletedVersion = { },
            LastInsertedVersion = { },
            LastUpdatedVersion = 2,
            Position = 0,
            Length = 50,
            Crc32 = 0
        });
        var newRoot = new SnapshotNode();
        newRoot.Children.Add("newdir", newRootChild);

        var snapshotIndex = new SnapshotIndex
        {
            SnapshotVersion = 2,
            Root = newRoot
        };

        // Add legacy entities (should be ignored in favor of root)
        snapshotIndex.Entities.Add("legacy/file.txt", new SnapshotEntity
        {
            LastDeletedVersion = { },
            LastInsertedVersion = { },
            LastUpdatedVersion = 1,
            Position = 0,
            Length = 100,
            Crc32 = 0
        });

        // Deserialize - should prefer root
        var state = ImmutableDatasetState.FromProto(snapshotIndex);

        Assert.NotNull(state.IndexNode);
        // Should have the new file, not legacy
        var flat = state.GetFlatEntities();
        Assert.Contains("newdir/new_file.txt", flat.Keys);
    }
}
