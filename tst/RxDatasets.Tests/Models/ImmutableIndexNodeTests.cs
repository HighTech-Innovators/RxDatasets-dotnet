using RxDatasets.Models;
using System.Collections.Immutable;

namespace RxDatasets.Tests.Models;

public class ImmutableIndexNodeTests
{
    private ImmutableSnapshotEntity CreateTestEntity(string name = "test", long position = 0, int length = 100)
    {
        return new ImmutableSnapshotEntity(
            ImmutableArray<ulong>.Empty,
            ImmutableArray<ulong>.Empty,
            1,
            position,
            length);
    }

    [Fact]
    public void Empty_ReturnsNodeWithNoEntitiesOrChildren()
    {
        var emptyNode = ImmutableIndexNode.Empty;

        Assert.NotNull(emptyNode);
        Assert.Empty(emptyNode.Entities);
        Assert.Empty(emptyNode.Children);
    }

    [Fact]
    public void AddEntity_AddsEntityAtNodeLevel()
    {
        var node = ImmutableIndexNode.Empty;
        var entity = CreateTestEntity();

        var updated = node.AddEntity("file.txt", entity);

        Assert.Contains("file.txt", updated.Entities.Keys);
        Assert.Equal(entity, updated.Entities["file.txt"]);
        // Original node should remain unchanged
        Assert.Empty(node.Entities);
    }

    [Fact]
    public void AddEntity_ReplaceExistingEntity()
    {
        var node = ImmutableIndexNode.Empty;
        var entity1 = CreateTestEntity(position: 0);
        var entity2 = CreateTestEntity(position: 100);

        var updated1 = node.AddEntity("file.txt", entity1);
        var updated2 = updated1.AddEntity("file.txt", entity2);

        Assert.Single(updated2.Entities);
        Assert.Equal(entity2, updated2.Entities["file.txt"]);
    }

    [Fact]
    public void RemoveEntity_RemovesEntityFromNode()
    {
        var node = ImmutableIndexNode.Empty;
        var entity = CreateTestEntity();

        var added = node.AddEntity("file.txt", entity);
        var removed = added.RemoveEntity("file.txt");

        Assert.Empty(removed.Entities);
        Assert.Contains("file.txt", added.Entities.Keys);
    }

    [Fact]
    public void RemoveEntity_NonExistentEntity_NoOp()
    {
        var node = ImmutableIndexNode.Empty;

        var result = node.RemoveEntity("nonexistent.txt");

        Assert.Empty(result.Entities);
    }

    [Fact]
    public void GetEntity_ReturnsEntityIfPresent()
    {
        var node = ImmutableIndexNode.Empty;
        var entity = CreateTestEntity();

        var added = node.AddEntity("file.txt", entity);
        var retrieved = added.GetEntity("file.txt");

        Assert.NotNull(retrieved);
        Assert.Equal(entity, retrieved);
    }

    [Fact]
    public void GetEntity_ReturnsNullIfNotPresent()
    {
        var node = ImmutableIndexNode.Empty;

        var result = node.GetEntity("nonexistent.txt");

        Assert.Null(result);
    }

    [Fact]
    public void SetChild_CreatesOrUpdatesChildNode()
    {
        var parent = ImmutableIndexNode.Empty;
        var child = ImmutableIndexNode.Empty.AddEntity("file.txt", CreateTestEntity());

        var updated = parent.SetChild("subdir", child);

        Assert.NotNull(updated.GetChild("subdir"));
        Assert.Contains("file.txt", updated.GetChild("subdir")!.Entities.Keys);
    }

    [Fact]
    public void GetOrCreateChild_ReturnsExistingChild()
    {
        var parent = ImmutableIndexNode.Empty;
        var child = ImmutableIndexNode.Empty.AddEntity("file.txt", CreateTestEntity());
        var withChild = parent.SetChild("subdir", child);

        var retrieved = withChild.GetOrCreateChild("subdir");

        Assert.Equal(child, retrieved);
    }

    [Fact]
    public void GetOrCreateChild_ReturnsEmptyNodeIfNotPresent()
    {
        var parent = ImmutableIndexNode.Empty;

        var result = parent.GetOrCreateChild("nonexistent");

        Assert.NotNull(result);
        Assert.Empty(result.Entities);
        Assert.Empty(result.Children);
    }

    [Fact]
    public void RemoveChild_RemovesChildNode()
    {
        var parent = ImmutableIndexNode.Empty;
        var child = ImmutableIndexNode.Empty;
        var withChild = parent.SetChild("subdir", child);

        var removed = withChild.RemoveChild("subdir");

        Assert.Null(removed.GetChild("subdir"));
        Assert.Empty(removed.Children);
    }

    [Fact]
    public void Navigate_FindsEntityUsingPathSegments()
    {
        // Build a tree: root/subdir/file.txt
        var child = ImmutableIndexNode.Empty.AddEntity("file.txt", CreateTestEntity());
        var root = ImmutableIndexNode.Empty.SetChild("subdir", child);

        var entity = root.Navigate(new[] { "subdir", "file.txt" });

        Assert.NotNull(entity);
    }

    [Fact]
    public void Navigate_ReturnsNullIfPathNotFound()
    {
        var node = ImmutableIndexNode.Empty;

        var result = node.Navigate(new[] { "nonexistent", "file.txt" });

        Assert.Null(result);
    }

    [Fact]
    public void Navigate_ReturnsNullForEmptyPath()
    {
        var node = ImmutableIndexNode.Empty;

        var result = node.Navigate(Array.Empty<string>());

        Assert.Null(result);
    }

    [Fact]
    public void Navigate_ReturnsEntityAtSingleLevel()
    {
        var entity = CreateTestEntity();
        var node = ImmutableIndexNode.Empty.AddEntity("file.txt", entity);

        var result = node.Navigate(new[] { "file.txt" });

        Assert.NotNull(result);
        Assert.Equal(entity, result);
    }

    [Fact]
    public void FromProto_CreatesNodeFromProtobufStructure()
    {
        var protoNode = new SnapshotNode();
        var entity = new SnapshotEntity
        {
            LastDeletedVersion = { },
            LastInsertedVersion = { },
            LastUpdatedVersion = 1,
            Position = 0,
            Length = 100,
            Crc32 = 0
        };
        protoNode.Entities.Add("file.txt", entity);

        var indexNode = ImmutableIndexNode.FromProto(protoNode);

        Assert.NotEmpty(indexNode.Entities);
        Assert.Contains("file.txt", indexNode.Entities.Keys);
    }

    [Fact]
    public void FromProto_HandlesNullNode()
    {
        var result = ImmutableIndexNode.FromProto(null!);

        Assert.Equal(ImmutableIndexNode.Empty, result);
    }

    [Fact]
    public void FromProto_RecursivelyConvertsChildren()
    {
        var protoChild = new SnapshotNode();
        var childEntity = new SnapshotEntity
        {
            LastDeletedVersion = { },
            LastInsertedVersion = { },
            LastUpdatedVersion = 1,
            Position = 0,
            Length = 50,
            Crc32 = 0
        };
        protoChild.Entities.Add("child.txt", childEntity);

        var protoParent = new SnapshotNode();
        protoParent.Children.Add("subdir", protoChild);

        var indexNode = ImmutableIndexNode.FromProto(protoParent);

        Assert.NotNull(indexNode.GetChild("subdir"));
        Assert.NotNull(indexNode.GetChild("subdir")!.GetEntity("child.txt"));
    }

    [Fact]
    public void ToProto_ConvertsNodeToProtobufStructure()
    {
        var entity = CreateTestEntity();
        var node = ImmutableIndexNode.Empty.AddEntity("file.txt", entity);

        var protoNode = node.ToProto();

        Assert.NotEmpty(protoNode.Entities);
        Assert.Contains("file.txt", protoNode.Entities.Keys);
    }

    [Fact]
    public void ToProto_RecursivelyConvertsChildren()
    {
        var childEntity = CreateTestEntity();
        var child = ImmutableIndexNode.Empty.AddEntity("child.txt", childEntity);
        var parent = ImmutableIndexNode.Empty.SetChild("subdir", child);

        var protoNode = parent.ToProto();

        Assert.NotEmpty(protoNode.Children);
        Assert.Contains("subdir", protoNode.Children.Keys);
        Assert.NotEmpty(protoNode.Children["subdir"].Entities);
    }

    [Fact]
    public void RoundTrip_ProtoToNodeToProto()
    {
        // Create a protobuf node
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
        protoParent.Entities.Add("root.txt", new SnapshotEntity
        {
            LastDeletedVersion = { },
            LastInsertedVersion = { },
            LastUpdatedVersion = 1,
            Position = 100,
            Length = 50,
            Crc32 = 0
        });
        protoParent.Children.Add("subdir", protoChild);

        // Convert to node
        var node = ImmutableIndexNode.FromProto(protoParent);

        // Convert back to proto
        var protoResult = node.ToProto();

        // Verify structure is preserved
        Assert.NotEmpty(protoResult.Entities);
        Assert.Contains("root.txt", protoResult.Entities.Keys);
        Assert.NotEmpty(protoResult.Children);
        Assert.Contains("subdir", protoResult.Children.Keys);
        Assert.NotEmpty(protoResult.Children["subdir"].Entities);
        Assert.Contains("file.txt", protoResult.Children["subdir"].Entities.Keys);
    }

    [Fact]
    public void ImmutabilityTest_NodeModificationsCreateNewInstances()
    {
        var node1 = ImmutableIndexNode.Empty;
        var entity = CreateTestEntity();
        var node2 = node1.AddEntity("file.txt", entity);

        Assert.NotEqual(node1, node2);
        Assert.Empty(node1.Entities);
        Assert.Single(node2.Entities);
    }

    [Fact]
    public void ComplexTree_BuildsMultiLevelHierarchy()
    {
        // Create: root/dir1/dir2/file.txt and root/dir1/file2.txt
        var entity1 = CreateTestEntity(position: 0);
        var entity2 = CreateTestEntity(position: 100);

        var dir2 = ImmutableIndexNode.Empty.AddEntity("file.txt", entity1);
        var dir1 = ImmutableIndexNode.Empty
            .SetChild("dir2", dir2)
            .AddEntity("file2.txt", entity2);
        var root = ImmutableIndexNode.Empty.SetChild("dir1", dir1);

        // Verify navigation
        var result1 = root.Navigate(new[] { "dir1", "dir2", "file.txt" });
        var result2 = root.Navigate(new[] { "dir1", "file2.txt" });

        Assert.NotNull(result1);
        Assert.NotNull(result2);
        Assert.Equal(entity1, result1);
        Assert.Equal(entity2, result2);
    }
}
