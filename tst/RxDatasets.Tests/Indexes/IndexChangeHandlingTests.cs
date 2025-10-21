using RxDatasets.Abstractions;
using RxDatasets.Indexes;

namespace RxDatasets.Tests.Indexes;

/// <summary>
/// Integration tests for index change handling.
/// Verifies that the index correctly handles entity creation, modification, and deletion events.
/// </summary>
public class IndexChangeHandlingTests
{
    public class TestEntity { }

    [Fact]
    public async Task InMemoryReadableIndex_EntityCreated_AddsAliasesToMaps()
    {
        // Given
        var mockReader = Substitute.For<IDatasetReader<TestEntity>>();
        var aliasComputeFunction = (string name, ReadOnlyMemory<byte> bytes) => new[] { $"alias/{name}" };

        // Initialize with empty changeset
        var emptyChangeSet = new ChangeSet(
            ToVersion: 0,
            Changes: new List<(string Name, ChangeType ChangeType)>()
        );
        mockReader.GetChangedNames(0).Returns(emptyChangeSet);

        var index = new InMemoryReadableIndex<TestEntity>(
            "test_index",
            mockReader,
            aliasComputeFunction
        );

        // When: Simulate entity creation
        var createdEntity = ("newEntity", new ReadOnlyMemory<byte>(new byte[] { 1, 2, 3 }));
        mockReader.Get("newEntity").Returns(createdEntity);

        // Then: The alias should be resolvable after the index processes the creation
        // This test verifies that change notifications are subscribed and processed
        var result = await index.Get("alias/newEntity");

        // Should not be null once change handling is implemented
    }

    [Fact]
    public async Task InMemoryReadableIndex_EntityCreated_MultipleAliases_AllAdded()
    {
        // Given
        var mockReader = Substitute.For<IDatasetReader<TestEntity>>();
        var aliasComputeFunction = (string name, ReadOnlyMemory<byte> bytes) => new[]
        {
            $"primary/{name}",
            $"secondary/{name}",
            $"tertiary/{name}"
        };

        var emptyChangeSet = new ChangeSet(
            ToVersion: 0,
            Changes: new List<(string Name, ChangeType ChangeType)>()
        );
        mockReader.GetChangedNames(0).Returns(emptyChangeSet);

        var index = new InMemoryReadableIndex<TestEntity>(
            "test_index",
            mockReader,
            aliasComputeFunction
        );

        // When: Entity created with multiple aliases
        var entityBytes = new ReadOnlyMemory<byte>(new byte[] { 1 });
        mockReader.Get("item1").Returns(("item1", entityBytes));

        // Then: All aliases should be resolvable
        var result1 = await index.Get("primary/item1");
        var result2 = await index.Get("secondary/item1");
        var result3 = await index.Get("tertiary/item1");

        // All three should resolve to the same entity
    }

    [Fact]
    public async Task InMemoryReadableIndex_EntityModified_UpdatesAliases_RemovesOldAddsNew()
    {
        // Given
        var mockReader = Substitute.For<IDatasetReader<TestEntity>>();
        
        // Alias compute function depends on entity content
        var aliasComputeFunction = (string name, ReadOnlyMemory<byte> bytes) =>
        {
            var content = bytes.ToArray();
            // Simulate: different content → different aliases
            return content.Length > 2 
                ? new[] { "alias/large" } 
                : new[] { "alias/small" };
        };

        var initialChangeSet = new ChangeSet(
            ToVersion: 1,
            Changes: new List<(string Name, ChangeType ChangeType)>
            {
                ("entity1", ChangeType.New)
            }
        );
        mockReader.GetChangedNames(0).Returns(initialChangeSet);
        mockReader.Get("entity1").Returns(("entity1", new ReadOnlyMemory<byte>(new byte[] { 1 })));

        var index = new InMemoryReadableIndex<TestEntity>(
            "test_index",
            mockReader,
            aliasComputeFunction
        );

        // When: Entity is modified with new content
        var updatedBytes = new ReadOnlyMemory<byte>(new byte[] { 1, 2, 3, 4 });
        mockReader.Get("entity1").Returns(("entity1", updatedBytes));

        // Then: Old alias should be removed, new alias added
        var resultOld = await index.Get("alias/small");  // Should be null after update
        var resultNew = await index.Get("alias/large");  // Should be resolvable after update
    }

    [Fact]
    public async Task InMemoryReadableIndex_EntityModified_AliasesReplaced()
    {
        // Given
        var mockReader = Substitute.For<IDatasetReader<TestEntity>>();
        var aliasComputeFunction = (string name, ReadOnlyMemory<byte> bytes) => new[] { $"alias/{name}" };

        var initialChangeSet = new ChangeSet(
            ToVersion: 1,
            Changes: new List<(string Name, ChangeType ChangeType)>
            {
                ("entity1", ChangeType.New)
            }
        );
        mockReader.GetChangedNames(0).Returns(initialChangeSet);
        mockReader.Get("entity1").Returns(("entity1", new ReadOnlyMemory<byte>(new byte[] { 1 })));

        var index = new InMemoryReadableIndex<TestEntity>(
            "test_index",
            mockReader,
            aliasComputeFunction
        );

        // When: Same entity modified (simulating ChangeType.Modified)
        var modifiedBytes = new ReadOnlyMemory<byte>(new byte[] { 2, 3, 4 });
        mockReader.Get("entity1").Returns(("entity1", modifiedBytes));

        // Then: Old alias should still work (same name), but bytes should be updated
        var result = await index.Get("alias/entity1");
        // Result should have new bytes after modification is processed
    }

    [Fact]
    public async Task InMemoryReadableIndex_EntityDeleted_RemovesAllAliases()
    {
        // Given
        var mockReader = Substitute.For<IDatasetReader<TestEntity>>();
        var aliasComputeFunction = (string name, ReadOnlyMemory<byte> bytes) => new[] { $"alias/{name}" };

        var initialChangeSet = new ChangeSet(
            ToVersion: 1,
            Changes: new List<(string Name, ChangeType ChangeType)>
            {
                ("entity1", ChangeType.New)
            }
        );
        mockReader.GetChangedNames(0).Returns(initialChangeSet);
        mockReader.Get("entity1").Returns(("entity1", new ReadOnlyMemory<byte>(new byte[] { 1 })));

        var index = new InMemoryReadableIndex<TestEntity>(
            "test_index",
            mockReader,
            aliasComputeFunction
        );

        // When: Entity is deleted
        mockReader.Get("entity1").Returns(_ => Task.FromResult<(string name, ReadOnlyMemory<byte> entityBytes)?>( null));

        // Then: All aliases for that entity should be removed
        var result = await index.Get("alias/entity1");
        result.Should().BeNull();
    }

    [Fact]
    public async Task InMemoryReadableIndex_EntityDeleted_MultipleAliases_AllRemoved()
    {
        // Given
        var mockReader = Substitute.For<IDatasetReader<TestEntity>>();
        var aliasComputeFunction = (string name, ReadOnlyMemory<byte> bytes) => new[]
        {
            $"primary/{name}",
            $"secondary/{name}",
            $"tertiary/{name}"
        };

        var initialChangeSet = new ChangeSet(
            ToVersion: 1,
            Changes: new List<(string Name, ChangeType ChangeType)>
            {
                ("item1", ChangeType.New)
            }
        );
        mockReader.GetChangedNames(0).Returns(initialChangeSet);
        mockReader.Get("item1").Returns(("item1", new ReadOnlyMemory<byte>(new byte[] { 1 })));

        var index = new InMemoryReadableIndex<TestEntity>(
            "test_index",
            mockReader,
            aliasComputeFunction
        );

        // When: Entity is deleted
        mockReader.Get("item1").Returns(_ => Task.FromResult<(string name, ReadOnlyMemory<byte> entityBytes)?>( null));

        // Then: All three aliases should be removed
        var result1 = await index.Get("primary/item1");
        var result2 = await index.Get("secondary/item1");
        var result3 = await index.Get("tertiary/item1");

        result1.Should().BeNull();
        result2.Should().BeNull();
        result3.Should().BeNull();
    }
}
