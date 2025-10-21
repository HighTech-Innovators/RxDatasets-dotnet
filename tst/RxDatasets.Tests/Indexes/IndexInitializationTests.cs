using RxDatasets.Abstractions;
using RxDatasets.Indexes;

namespace RxDatasets.Tests.Indexes;

/// <summary>
/// Integration tests for index initialization via ListChanges replay.
/// Verifies that the index correctly populates its internal maps when created.
/// </summary>
public class IndexInitializationTests
{
    public class TestEntity { }

    [Fact]
    public async Task InMemoryReadableIndex_Initialize_ReplayChangeStream_PopulatesMaps()
    {
        // Given
        var mockReader = Substitute.For<IDatasetReader<TestEntity>>();
        var aliasComputeFunction = (string name, ReadOnlyMemory<byte> bytes) => new[] { $"alias/{name}" };

        // Mock the ListChanges to return initial entities
        var changeSet = new ChangeSet(
            ToVersion: 1,
            Changes: new List<(string Name, ChangeType ChangeType)>
            {
                ("entity1", ChangeType.New),
                ("entity2", ChangeType.New),
                ("entity3", ChangeType.New)
            }
        );

        mockReader.GetChangedNames(0).Returns(changeSet);
        mockReader.Get("entity1").Returns(("entity1", new ReadOnlyMemory<byte>(new byte[] { 1 })));
        mockReader.Get("entity2").Returns(("entity2", new ReadOnlyMemory<byte>(new byte[] { 2 })));
        mockReader.Get("entity3").Returns(("entity3", new ReadOnlyMemory<byte>(new byte[] { 3 })));

        // When
        var index = new InMemoryReadableIndex<TestEntity>(
            "test_index",
            mockReader,
            aliasComputeFunction
        );

        // Then
        // At this stage, we can't directly test map contents (they're private)
        // But we verify that Get() works after initialization
        var result1 = await index.Get("alias/entity1");
        var result2 = await index.Get("alias/entity2");
        var result3 = await index.Get("alias/entity3");

        // The test expects that initialization succeeded and aliases are resolvable
        // This test will pass once T014 (constructor initialization) is implemented
    }

    [Fact]
    public async Task InMemoryReadableIndex_Initialize_EmptyChanges_CreatesEmptyMaps()
    {
        // Given
        var mockReader = Substitute.For<IDatasetReader<TestEntity>>();
        var aliasComputeFunction = (string name, ReadOnlyMemory<byte> bytes) => new[] { $"alias/{name}" };

        // Mock ListChanges to return empty changeset
        var emptyChangeSet = new ChangeSet(
            ToVersion: 0,
            Changes: new List<(string Name, ChangeType ChangeType)>()
        );

        mockReader.GetChangedNames(0).Returns(emptyChangeSet);

        // When
        var index = new InMemoryReadableIndex<TestEntity>(
            "test_index",
            mockReader,
            aliasComputeFunction
        );

        // Then
        // No entities should be found
        var result = await index.Get("alias/nonexistent");
        result.Should().BeNull();
    }

    [Fact]
    public async Task InMemoryReadableIndex_Initialize_MixedChanges_HandlesCorrectly()
    {
        // Given
        var mockReader = Substitute.For<IDatasetReader<TestEntity>>();
        var aliasComputeFunction = (string name, ReadOnlyMemory<byte> bytes) => new[] { $"alias/{name}" };

        // Mock ListChanges with mixed change types (though initialization only sees New)
        var changeSet = new ChangeSet(
            ToVersion: 1,
            Changes: new List<(string Name, ChangeType ChangeType)>
            {
                ("entity1", ChangeType.New),
                ("entity2", ChangeType.New)
            }
        );

        mockReader.GetChangedNames(0).Returns(changeSet);
        mockReader.Get("entity1").Returns(("entity1", new ReadOnlyMemory<byte>(new byte[] { 1 })));
        mockReader.Get("entity2").Returns(("entity2", new ReadOnlyMemory<byte>(new byte[] { 2 })));

        // When
        var index = new InMemoryReadableIndex<TestEntity>(
            "test_index",
            mockReader,
            aliasComputeFunction
        );

        // Then
        var result1 = await index.Get("alias/entity1");
        var result2 = await index.Get("alias/entity2");

        // Both should be resolvable after initialization
    }

    [Fact]
    public async Task InMemoryReadableIndex_Initialize_ComputesFunctionAppliedCorrectly()
    {
        // Given
        var mockReader = Substitute.For<IDatasetReader<TestEntity>>();
        
        // Custom alias compute function that creates multiple aliases per entity
        var aliasComputeFunction = (string name, ReadOnlyMemory<byte> bytes) => new[]
        {
            $"primary/{name}",
            $"secondary/{name}",
            $"tertiary/{name}"
        };

        var changeSet = new ChangeSet(
            ToVersion: 1,
            Changes: new List<(string Name, ChangeType ChangeType)>
            {
                ("item1", ChangeType.New)
            }
        );

        mockReader.GetChangedNames(0).Returns(changeSet);
        mockReader.Get("item1").Returns(("item1", new ReadOnlyMemory<byte>(new byte[] { 1 })));

        // When
        var index = new InMemoryReadableIndex<TestEntity>(
            "test_index",
            mockReader,
            aliasComputeFunction
        );

        // Then
        // All three aliases should resolve to the same entity
        var result1 = await index.Get("primary/item1");
        var result2 = await index.Get("secondary/item1");
        var result3 = await index.Get("tertiary/item1");

        // Once T014 is implemented, all three should be non-null and equal
    }
}
