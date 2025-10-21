using RxDatasets.Abstractions;
using RxDatasets.Indexes;

namespace RxDatasets.Tests.Indexes;

/// <summary>
/// Integration tests for "last one wins" conflict resolution.
/// Verifies that when multiple entities generate the same alias, 
/// the most recently processed one wins.
/// </summary>
public class IndexConflictTests
{
    public class TestEntity { }

    [Fact]
    public async Task InMemoryReadableIndex_AliasCollision_LastOneWins_OverwritesPrevious()
    {
        // Given: Two different entities that generate the same alias
        var mockReader = Substitute.For<IDatasetReader<TestEntity>>();
        
        // Alias compute function ignores entity name, always returns same alias
        var aliasComputeFunction = (string name, ReadOnlyMemory<byte> bytes) => new[] { "shared/alias" };

        // Initialize with entity1 first
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

        // When: A second entity generates the same alias
        var entity2Bytes = new ReadOnlyMemory<byte>(new byte[] { 2 });
        mockReader.Get("entity2").Returns(("entity2", entity2Bytes));

        // Then: The latest entity (entity2) should win and be resolvable
        var result = await index.Get("shared/alias");
        
        // Should resolve to entity2 (last one wins)
        result?.name.Should().Be("entity2");
        result?.entityBytes.ToArray().Should().Equal(new byte[] { 2 });
    }

    [Fact]
    public async Task InMemoryReadableIndex_AliasCollision_FirstToSecondToThird_ThirdWins()
    {
        // Given: Three entities all generating the same alias
        var mockReader = Substitute.For<IDatasetReader<TestEntity>>();
        var aliasComputeFunction = (string name, ReadOnlyMemory<byte> bytes) => new[] { "contested/alias" };

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

        // When: Second entity also generates same alias
        mockReader.Get("entity2").Returns(("entity2", new ReadOnlyMemory<byte>(new byte[] { 2 })));

        // And third entity generates same alias
        var entity3Bytes = new ReadOnlyMemory<byte>(new byte[] { 3 });
        mockReader.Get("entity3").Returns(("entity3", entity3Bytes));

        // Then: The latest entity (entity3) should be resolvable via the alias
        var result = await index.Get("contested/alias");
        
        result?.name.Should().Be("entity3");
        result?.entityBytes.ToArray().Should().Equal(new byte[] { 3 });
    }

    [Fact]
    public async Task InMemoryReadableIndex_AliasCollision_OldEntityDeleted_NewEntityVisible()
    {
        // Given: Entity1 holds the alias
        var mockReader = Substitute.For<IDatasetReader<TestEntity>>();
        var aliasComputeFunction = (string name, ReadOnlyMemory<byte> bytes) => new[] { "alias" };

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

        // When: Entity1 is deleted (entity1 no longer owns the alias)
        mockReader.Get("entity1").Returns(_ => Task.FromResult<(string name, ReadOnlyMemory<byte> entityBytes)?>( null));

        // And entity2 also generates the same alias
        mockReader.Get("entity2").Returns(("entity2", new ReadOnlyMemory<byte>(new byte[] { 2 })));

        // Then: entity2 should now own the alias
        var result = await index.Get("alias");
        
        result?.name.Should().Be("entity2");
        result?.entityBytes.ToArray().Should().Equal(new byte[] { 2 });
    }

    [Fact]
    public async Task InMemoryReadableIndex_PartialAliasCollision_NonCollidingRemainAccessible()
    {
        // Given: Entity generates multiple aliases, some colliding with others
        var mockReader = Substitute.For<IDatasetReader<TestEntity>>();
        
        var aliasComputeFunction = (string name, ReadOnlyMemory<byte> bytes) =>
        {
            return name == "entity1" 
                ? new[] { "alias/shared", "alias/unique1" } 
                : new[] { "alias/shared", "alias/unique2" };
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

        // When: Entity2 is added (collides on shared but not unique aliases)
        mockReader.Get("entity2").Returns(("entity2", new ReadOnlyMemory<byte>(new byte[] { 2 })));

        // Then: 
        var sharedResult = await index.Get("alias/shared");      // Should resolve to entity2 (last one wins)
        var unique1Result = await index.Get("alias/unique1");    // Should resolve to entity1 (no collision)
        var unique2Result = await index.Get("alias/unique2");    // Should resolve to entity2 (no collision)

        sharedResult?.name.Should().Be("entity2");
        unique1Result?.name.Should().Be("entity1");
        unique2Result?.name.Should().Be("entity2");
    }
}
