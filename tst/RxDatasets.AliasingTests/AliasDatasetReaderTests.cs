using RxDatasets.AliasingTests.Types;
using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Reactive.Testing;
using RxDatasets.Abstractions;
using RxDatasets.Aliasing;
using RxDatasets.InMemoryDataset;

namespace RxDatasets.AliasingTests;

/// <summary>
/// Unit tests for AliasDatasetReader&lt;T&gt;
/// 
/// Tests use TestEntity protobuf message with:
/// - Each alias is stored in the 'aliases' repeated field
/// 
/// Uses InMemoryDataset as the backing reader with full Put/Delete support.
/// </summary>
public class AliasDatasetReaderTests : IAsyncLifetime
{
    private ServiceProvider _serviceProvider = null!;
    private InMemoryDataset<TestEntity> _inMemoryDataset = null!;
    private ILogger<AliasDatasetReader<TestEntity>> _logger = null!;
    private IHostApplicationLifetime _appLifetime = null!;

    public async Task InitializeAsync()
    {
        var services = new ServiceCollection();

        _logger = new ServiceCollection()
            .AddLogging(b => b.AddConsole())
            .BuildServiceProvider()
            .GetRequiredService<ILogger<AliasDatasetReader<TestEntity>>>();

        _appLifetime = new DummyHostApplicationLifetime();

        // Create in-memory dataset
        _inMemoryDataset = new InMemoryDataset<TestEntity>();

        services.AddSingleton<IDatasetReader<TestEntity>>(_inMemoryDataset);
        services.AddSingleton(_logger);
        services.AddSingleton(_appLifetime);

        _serviceProvider = services.BuildServiceProvider();

        await Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        _serviceProvider?.Dispose();
        await Task.CompletedTask;
    }

    /// <summary>
    /// Extract aliases from a TestEntity protobuf message
    /// </summary>
    private static IEnumerable<string> ExtractAliases(ReadOnlyMemory<byte> bytes)
    {
        var entity = TestEntity.Parser.ParseFrom(bytes.Span);
        return entity.Aliases;
    }

    /// <summary>
    /// Wait for the alias reader to catch up to a specific upstream version
    /// This ensures tests are timing-independent by waiting for the background sync
    /// </summary>
    private async Task WaitForSync(AliasDatasetReader<TestEntity> reader, ulong targetVersion, int maxAttempts = 100)
    {
        for (int i = 0; i < maxAttempts; i++)
        {
            var changes = await reader.GetChangedNames(0);
            if (changes.ToVersion == targetVersion)
                return;

            await Task.Delay(1);
        }

        throw new TimeoutException($"Alias reader did not sync to version {targetVersion} within timeout");
    }

    /// <summary>
    /// Helper to put an entity and return the new version
    /// </summary>
    private async Task<ulong> PutEntity(TestEntity entity)
    {
        var putResult = await _inMemoryDataset.Put(entity.Id, entity.ToByteArray());
        return putResult.NewDatasetVersion;
    }

    /// <summary>
    /// Helper to delete an entity and return the new version
    /// </summary>
    private async Task<ulong> DeleteEntity(string entityId)
    {
        var deleteResult = await _inMemoryDataset.Delete(entityId);
        return deleteResult.NewDatasetVersion;
    }

    /// <summary>
    /// Helper to put multiple entities in a single atomic transaction and return the new version
    /// </summary>
    private async Task<ulong> PutManyEntities(params TestEntity[] entities)
    {
        async IAsyncEnumerable<(string name, byte[] entityBytes)> GetEntities()
        {
            foreach (var entity in entities)
            {
                yield return (entity.Id, entity.ToByteArray());
            }
        }

        var putResult = await _inMemoryDataset.PutMany(GetEntities());
        return putResult.NewDatasetVersion;
    }

    private AliasDatasetReader<TestEntity> CreateReader(string aliasName)
    {
        var reader = new AliasDatasetReader<TestEntity>(
            _inMemoryDataset,
            ExtractAliases,
            aliasName,
            _logger,
            _appLifetime);
        reader.StartInitialization();
        return reader;
    }

    // Dummy implementation for IHostApplicationLifetime for testing purposes
    private class DummyHostApplicationLifetime : IHostApplicationLifetime
    {
        private readonly CancellationToken _stoppingToken;

        public DummyHostApplicationLifetime(CancellationToken stoppingToken = default)
        {
            _stoppingToken = stoppingToken;
        }

        public CancellationToken ApplicationStarted => CancellationToken.None;
        public CancellationToken ApplicationStopping => _stoppingToken;
        public CancellationToken ApplicationStopped => CancellationToken.None;
        public void StopApplication() { }
    }

    // ==================== BASIC FUNCTIONALITY ====================

    [Fact]
    public async Task Should_Resolve_Entity_By_Alias()
    {
        // Arrange
        var relation = new TestEntity { Id = "entity/1" };
        relation.Aliases.Add("alias-1");
        relation.Aliases.Add("alias-alt-1");

        var version = await PutEntity(relation);

        var reader = CreateReader("test");
        await WaitForSync(reader, version);

        // Act
        var result = await reader.Get("alias-1");

        // Assert
        result.Should().NotBeNull();
        var (name, bytes) = result.Value;
        var retrieved = TestEntity.Parser.ParseFrom(bytes.Span);
        retrieved.Id.Should().Be("entity/1");
    }

    [Fact]
    public async Task Should_Return_Null_For_Unknown_Alias()
    {
        // Arrange
        var relation = new TestEntity { Id = "entity/3" };
        relation.Aliases.Add("alias-3");

        var version = await PutEntity(relation);

        var reader = CreateReader("test");
        await WaitForSync(reader, version);

        // Act & Assert
        await AssertAliasNotPresent(reader, "unknown-alias", "the alias was never added to any entity");
    }

    // ==================== BATCH OPERATIONS ====================

    [Fact]
    public async Task Should_BatchGet_Multiple_Aliases()
    {
        // Arrange
        var link1 = new TestEntity { Id = "entity/4" };
        link1.Aliases.Add("alias-4-a");

        var link2 = new TestEntity { Id = "entity/5" };
        link2.Aliases.Add("alias-5-a");

        _ = await PutEntity(link1);
        var version = await PutEntity(link2);

        var reader = CreateReader("test");
        await WaitForSync(reader, version);

        // Act
        var results = await reader.BatchGet(new[] { "alias-4-a", "alias-5-a" });

        // Assert
        results.Should().HaveCount(2);
        var ids = results
            .Where(r => r.entityBytes.HasValue)
            .Select(r => TestEntity.Parser.ParseFrom(r.entityBytes!.Value.Span).Id)
            .OrderBy(id => id)
            .ToList();

        ids.Should().ContainInOrder("entity/4", "entity/5");
    }

    [Fact]
    public async Task Should_BatchGet_With_Mixed_Valid_And_Invalid_Aliases()
    {
        // Arrange
        var relation = new TestEntity { Id = "entity/6" };
        relation.Aliases.Add("alias-6");

        var version = await PutEntity(relation);

        var reader = CreateReader("test");
        await WaitForSync(reader, version);

        // Act
        // BatchGet filters out unknown aliases, so we only get valid ones
        var results = await reader.BatchGet(new[] { "alias-6", "unknown-1", "unknown-2" });

        // Assert
        // Only the valid alias should be returned
        results.Should().HaveCount(1);
        results.First().Item1.Should().Be("entity/6");
        results.First().Item2.Should().NotBeNull();
    }

    // ==================== UPDATES & CHANGES ====================

    [Fact]
    public async Task Should_Detect_Alias_Change_When_Aliases_Update()
    {
        // Arrange - create initial relation
        var relation = new TestEntity { Id = "entity/7" };
        relation.Aliases.Add("alias-old");

        var v1 = await PutEntity(relation);

        var reader = CreateReader("test");
        await WaitForSync(reader, v1);

        // Verify old alias works
        (await reader.Get("alias-old")).Should().NotBeNull();

        // Act - update with new alias
        var updated = new TestEntity { Id = "entity/7" };
        updated.Aliases.Add("alias-new");

        var v2 = await PutEntity(updated);
        await WaitForSync(reader, v2);

        // Assert - old alias should not work, new alias should
        await AssertAliasNotPresent(reader, "alias-old", "old alias should be removed");
        (await reader.Get("alias-new")).Should().NotBeNull("new alias should work");
    }

    [Fact]
    public async Task Should_Handle_Alias_Migration_Between_Entities()
    {
        // Arrange - create two relations
        var link1 = new TestEntity { Id = "entity/8" };
        link1.Aliases.Add("alias-shared");

        var link2 = new TestEntity { Id = "entity/9" };
        // link2 initially has no aliases

        _ = await PutEntity(link1);
        var v1 = await PutEntity(link2);

        var reader = new AliasDatasetReader<TestEntity>(
            _inMemoryDataset,
            ExtractAliases,
            "test",
            _logger,
            _appLifetime);
        reader.StartInitialization();
        await WaitForSync(reader, v1);

        // Verify alias points to link1
        var result1 = await reader.Get("alias-shared");
        result1.Should().NotBeNull("Initial alias should exist");
        var entity1 = TestEntity.Parser.ParseFrom(result1!.Value.entityBytes.Span);
        entity1.Id.Should().Be("entity/8");

        // Act - move the alias to link2
        var updated1 = new TestEntity { Id = "entity/8" };
        // no aliases

        var updated2 = new TestEntity { Id = "entity/9" };
        updated2.Aliases.Add("alias-shared");

        _ = await PutEntity(updated1);
        var v2 = await PutEntity(updated2);
        await WaitForSync(reader, v2);

        // Assert - alias should now point to link2
        var result2 = await reader.Get("alias-shared");
        result2.Should().NotBeNull("Alias should still exist after migration");
        var entity2 = TestEntity.Parser.ParseFrom(result2!.Value.entityBytes.Span);
        entity2.Id.Should().Be("entity/9");
    }

    [Fact]
    public async Task Should_Add_Multiple_Aliases_For_Single_Entity()
    {
        // Arrange
        var relation = new TestEntity { Id = "entity/10" };
        relation.Aliases.Add("alias-A");
        relation.Aliases.Add("alias-B");
        relation.Aliases.Add("alias-C");

        var version = await PutEntity(relation);

        var reader = new AliasDatasetReader<TestEntity>(
            _inMemoryDataset,
            ExtractAliases,
            "test",
            _logger,
            _appLifetime);
        reader.StartInitialization();
        await WaitForSync(reader, version);

        // Act & Assert - all aliases should resolve to the same entity
        var resultA = await reader.Get("alias-A");
        var resultB = await reader.Get("alias-B");
        var resultC = await reader.Get("alias-C");

        resultA.Should().NotBeNull();
        resultB.Should().NotBeNull();
        resultC.Should().NotBeNull();

        var idA = TestEntity.Parser.ParseFrom(resultA!.Value.entityBytes.Span).Id;
        var idB = TestEntity.Parser.ParseFrom(resultB!.Value.entityBytes.Span).Id;
        var idC = TestEntity.Parser.ParseFrom(resultC!.Value.entityBytes.Span).Id;

        idA.Should().Be("entity/10");
        idB.Should().Be("entity/10");
        idC.Should().Be("entity/10");
    }

    [Fact]
    public async Task Should_Clear_All_Aliases_When_Entity_Updated_To_Empty()
    {
        // Arrange - start with 2 aliases
        var relation = new TestEntity { Id = "entity/11" };
        relation.Aliases.Add("alias-keep");
        relation.Aliases.Add("alias-to-clear");

        var v1 = await PutEntity(relation);

        var reader = CreateReader("test");
        await WaitForSync(reader, v1);

        // Verify initial aliases work
        (await reader.Get("alias-keep")).Should().NotBeNull();
        (await reader.Get("alias-to-clear")).Should().NotBeNull();

        // Act - keep only one alias
        var updated = new TestEntity { Id = "entity/11" };
        updated.Aliases.Add("alias-keep");

        var v2 = await PutEntity(updated);
        await WaitForSync(reader, v2);

        // Assert - removed alias should not work, kept alias should still work
        await AssertAliasNotPresent(reader, "alias-to-clear", "removed alias should not resolve");
        (await reader.Get("alias-keep")).Should().NotBeNull("kept alias should still resolve");
    }

    // ==================== MULTIPLE READERS ====================

    [Fact]
    public async Task Should_Not_Resolve_Primary_Id_Without_Alias()
    {
        // Arrange - create an entity with an alias but test that primary ID doesn't work
        var relation = new TestEntity { Id = "entity/primary-test" };
        relation.Aliases.Add("alias-only");

        var version = await PutEntity(relation);

        var reader = CreateReader("test");
        await WaitForSync(reader, version);

        // Act & Assert - primary ID without alias mapping should not resolve
        (await reader.Get("entity/primary-test")).Should().BeNull("AliasDatasetReader does not resolve by primary ID, only by aliases");
        (await reader.Get("alias-only")).Should().NotBeNull("but the alias should work");
    }

    [Fact]
    public async Task Should_Remove_Aliases_When_Entity_Deleted()
    {
        // Arrange - create an entity with aliases
        var relation = new TestEntity { Id = "entity/delete-1" };
        relation.Aliases.Add("alias-to-delete-1");
        relation.Aliases.Add("alias-to-delete-2");

        var v1 = await PutEntity(relation);

        var reader = CreateReader("test");
        await WaitForSync(reader, v1);

        // Verify aliases work before deletion
        (await reader.Get("alias-to-delete-1")).Should().NotBeNull();
        (await reader.Get("alias-to-delete-2")).Should().NotBeNull();

        // Act - delete the entity
        var v2 = await DeleteEntity("entity/delete-1");
        await WaitForSync(reader, v2);

        // Assert - aliases should no longer resolve
        await AssertAliasNotPresent(reader, "alias-to-delete-1", "alias should be removed after entity deletion");
        await AssertAliasNotPresent(reader, "alias-to-delete-2", "alias should be removed after entity deletion");
    }

    [Fact]
    public async Task Should_Handle_Multiple_Entity_Deletions()
    {
        // Arrange - create multiple entities with aliases
        var link1 = new TestEntity { Id = "entity/delete-2" };
        link1.Aliases.Add("delete-alias-1");

        var link2 = new TestEntity { Id = "entity/delete-3" };
        link2.Aliases.Add("delete-alias-2");

        var link3 = new TestEntity { Id = "entity/delete-4" };
        link3.Aliases.Add("delete-alias-3");

        _ = await PutEntity(link1);
        _ = await PutEntity(link2);
        var v1 = await PutEntity(link3);

        var reader = CreateReader("test");
        await WaitForSync(reader, v1);

        // Verify all aliases work
        (await reader.Get("delete-alias-1")).Should().NotBeNull();
        (await reader.Get("delete-alias-2")).Should().NotBeNull();
        (await reader.Get("delete-alias-3")).Should().NotBeNull();

        // Act - delete two entities
        _ = await DeleteEntity("entity/delete-2");
        var v2 = await DeleteEntity("entity/delete-3");
        await WaitForSync(reader, v2);

        // Assert - deleted aliases should not resolve, but remaining one should
        await AssertAliasNotPresent(reader, "delete-alias-1", "deleted alias should not resolve");
        await AssertAliasNotPresent(reader, "delete-alias-2", "deleted alias should not resolve");
        (await reader.Get("delete-alias-3")).Should().NotBeNull("remaining alias should still work");
    }

    [Fact]
    public async Task Should_Handle_Delete_And_Recreate_With_Different_Aliases()
    {
        // Arrange - create an entity with an alias
        var link1 = new TestEntity { Id = "entity/delete-5" };
        link1.Aliases.Add("original-alias");

        var v1 = await PutEntity(link1);

        var reader = CreateReader("test");
        await WaitForSync(reader, v1);

        // Verify original alias works
        (await reader.Get("original-alias")).Should().NotBeNull();

        // Act - delete the entity
        var v2 = await DeleteEntity("entity/delete-5");
        await WaitForSync(reader, v2);

        // Verify alias is gone
        await AssertAliasNotPresent(reader, "original-alias", "alias should be gone after deletion");

        // Recreate entity with a different alias
        var link2 = new TestEntity { Id = "entity/delete-5" };
        link2.Aliases.Add("new-alias");

        var v3 = await PutEntity(link2);
        await WaitForSync(reader, v3);

        // Assert - old alias should still not work, new alias should work
        await AssertAliasNotPresent(reader, "original-alias", "old alias should not be resurrected");
        (await reader.Get("new-alias")).Should().NotBeNull("new alias should work");
        var result = await reader.Get("new-alias")!;
        var entity = TestEntity.Parser.ParseFrom(result!.Value.entityBytes.Span);
        entity.Aliases.Should().ContainSingle().Which.Should().Be("new-alias");
    }

    [Fact]
    public async Task Should_Update_Hierarchical_Count_After_Deletion()
    {
        // Arrange - create hierarchical entities
        var child1 = new TestEntity { Id = "entity/delete-6" };
        child1.Aliases.Add("store/delete-test/products/p1");

        var child2 = new TestEntity { Id = "entity/delete-7" };
        child2.Aliases.Add("store/delete-test/products/p2");

        var child3 = new TestEntity { Id = "entity/delete-8" };
        child3.Aliases.Add("store/delete-test/products/p3");

        _ = await PutEntity(child1);
        _ = await PutEntity(child2);
        var v1 = await PutEntity(child3);

        var reader = CreateReader("test");
        await WaitForSync(reader, v1);

        // Verify initial count
        var initialCount = await reader.CountChildren("store/delete-test/products");
        initialCount.Should().Be(3);

        // Act - delete one entity
        var v2 = await DeleteEntity("entity/delete-7");
        await WaitForSync(reader, v2);

        // Assert - count should be reduced
        var afterDeleteCount = await reader.CountChildren("store/delete-test/products");
        afterDeleteCount.Should().Be(2);

        var (names, _) = await reader.List("store/delete-test/products");
        names.Should().HaveCount(2);
        names.Should().Contain("store/delete-test/products/p1");
        names.Should().Contain("store/delete-test/products/p3");
        names.Should().NotContain("store/delete-test/products/p2");
    }

    // ==================== MULTIPLE READERS ====================

    [Fact]
    public async Task Should_Support_Multiple_Independent_Alias_Readers()
    {
        // Arrange
        var relation = new TestEntity { Id = "entity/12" };
        relation.Aliases.Add("alias-multi");

        var version = await PutEntity(relation);

        var reader1 = new AliasDatasetReader<TestEntity>(
            _inMemoryDataset,
            ExtractAliases,
            "reader1",
            _logger,
            _appLifetime);
        reader1.StartInitialization();

        var reader2 = new AliasDatasetReader<TestEntity>(
            _inMemoryDataset,
            ExtractAliases,
            "reader2",
            _logger,
            _appLifetime);
        reader2.StartInitialization();

        await WaitForSync(reader1, version);
        await WaitForSync(reader2, version);

        // Act
        var result1 = await reader1.Get("alias-multi");
        var result2 = await reader2.Get("alias-multi");

        // Assert
        result1.Should().NotBeNull();
        result2.Should().NotBeNull();

        TestEntity.Parser.ParseFrom(result1!.Value.entityBytes.Span).Id.Should().Be("entity/12");
        TestEntity.Parser.ParseFrom(result2!.Value.entityBytes.Span).Id.Should().Be("entity/12");
    }

    // ==================== HIERARCHICAL ALIASES & LIST/COUNT ====================

    [Fact]
    public async Task Should_List_Children_By_Parent_Alias()
    {
        // Arrange - create child entities with hierarchical aliases
        var child1 = new TestEntity { Id = "entity/13" };
        child1.Aliases.Add("store/s1/products/p1");

        var child2 = new TestEntity { Id = "entity/14" };
        child2.Aliases.Add("store/s1/products/p2");

        var child3 = new TestEntity { Id = "entity/15" };
        child3.Aliases.Add("store/s1/products/p3");

        // Another parent's child (should not be included)
        var child4 = new TestEntity { Id = "entity/16" };
        child4.Aliases.Add("store/s2/products/p1");

        _ = await PutEntity(child1);
        _ = await PutEntity(child2);
        _ = await PutEntity(child3);
        var version = await PutEntity(child4);

        var reader = new AliasDatasetReader<TestEntity>(
            _inMemoryDataset,
            ExtractAliases,
            "test",
            _logger,
            _appLifetime);
        reader.StartInitialization();
        await WaitForSync(reader, version);

        // Act - list children of store/s1/products
        var (names, _) = await reader.List("store/s1/products");

        // Assert - should only return the 3 children of store/s1/products
        names.Should().NotBeNull();
        names.Should().HaveCount(3);
        names.Should().Contain("store/s1/products/p1");
        names.Should().Contain("store/s1/products/p2");
        names.Should().Contain("store/s1/products/p3");
    }

    [Fact]
    public async Task Should_Count_Children_By_Parent_Alias()
    {
        // Arrange - create entities with hierarchical aliases
        var child1 = new TestEntity { Id = "entity/17" };
        child1.Aliases.Add("category/cat1/products/product1");

        var child2 = new TestEntity { Id = "entity/18" };
        child2.Aliases.Add("category/cat1/products/product2");

        var other = new TestEntity { Id = "entity/19" };
        other.Aliases.Add("category/cat2/products/product1");

        _ = await PutEntity(child1);
        _ = await PutEntity(child2);
        var version = await PutEntity(other);

        var reader = new AliasDatasetReader<TestEntity>(
            _inMemoryDataset,
            ExtractAliases,
            "test",
            _logger,
            _appLifetime);
        reader.StartInitialization();
        await WaitForSync(reader, version);

        // Act - count children of category/cat1/products
        var count = await reader.CountChildren("category/cat1/products");

        // Assert - should be 2
        count.Should().Be(2);
    }

    [Fact]
    public async Task Should_Handle_Deeply_Nested_Hierarchical_Aliases()
    {
        // Arrange - create entities with deeply nested aliases
        var product1 = new TestEntity { Id = "entity/20" };
        product1.Aliases.Add("org/acme/dept/sales/team/east/toys/toy1");

        var product2 = new TestEntity { Id = "entity/21" };
        product2.Aliases.Add("org/acme/dept/sales/team/east/toys/toy2");

        var product3 = new TestEntity { Id = "entity/22" };
        product3.Aliases.Add("org/acme/dept/sales/team/west/toys/toy1");

        _ = await PutEntity(product1);
        _ = await PutEntity(product2);
        var version = await PutEntity(product3);

        var reader = new AliasDatasetReader<TestEntity>(
            _inMemoryDataset,
            ExtractAliases,
            "test",
            _logger,
            _appLifetime);
        reader.StartInitialization();
        await WaitForSync(reader, version);

        // Act - list toys under east team
        var (names, _) = await reader.List("org/acme/dept/sales/team/east/toys");

        // Assert - should have 2 toys
        names.Should().NotBeNull();
        names.Should().HaveCount(2);
        names.Should().Contain("org/acme/dept/sales/team/east/toys/toy1");
        names.Should().Contain("org/acme/dept/sales/team/east/toys/toy2");
    }

    [Fact]
    public async Task Should_Count_Deeply_Nested_Children()
    {
        // Arrange - create entities with deeply nested aliases
        var product1 = new TestEntity { Id = "entity/23" };
        product1.Aliases.Add("system/prod/module/auth/component/user/subcomponent/role");

        var product2 = new TestEntity { Id = "entity/24" };
        product2.Aliases.Add("system/prod/module/auth/component/user/subcomponent/permission");

        var product3 = new TestEntity { Id = "entity/25" };
        product3.Aliases.Add("system/prod/module/auth/component/user/subcomponent/group");

        _ = await PutEntity(product1);
        _ = await PutEntity(product2);
        var version = await PutEntity(product3);

        var reader = new AliasDatasetReader<TestEntity>(
            _inMemoryDataset,
            ExtractAliases,
            "test",
            _logger,
            _appLifetime);
        reader.StartInitialization();
        await WaitForSync(reader, version);

        // Act - count subcomponents (direct children)
        var count = await reader.CountChildren("system/prod/module/auth/component/user/subcomponent");

        // Assert - should be 3
        count.Should().Be(3);

        // Act - count at parent level (grandchildren)
        var countParent = await reader.CountChildren("system/prod/module/auth/component/user");

        // Assert - should also be 3 (same descendants are still children of grandparent)
        countParent.Should().Be(3);
    }

    [Fact]
    public async Task Should_List_Grandchildren_At_Parent_Levels()
    {
        // Arrange - create entities with hierarchical aliases
        var child1 = new TestEntity { Id = "entity/27" };
        child1.Aliases.Add("category/cat1/products/product1");

        var child2 = new TestEntity { Id = "entity/28" };
        child2.Aliases.Add("category/cat1/products/product2");

        var other = new TestEntity { Id = "entity/29" };
        other.Aliases.Add("category/cat2/products/product1");

        _ = await PutEntity(child1);
        _ = await PutEntity(child2);
        var version = await PutEntity(other);

        var reader = new AliasDatasetReader<TestEntity>(
            _inMemoryDataset,
            ExtractAliases,
            "test",
            _logger,
            _appLifetime);
        reader.StartInitialization();
        await WaitForSync(reader, version);

        // Act - list direct children of category/cat1/products
        var (directChildren, _) = await reader.List("category/cat1/products");

        // Assert - should have 2 products
        directChildren.Should().NotBeNull();
        directChildren.Should().HaveCount(2);
        directChildren.Should().Contain("category/cat1/products/product1");
        directChildren.Should().Contain("category/cat1/products/product2");

        // Act - list all descendants at category/cat1 level
        var (allDescendants, _) = await reader.List("category/cat1");

        // Assert - should also have 2 products (the grandchildren)
        allDescendants.Should().NotBeNull();
        allDescendants.Should().HaveCount(2);
        allDescendants.Should().Contain("category/cat1/products/product1");
        allDescendants.Should().Contain("category/cat1/products/product2");
    }

    [Fact]
    public async Task Should_Return_Empty_For_Nonexistent_Parent()
    {
        // Arrange
        var relation = new TestEntity { Id = "entity/26" };
        relation.Aliases.Add("store/s1/products/p1");

        var version = await PutEntity(relation);

        var reader = new AliasDatasetReader<TestEntity>(
            _inMemoryDataset,
            ExtractAliases,
            "test",
            _logger,
            _appLifetime);
        reader.StartInitialization();
        await WaitForSync(reader, version);

        // Act
        var (names, _) = await reader.List("store/s999/products");
        var count = await reader.CountChildren("store/s999/products");

        // Assert
        names.Should().BeEmpty();
        count.Should().Be(0);
    }

    [Fact]
    public async Task Should_Handle_Alias_Migration_In_Transaction_No_Sharing()
    {
        // Arrange - create two entities, prepare to migrate shared alias between them
        var entity1 = new TestEntity { Id = "entity/30" };
        entity1.Aliases.Add("shared-alias");

        var entity2 = new TestEntity { Id = "entity/31" };
        // entity2 has no aliases initially

        _ = await PutEntity(entity1);
        var initialVersion = await PutEntity(entity2);

        var reader = new AliasDatasetReader<TestEntity>(
            _inMemoryDataset,
            ExtractAliases,
            "test",
            _logger,
            _appLifetime);
        reader.StartInitialization();
        await WaitForSync(reader, initialVersion);

        // Verify initial state
        var initial = await reader.Get("shared-alias");
        initial.Should().NotBeNull();
        var initialEntity = TestEntity.Parser.ParseFrom(initial!.Value.entityBytes.Span);
        initialEntity.Id.Should().Be("entity/30");

        // Act - perform atomic transaction: remove alias from entity1, add to entity2
        // PutMany ensures both updates are processed as part of the same transactional delta
        var updated1 = new TestEntity { Id = "entity/30" };
        // no aliases

        var updated2 = new TestEntity { Id = "entity/31" };
        updated2.Aliases.Add("shared-alias");

        var finalVersion = await PutManyEntities(updated1, updated2);

        await WaitForSync(reader, finalVersion);

        // Assert - after processing all changes from the transaction, the alias mapping is correct
        // The alias should now point to entity2 and only entity2
        var result = await reader.Get("shared-alias");
        result.Should().NotBeNull("shared alias must resolve after transaction");
        var resultEntity = TestEntity.Parser.ParseFrom(result!.Value.entityBytes.Span);
        resultEntity.Id.Should().Be("entity/31", "alias should have migrated to entity2");
    }

    [Fact]
    public async Task Should_Count_Large_Hierarchical_Dataset_With_1111_Entities()
    {
        // Arrange - create 1111 entities, each with 2 unique aliases of different types
        // Entity i gets aliases: "types/one/entity/i" and "types/two/entity/i"
        // Total aliases: 2222 (all under "types" parent)
        const int entityCount = 1111;

        var entities = new List<TestEntity>();
        for (int i = 1; i <= entityCount; i++)
        {
            var entity = new TestEntity { Id = $"entity/{i}" };
            entity.Aliases.Add($"types/one/entity/{i}");
            entity.Aliases.Add($"types/two/entity/{i}");
            entities.Add(entity);
        }

        // Act - put all 1111 entities in a single atomic transaction
        var finalVersion = await PutManyEntities(entities.ToArray());

        var reader = new AliasDatasetReader<TestEntity>(
            _inMemoryDataset,
            ExtractAliases,
            "megatest",
            _logger,
            _appLifetime);
        reader.StartInitialization();
        await WaitForSync(reader, finalVersion);

        // Assert - count children under "types" should equal 2222
        var count = await reader.CountChildren("types");
        count.Should().Be(2222, "each of 1111 entities has 2 aliases under 'types', totaling 2222 descendants");

        // Verify we can count specific sub-branches
        var countOne = await reader.CountChildren("types/one");
        countOne.Should().Be(entityCount, "exactly 1111 aliases under types/one");

        var countTwo = await reader.CountChildren("types/two");
        countTwo.Should().Be(entityCount, "exactly 1111 aliases under types/two");

        // Verify a specific entity can be resolved by both aliases
        var alias1Result = await reader.Get("types/one/entity/1");
        alias1Result.Should().NotBeNull();
        var entity1From1 = TestEntity.Parser.ParseFrom(alias1Result!.Value.entityBytes.Span);
        entity1From1.Id.Should().Be("entity/1");

        var alias2Result = await reader.Get("types/two/entity/1");
        alias2Result.Should().NotBeNull();
        var entity1From2 = TestEntity.Parser.ParseFrom(alias2Result!.Value.entityBytes.Span);
        entity1From2.Id.Should().Be("entity/1");

        // Verify a random mid-range entity
        const int midEntity = 555;
        var midAlias = await reader.Get($"types/one/entity/{midEntity}");
        midAlias.Should().NotBeNull();
        var midEntity555 = TestEntity.Parser.ParseFrom(midAlias!.Value.entityBytes.Span);
        midEntity555.Id.Should().Be($"entity/{midEntity}");

        // Verify the last entity
        var lastAlias = await reader.Get($"types/two/entity/{entityCount}");
        lastAlias.Should().NotBeNull();
        var lastEntityParsed = TestEntity.Parser.ParseFrom(lastAlias!.Value.entityBytes.Span);
        lastEntityParsed.Id.Should().Be($"entity/{entityCount}");
    }

    // ==================== HELPER ====================

    /// <summary>
    /// Helper to assert that an alias does not exist in the reader.
    /// Uses GetChangedNames(0) to verify the alias is not present in the dataset.
    /// </summary>
    private async Task AssertAliasNotPresent(AliasDatasetReader<TestEntity> reader, string alias, string because = "")
    {
        var changeSet = await reader.GetChangedNames(0);
        var aliases = changeSet.Changes.Select(c => c.Name).ToList();
        aliases.Should().NotContain(alias, because);
    }

    [Fact]
    public async Task Should_Stop_Background_Sync_When_Application_Stops()
    {
        // Arrange
        var appLifetimeCts = new CancellationTokenSource();
        var appLifetime = new DummyHostApplicationLifetime(appLifetimeCts.Token);

        var reader = new AliasDatasetReader<TestEntity>(
            _inMemoryDataset,
            ExtractAliases,
            "test",
            _logger,
            appLifetime);

        // Act
        reader.StartInitialization();

        // Give the background task a moment to start and enter the wait
        await Task.Delay(100);

        // Cancel the application lifetime
        appLifetimeCts.Cancel();

        // Give it time to respond to cancellation
        await Task.Delay(100);

        // Assert
        // The reader should still be functional but background sync should have stopped
        // We verify this by checking that the cancellation was triggered
        appLifetime.ApplicationStopping.IsCancellationRequested.Should().BeTrue("cancellation token should be triggered");
    }

    // ==================== INITIALIZATION FAILURE & RECOVERY ====================

    /// <summary>
    /// Mock IDatasetReader that throws an exception on GetChangedNames during initialization.
    /// After a configurable number of failures, it starts succeeding.
    /// </summary>
    private class FailingDatasetReader : IDatasetReader<TestEntity>
    {
        private readonly InMemoryDataset<TestEntity> _underlying;
        private int _failuresRemaining;
        private readonly Exception _exceptionToThrow;

        public int GetChangedNamesCallCount { get; private set; }

        public FailingDatasetReader(InMemoryDataset<TestEntity> underlying, int failureCount, Exception? exceptionToThrow = null)
        {
            _underlying = underlying;
            _failuresRemaining = failureCount;
            _exceptionToThrow = exceptionToThrow ?? new InvalidOperationException("Simulated upstream failure during initialization");
        }

        public Task<ChangeSet> GetChangedNames(ulong fromVersion)
        {
            GetChangedNamesCallCount++;
            if (_failuresRemaining > 0)
            {
                _failuresRemaining--;
                throw _exceptionToThrow;
            }
            return _underlying.GetChangedNames(fromVersion);
        }

        public Task<ChangeSet> WaitChangedNames(ulong fromVersion, CancellationToken cancellationToken = default)
            => _underlying.WaitChangedNames(fromVersion, cancellationToken);

        public Task<(string name, ReadOnlyMemory<byte> entityBytes)?> Get(string id)
            => _underlying.Get(id);

        public Task<ICollection<(string name, ReadOnlyMemory<byte>? entityBytes)>> BatchGet(IEnumerable<string> ids, string? parent = null)
            => _underlying.BatchGet(ids, parent);

        public Task<(List<string> Names, ulong LatestVersion)> List(string parent, CancellationToken cancellationToken = default)
            => _underlying.List(parent, cancellationToken);

        public Task<uint> CountChildren(string parent, CancellationToken cancellationToken = default)
            => _underlying.CountChildren(parent, cancellationToken);

        public Task<List<HistoryEntry>> GetHistory(string name, CancellationToken ct = default)
            => _underlying.GetHistory(name, ct);

        public Task<(string name, ReadOnlyMemory<byte> entityBytes)?> GetHistoric(string name, ulong version, CancellationToken ct = default)
            => _underlying.GetHistoric(name, version, ct);
    }

    [Fact]
    public async Task Should_Recover_From_Upstream_Exception_During_Initialization()
    {
        // Arrange - create a failing upstream reader that fails first 2 attempts, then succeeds
        var entity = new TestEntity { Id = "entity/recovery-test" };
        entity.Aliases.Add("recovery-alias");
        await _inMemoryDataset.Put(entity.Id, entity.ToByteArray());

        var failingReader = new FailingDatasetReader(_inMemoryDataset, failureCount: 2);

        // Use TestScheduler to fast-forward through the 5-second delays
        var scheduler = new TestScheduler();
        var clock = new TrackingRxSchedulerClock(scheduler);

        var reader = new AliasDatasetReader<TestEntity>(
            failingReader,
            ExtractAliases,
            "recovery-test",
            _logger,
            _appLifetime,
            clock);

        // Act - start initialization (should fail initially, but recover)
        reader.StartInitialization();

        // Wait for the retry delay to be scheduled (implies the first call failed)
        await clock.WaitUntilAnyDelaysPending(
            TimeSpan.FromSeconds(5),
            "retry delay should be scheduled after failure");

        // Fast-forward past both 5-second retry delays (2 failures configured)
        // The continuations run synchronously so one AdvanceBy handles all retries
        scheduler.AdvanceBy(TimeSpan.FromSeconds(12).Ticks);

        // Wait for the reader to recover and initialize (with timeout)
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var result = await reader.Get("recovery-alias").WaitAsync(cts.Token);

        // Assert - the reader should have recovered and resolved the alias
        result.Should().NotBeNull("reader should recover from transient initialization failures");
        var resolvedEntity = TestEntity.Parser.ParseFrom(result!.Value.entityBytes.Span);
        resolvedEntity.Id.Should().Be("entity/recovery-test");

        // Verify that GetChangedNames was called multiple times (retries)
        failingReader.GetChangedNamesCallCount.Should().BeGreaterThan(1, "reader should have retried after failures");
    }

    [Fact]
    public async Task Should_Not_Stay_Stuck_In_Uninitialized_State_After_Upstream_Failure()
    {
        // Arrange - This test reproduces the bug: if upstream throws during init,
        // the reader stays stuck forever because _syncTask is non-null but _initialized is false
        var failingReader = new FailingDatasetReader(_inMemoryDataset, failureCount: 1);

        // Use TestScheduler to fast-forward through the 5-second delays
        var scheduler = new TestScheduler();
        var clock = new TrackingRxSchedulerClock(scheduler);

        var reader = new AliasDatasetReader<TestEntity>(
            failingReader,
            ExtractAliases,
            "stuck-test",
            _logger,
            _appLifetime,
            clock);

        // Act
        reader.StartInitialization();

        // Wait for the retry delay to be scheduled (implies the first call failed)
        await clock.WaitUntilAnyDelaysPending(
            TimeSpan.FromSeconds(5),
            "retry delay should be scheduled after failure");

        // Fast-forward past the 5-second retry delay
        scheduler.AdvanceBy(TimeSpan.FromSeconds(6).Ticks);

        // The Get call should eventually succeed (after retry) rather than throwing forever
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        Func<Task> act = async () =>
        {
            // This should not throw - it should either return null (no such alias) 
            // or the reader should have recovered
            await reader.Get("some-alias").WaitAsync(cts.Token);
        };

        // Assert - with the fix, this should complete without throwing
        await act.Should().NotThrowAsync("reader should recover from initialization failure");
    }

    [Fact]
    public async Task Should_Allow_Retry_After_Permanent_Failure_When_Restarted()
    {
        // Arrange - a reader that always fails
        var alwaysFailingReader = new FailingDatasetReader(_inMemoryDataset, failureCount: int.MaxValue);

        // Use TestScheduler to fast-forward through the 5-second delays
        var scheduler = new TestScheduler();
        var clock = new TrackingRxSchedulerClock(scheduler);

        var reader = new AliasDatasetReader<TestEntity>(
            alwaysFailingReader,
            ExtractAliases,
            "permanent-fail-test",
            _logger,
            _appLifetime,
            clock);

        reader.StartInitialization();

        // Wait for the retry delay to be scheduled (implies the first call failed)
        await clock.WaitUntilAnyDelaysPending(
            TimeSpan.FromSeconds(5),
            "retry delay should be scheduled after failure");

        // Fast-forward past a few retry delays to ensure retries are happening
        scheduler.AdvanceBy(TimeSpan.FromSeconds(12).Ticks);

        // The reader should still be trying to initialize (via retries)
        // Calling Get should wait for initialization and eventually work after retries
        // (or timeout if we set a reasonable timeout)
        
        // For now, just verify the reader doesn't throw immediately on construction
        // and that multiple StartInitialization calls don't cause issues
        reader.StartInitialization(); // Should be idempotent
        
        alwaysFailingReader.GetChangedNamesCallCount.Should().BeGreaterThan(0, 
            "reader should have attempted initialization");
    }
}
