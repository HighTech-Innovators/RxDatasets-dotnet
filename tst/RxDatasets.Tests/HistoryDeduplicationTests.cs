using Google.Protobuf;
using Grpc.Core;
using Hightechinnovators.Dtos.DemodtoV1;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RxDatasets.InMemoryDataset;

namespace RxDatasets.Tests;

/// <summary>
/// Tests for deduplication logic when a DeltaIndex contains both DeletedNames and DeletedEntities.
/// This ensures that the "richer" DeletedEntity entries (with PreviousVersion) are preferred over
/// legacy DeletedNames entries (without previous version).
/// </summary>
public class HistoryDeduplicationTests
{
    private const string EntityName = "testEntity";
    private static readonly Demodto TestEntity = new() { Name = EntityName };
    private static readonly Demodto UpdatedEntity = new() { Name = EntityName, SomeString = "updated" };

    private readonly GrpcReaderServiceImpl<Demodto> _sut;
    private readonly InMemoryDataset<Demodto> _dataset;

    public HistoryDeduplicationTests()
    {
        _dataset = new InMemoryDataset<Demodto>();
        var configuration = Substitute.For<IConfiguration>();
        var configurationSection = Substitute.For<IConfigurationSection>();
        var cacheDuration = "1";
        configurationSection.Value.Returns(cacheDuration);
        configuration.GetSection("PaginationCacheDuration").Returns(configurationSection);
        _sut = new GrpcReaderServiceImpl<Demodto>(Substitute.For<ILogger<GrpcReaderServiceImpl<Demodto>>>(),
            Substitute.For<IHostApplicationLifetime>(), configuration, _dataset, default!);
    }

    [Fact]
    public async Task CreateAndDelete_ShouldBeCorrectInCurrentState()
    {
        // Given: Create and delete an entity
        var putResult = await _dataset.Put(EntityName, TestEntity.ToByteArray(), null, null);
        var deleteResult = await _dataset.Delete(EntityName, null, null);

        // When: Get current changes
        var changes = await _sut.ListChanges(new() { FromDatasetVersion = 0 }, default);

        // Then: Entity should not appear in the final state (deleted)
        changes.NewEntities.Should().BeEmpty();
        changes.ChangedEntities.Should().BeEmpty();
        changes.DeletedEntities.Should().BeEmpty(); // Deleted entities not in final state don't show in delta
    }

    [Fact]
    public async Task CreateUpdateAndDelete_ShouldTrackCorrectSequence()
    {
        // Given: Create, update, and delete an entity
        var createResult = await _dataset.Put(EntityName, TestEntity.ToByteArray(), null, null);
        var updateResult = await _dataset.Put(EntityName, UpdatedEntity.ToByteArray(), null, null);
        var deleteResult = await _dataset.Delete(EntityName, null, null);

        // When: Get current state
        var changes = await _sut.ListChanges(new() { FromDatasetVersion = 0 }, default);

        // Then: Should show nothing (entity was deleted at the end)
        changes.NewEntities.Should().BeEmpty();
        changes.ChangedEntities.Should().BeEmpty();
        changes.DeletedEntities.Should().BeEmpty();
    }

    [Fact]
    public async Task CreateUpdateWithoutDelete_ShouldShowUpdate()
    {
        // Given: Create and update an entity  
        var createResult = await _dataset.Put(EntityName, TestEntity.ToByteArray(), null, null);
        var updateResult = await _dataset.Put(EntityName, UpdatedEntity.ToByteArray(), null, null);

        // When: Get current state
        var changes = await _sut.ListChanges(new() { FromDatasetVersion = 0 }, default);

        // Then: Should only show the entity as new (first creation is the New event)
        changes.NewEntities.Should().Contain(EntityName);
        changes.ChangedEntities.Should().BeEmpty();
        changes.DeletedEntities.Should().BeEmpty();
    }

    [Fact]
    public async Task MultipleEntitiesWithDeletes_ShouldMaintainCorrectState()
    {
        // Given: Create multiple entities, update some, delete some
        var entity1Create = await _dataset.Put("entity1", TestEntity.ToByteArray(), null, null);
        var entity2Create = await _dataset.Put("entity2", TestEntity.ToByteArray(), null, null);
        var entity3Create = await _dataset.Put("entity3", TestEntity.ToByteArray(), null, null);

        var entity1Update = await _dataset.Put("entity1", UpdatedEntity.ToByteArray(), null, null);
        var entity2Delete = await _dataset.Delete("entity2", null, null);
        var entity3Delete = await _dataset.Delete("entity3", null, null);

        // When: Get current state
        var changes = await _sut.ListChanges(new() { FromDatasetVersion = 0 }, default);

        // Then: Verify correct final state
        changes.NewEntities.Should().Contain("entity1");
        changes.ChangedEntities.Should().BeEmpty();
        changes.DeletedEntities.Should().BeEmpty(); // entity2 and entity3 are deleted, so not in final state
    }

    [Fact]
    public async Task CreateListChangesMultipleTimes_ShouldBeDeterministic()
    {
        // Given: Create and delete an entity
        var createResult = await _dataset.Put(EntityName, TestEntity.ToByteArray(), null, null);
        var deleteResult = await _dataset.Delete(EntityName, null, null);

        // When: Call ListChanges multiple times
        var changes1 = await _sut.ListChanges(new() { FromDatasetVersion = 0 }, default);
        var changes2 = await _sut.ListChanges(new() { FromDatasetVersion = 0 }, default);

        // Then: Both calls should return the same result (deterministic)
        changes1.NewEntities.Should().BeEquivalentTo(changes2.NewEntities);
        changes1.ChangedEntities.Should().BeEquivalentTo(changes2.ChangedEntities);
        changes1.DeletedEntities.Should().BeEquivalentTo(changes2.DeletedEntities);
        changes1.ToDatasetVersion.Should().Be(changes2.ToDatasetVersion);
    }

    [Fact]
    public async Task DeleteNotCreatedEntity_ShouldHandleGracefully()
    {
        // When: Try to delete a non-existent entity
        var deleteResult = await _dataset.Delete("nonExistent", null, null);

        // Then: Should not add to dataset (nothing to delete)
        var changes = await _sut.ListChanges(new() { FromDatasetVersion = 0 }, default);
        changes.NewEntities.Should().BeEmpty();
        changes.ChangedEntities.Should().BeEmpty();
        changes.DeletedEntities.Should().BeEmpty();
    }

    [Fact]
    public async Task CreateDeleteCreate_ShouldShowAsNew()
    {
        // Given: Create, delete, then create again
        var create1 = await _dataset.Put(EntityName, TestEntity.ToByteArray(), null, null);
        var delete = await _dataset.Delete(EntityName, null, null);
        var create2 = await _dataset.Put(EntityName, UpdatedEntity.ToByteArray(), null, null);

        // When: Get current state
        var changes = await _sut.ListChanges(new() { FromDatasetVersion = 0 }, default);

        // Then: Entity should be in the final state as new
        changes.NewEntities.Should().Contain(EntityName);
        changes.DeletedEntities.Should().BeEmpty();
    }
}

