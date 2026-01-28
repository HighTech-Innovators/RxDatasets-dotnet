using FluentAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;

namespace RxDatasets.Tests;

/// <summary>
/// E2E Tests for Spec 067: WriteResponse Change Details with Real Storage
/// 
/// This test validates the most complex E2E scenario (PutOrDeleteMany with mixed operations)
/// using the REAL DatasetWriter with LowLevelInMemoryStorage as the underlying provider,
/// to ensure change details work correctly in realistic scenarios with detailed tracking enabled.
/// </summary>
public class WriteResponseChangeDetailsWithInMemoryStorageTests
{
    #region Setup Helper

    private static async Task<DatasetWriter<byte[]>> CreateDatasetWriterWithInMemoryStorage()
    {
        // Create the notifications hub
        var notificationsHub = new NotificationsHubSync<byte[]>();

        // Create simple in-memory storage provider with notifications hub so it can signal readiness
        var storageLogger = Substitute.For<ILogger<InMemoryStorageProvider<byte[]>>>();
        var storageProvider = new InMemoryStorageProvider<byte[]>(storageLogger, notificationsHub);

        // Create DatasetWriter with the in-memory storage provider
        var writerLogger = Substitute.For<ILogger<DatasetWriter<byte[]>>>();
        var writerConfig = Options.Create(new WriterConfig());
        var writer = new DatasetWriter<byte[]>(
            notificationsHub,
            writerLogger,
            storageProvider,
            writerConfig,
            retryInitialization: false
        );

        // Wait for initialization task to be set and then await it
        var timeout = DateTime.UtcNow.AddSeconds(5);
        while (writer.InitializationTask == null && DateTime.UtcNow < timeout)
            await Task.Delay(1);

        await writer.InitializationTask;

        return writer;
    }

    #endregion

    #region Main E2E Test: PutOrDeleteMany with Real Storage (Most Complex)

    [Fact]
    public async Task E2E_WithRealStorage_PutOrDeleteMany_IncludeDetailedChanges_True_PopulatesChanges()
    {
        // This is the most complex E2E test:
        // - Mix of 3 operations: update existing entity, delete entity, create new entity
        // - Detailed change tracking enabled
        // - Uses REAL DatasetWriter with LowLevelInMemoryStorage to validate change details work correctly
        
        var writer = await CreateDatasetWriterWithInMemoryStorage();

        // Setup: Create initial entities
        await writer.Put("order/456/item/update", new byte[] { 1, 2, 3 }, false);
        await writer.Put("order/456/item/delete", new byte[] { 4, 5, 6 }, false);

        async IAsyncEnumerable<(string, byte[]?)> GetChanges()
        {
            yield return ("order/456/item/update", new byte[] { 9, 9, 9 }); // Update
            yield return ("order/456/item/delete", null); // Delete
            yield return ("order/456/item/create", new byte[] { 7, 8, 9 }); // Create
            await Task.CompletedTask;
        }

        // Act: Perform mixed put/delete operations with detailed change tracking
        var response = await writer.PutOrDeleteMany(GetChanges(), includeDetailedChanges: true);

        // Assert: Verify change details are correctly populated
        response.DetailedChanges.Should().NotBeNull("DetailedChanges should be populated when includeDetailedChanges=true");
        response.DetailedChanges!.Count.Should().Be(3, "Should have exactly 3 changes (1 update + 1 delete + 1 create)");
        
        // Verify each change type
        response.DetailedChanges!.Should().Contain(
            c => c.EntityId == "order/456/item/update" && c.ChangeType == ChangeType.Changed,
            "Should record the update as CHANGED"
        );
        
        response.DetailedChanges!.Should().Contain(
            c => c.EntityId == "order/456/item/delete" && c.ChangeType == ChangeType.Deleted,
            "Should record the delete as DELETED"
        );
        
        response.DetailedChanges!.Should().Contain(
            c => c.EntityId == "order/456/item/create" && c.ChangeType == ChangeType.New,
            "Should record the create as NEW"
        );

        // Additional validation: verify the version was incremented
        response.NewDatasetVersion.Should().BeGreaterThan(0, "Dataset version should be greater than 0 after operations");
    }


    #endregion

    #region Additional Validation Tests with Real Storage

    [Fact]
    public async Task E2E_WithRealStorage_Put_IncludeDetailedChanges_True_PopulatesChanges()
    {
        var writer = await CreateDatasetWriterWithInMemoryStorage();

        var response = await writer.Put("store/12/label/14", new byte[] { 1, 2, 3, 4 }, includeDetailedChanges: true);

        response.NumAddedEntities.Should().Be(1);
        response.DetailedChanges.Should().NotBeNull();
        response.DetailedChanges!.Count.Should().Be(1);
        response.DetailedChanges!.First().EntityId.Should().Be("store/12/label/14");
        response.DetailedChanges!.First().ChangeType.Should().Be(ChangeType.New);
    }

    [Fact]
    public async Task E2E_WithRealStorage_Delete_IncludeDetailedChanges_True_PopulatesChanges()
    {
        var writer = await CreateDatasetWriterWithInMemoryStorage();

        // Setup
        await writer.Put("store/12/label/14", new byte[] { 1, 2, 3, 4 }, false);

        // Act
        var response = await writer.Delete("store/12/label/14", includeDetailedChanges: true);

        // Assert
        response.NumDeletedEntities.Should().Be(1);
        response.DetailedChanges.Should().NotBeNull();
        response.DetailedChanges!.Count.Should().Be(1);
        response.DetailedChanges!.First().EntityId.Should().Be("store/12/label/14");
        response.DetailedChanges!.First().ChangeType.Should().Be(ChangeType.Deleted);
    }

    [Fact]
    public async Task E2E_WithRealStorage_DeleteMany_IncludeDetailedChanges_True_PopulatesChanges()
    {
        var writer = await CreateDatasetWriterWithInMemoryStorage();

        // Setup
        await writer.Put("org/123/child/item1", new byte[] { 1, 2, 3 }, false);
        await writer.Put("org/123/child/item2", new byte[] { 4, 5, 6 }, false);

        // Act
        async IAsyncEnumerable<string> GetNames()
        {
            yield return "org/123/child/item1";
            yield return "org/123/child/item2";
            await Task.CompletedTask;
        }

        var response = await writer.DeleteMany(GetNames(), includeDetailedChanges: true);

        // Assert
        response.NumDeletedEntities.Should().Be(2);
        response.DetailedChanges.Should().NotBeNull();
        response.DetailedChanges!.Count.Should().Be(2);
        response.DetailedChanges!.Should().Contain(
            c => c.EntityId == "org/123/child/item1" && c.ChangeType == ChangeType.Deleted
        );
        response.DetailedChanges!.Should().Contain(
            c => c.EntityId == "org/123/child/item2" && c.ChangeType == ChangeType.Deleted
        );
    }

    #endregion
}

