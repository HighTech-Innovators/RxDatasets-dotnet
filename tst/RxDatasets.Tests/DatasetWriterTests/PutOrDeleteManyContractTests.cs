using RxDatasets.Abstractions;
using RxDatasets.InMemoryDataset;

namespace RxDatasets.Tests.DatasetWriterTests;

public class PutOrDeleteManyContractTests
{
    private readonly InMemoryDataset<byte[]> _dataset;
    private readonly IDatasetWriter<byte[]> _writer;

    public PutOrDeleteManyContractTests()
    {
        _dataset = new InMemoryDataset<byte[]>();
        _writer = _dataset;
    }

    private static byte[] GenerateTestData(int seed) => [.. System.Text.Encoding.UTF8.GetBytes($"test_data_{seed}")];

    [Fact]
    public async Task PutOrDeleteMany_DeleteThenPutSameEntity_ShouldReportNoChange()
    {
        // This test verifies the bug fix: delete followed by put of identical entity should report no net change

        // Arrange
        var entityName = "test_entity";
        var originalData = GenerateTestData(1);

        // First put the entity
        await _writer.Put(entityName, originalData, null, null);
        var initialVersion = await _dataset.GetLatestVersion();

        // Act: Delete then put the same entity with identical data in one batch
        var operations = new List<(string name, byte[]? entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)>
        {
            (entityName, null, null, null), // delete
            (entityName, originalData, null, null) // put back identical data
        };

        var response = await _writer.PutOrDeleteMany(operations.ToAsyncEnumerable());

        // Assert: Should report no net change
        // CRITICAL: This test MUST FAIL before the bug fix is implemented
        // Current buggy behavior: reports deletion
        // Expected fixed behavior: reports no change (all counts should be 0)
        response.NumAddedEntities.Should().Be(0, "No entities should be added when delete+put results in same state");
        response.NumUpdatedEntities.Should().Be(0, "No entities should be updated when delete+put results in same state");
        response.NumDeletedEntities.Should().Be(0, "No entities should be deleted when delete+put results in same state");
        response.NewDatasetVersion.Should().Be(initialVersion, "Dataset version should not change when no net effect occurs");

        // Verify entity still exists with original data
        var retrievedEntity = await _dataset.Get(entityName);
        retrievedEntity.Should().NotBeNull("Entity should still exist after delete+put with same data");
        retrievedEntity!.Value.entityBytes.ToArray().Should().BeEquivalentTo(originalData, "Entity data should be unchanged");
    }

    [Fact]
    public async Task PutOrDeleteMany_DeleteThenPutDifferentEntity_ShouldReportChange()
    {
        // This test verifies that delete followed by put of different entity should report a change

        // Arrange
        var entityName = "test_entity";
        var originalData = GenerateTestData(1);
        var modifiedData = GenerateTestData(2);

        // First put the entity
        await _writer.Put(entityName, originalData, null, null);
        var initialVersion = await _dataset.GetLatestVersion();

        // Act: Delete then put the same entity with different data in one batch
        var operations = new List<(string name, byte[]? entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)>
        {
            (entityName, null, null, null), // delete
            (entityName, modifiedData, null, null) // put back different data
        };

        var response = await _writer.PutOrDeleteMany(operations.ToAsyncEnumerable());

        // Assert: Should report a change
        response.NumAddedEntities.Should().Be(0, "Should not count as added since entity existed before");
        response.NumUpdatedEntities.Should().Be(1, "Should count as updated since final data differs from initial");
        response.NumDeletedEntities.Should().Be(0, "Should not count as deleted since entity exists after batch");
        response.NewDatasetVersion.Should().Be(initialVersion + 1, "Dataset version should increment when net change occurs");

        // Verify entity exists with modified data
        var retrievedEntity = await _dataset.Get(entityName);
        retrievedEntity.Should().NotBeNull("Entity should exist after delete+put with different data");
        retrievedEntity!.Value.entityBytes.ToArray().Should().BeEquivalentTo(modifiedData, "Entity should have the modified data");
    }

    [Fact]
    public async Task PutOrDeleteMany_MultiplePutsEndingWithOriginal_ShouldReportNoChange()
    {
        // This test verifies that multiple operations ending with original state should report no change

        // Arrange
        var entityName = "test_entity";
        var originalData = GenerateTestData(1);
        var tempData1 = GenerateTestData(2);
        var tempData2 = GenerateTestData(3);

        // First put the entity
        await _writer.Put(entityName, originalData, null, null);
        var initialVersion = await _dataset.GetLatestVersion();

        // Act: Multiple operations ending with original data
        var operations = new List<(string name, byte[]? entityBytes, string? trackingId, DateTimeOffset? slaTimestamp)>
        {
            (entityName, tempData1, null, null), // put different data
            (entityName, tempData2, null, null), // put more different data
            (entityName, originalData, null, null) // put back original data
        };

        var response = await _writer.PutOrDeleteMany(operations.ToAsyncEnumerable());

        // Assert: Should report no net change
        // CRITICAL: This test MUST FAIL before the bug fix is implemented
        // Current buggy behavior: likely reports update
        // Expected fixed behavior: reports no change (all counts should be 0)
        response.NumAddedEntities.Should().Be(0, "No entities should be added when ending with original state");
        response.NumUpdatedEntities.Should().Be(0, "No entities should be updated when ending with original state");
        response.NumDeletedEntities.Should().Be(0, "No entities should be deleted when ending with original state");
        response.NewDatasetVersion.Should().Be(initialVersion, "Dataset version should not change when no net effect occurs");

        // Verify entity still exists with original data
        var retrievedEntity = await _dataset.Get(entityName);
        retrievedEntity.Should().NotBeNull("Entity should still exist with original data");
        retrievedEntity!.Value.entityBytes.ToArray().Should().BeEquivalentTo(originalData, "Entity data should be back to original");
    }
}


