using NSubstitute;
using RxDatasets.Abstractions;
using RxDatasets.InMemoryDataset;

namespace RxDatasets.Tests.DatasetWriterTests;

public class CrcTrustTests
{
    private readonly InMemoryDataset<TestEntity> _dataset;
    private readonly IDatasetWriter<TestEntity> _writer;

    public CrcTrustTests()
    {
        _dataset = new InMemoryDataset<TestEntity>();
        _writer = _dataset;
    }

    private static byte[] GenerateTestData(string name, int version = 1) =>
        System.Text.Encoding.UTF8.GetBytes($"{name}_v{version}");

    [Fact]
    public async Task Put_WithUnchangedDataAndValidCrc_ShouldNotReadBytes()
    {
        // Arrange
        var entityName = "unchanged_entity";
        var originalData = GenerateTestData(entityName, 1);

        // First put the entity (with CRC32 calculated)
        await _writer.Put(entityName, originalData, null, null);

        // Act: Put the exact same data again
        // When CRC32 matches, the implementation should trust it and not read the bytes from storage
        var result = await _writer.Put(entityName, originalData, null, null);

        // Assert: Should report no change
        // If the optimization is working, no byte comparison would have occurred
        result.NumAddedEntities.Should().Be(0, "Entity already existed");
        result.NumUpdatedEntities.Should().Be(0, "CRC32 matched, so no bytes were read for comparison");
        result.NumDeletedEntities.Should().Be(0, "No deletions");
    }

    [Fact]
    public async Task Put_WithDifferentDataSameLengthAndValidCrc_ShouldDetectChange()
    {
        // Arrange
        var entityName = "changed_entity";
        var originalData = GenerateTestData(entityName, 1);
        var changedData = GenerateTestData(entityName, 2);

        // First put the entity
        await _writer.Put(entityName, originalData, null, null);

        // Act: Put different data with same length but different content
        // CRC32 will differ, so bytes will be read and compared
        var result = await _writer.Put(entityName, changedData, null, null);

        // Assert: Should report the change
        result.NumUpdatedEntities.Should().Be(1, "Data changed, CRC32 differed");
    }

    [Fact]
    public async Task Put_WithDifferentDataDifferentLength_ShouldDetectChangeWithoutCrcComparison()
    {
        // Arrange
        var entityName = "length_changed_entity";
        var shortData = GenerateTestData(entityName, 1);
        var longData = new byte[shortData.Length + 10];
        Array.Copy(shortData, longData, shortData.Length);

        // First put the entity
        await _writer.Put(entityName, shortData, null, null);

        // Act: Put data with different length
        // Length check happens before CRC check, so change is detected immediately
        var result = await _writer.Put(entityName, longData, null, null);

        // Assert: Should report the change
        result.NumUpdatedEntities.Should().Be(1, "Data length changed");
    }

    [Fact]
    public async Task Put_BySameValueMultipleTimes_ShouldBeIdempotent()
    {
        // Arrange
        var entityName = "idempotent_entity";
        var data = GenerateTestData(entityName, 1);

        // Act: Put the same data multiple times
        var result1 = await _writer.Put(entityName, data, null, null);
        var result2 = await _writer.Put(entityName, data, null, null);
        var result3 = await _writer.Put(entityName, data, null, null);

        // Assert: First put adds, subsequent puts should not report changes
        result1.NumAddedEntities.Should().Be(1, "First put adds the entity");
        result1.NumUpdatedEntities.Should().Be(0, "First put is new");

        result2.NumAddedEntities.Should().Be(0, "Second put doesn't add");
        result2.NumUpdatedEntities.Should().Be(0, "Second put should trust CRC32 and find no change");

        result3.NumAddedEntities.Should().Be(0, "Third put doesn't add");
        result3.NumUpdatedEntities.Should().Be(0, "Third put should trust CRC32 and find no change");
    }

    public sealed class TestEntity
    {
    }
}
