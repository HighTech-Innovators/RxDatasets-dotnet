using System.Collections.Immutable;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using RxDatasets.Abstractions;
using RxDatasets.Models;

namespace RxDatasets.StorageTests.Common;

public class DatasetHelperTests
{
    [Fact]
    public void GetRandomTempFilePath_Should_Return_CustomTempPathWhenConfigured()
    {
        // Arrange
        var datasetHelper = new DatasetHelper(Mock.Of<ILogger<DatasetHelper>>(), Mock.Of<IStorageProvider<object>>());
        const string customPath = "tempOfTests";
        Environment.SetEnvironmentVariable("TEMP_FILE_DIRECTORY", customPath);

        // Act
        var result = DatasetHelper.GetTempFilePath();

        // Assert
        result.Should().StartWith(customPath);
    }

    [Fact]
    public void GetRandomTempFilePath_Should_Return_SystemTempPathWhenNotConfigured()
    {
        // Arrange
        var datasetHelper = new DatasetHelper(Mock.Of<ILogger<DatasetHelper>>(), Mock.Of<IStorageProvider<object>>());

        // Act
        var result = DatasetHelper.GetTempFilePath();

        // Assert
        result.Should().StartWith(Path.GetTempPath());
    }


    // Existing default behavior tests (no level parameter)
    [Theory]
    [InlineData("channels/64090/days/20240923/events/287431128", "channels/64090/days/20240923")]
    [InlineData("bla/A/bla/K", "bla/A")]
    [InlineData("prov/bbc.nl/pack/BBCC1234/video/123/singlechild", "prov/bbc.nl/pack/BBCC1234/video/123")]
    [InlineData("root/child", null)]
    [InlineData("singleword", null)]
    [InlineData("parent/child/", null)]
    public void FindParentOfEntity_Default_ShouldReturnCorrectParent(string input, string? expected)
    {
        // Act
        var result = DatasetHelper.FindParentOfEntity(input);
        // Assert
        result.Should().Be(expected);
    }

    // New tests with the level parameter.
    // The level represents the number of segments from the parent's value (match.Groups[1].Value) to return.
    // For example, given:
    // "channels/64090/days/20240923/events/287431128" -> default parent is "channels/64090/days/20240923"
    // - Level 2 returns "channels/64090" (first 2 segments)
    // - Level 3 returns "channels/64090/days"
    // - Level 4 returns "channels/64090/days/20240923"
    [Theory]
    [InlineData("channels/64090/days/20240923/events/287431128", 2, "channels/64090")]
    [InlineData("channels/64090/days/20240923/events/287431128", 3, "channels/64090/days")]
    [InlineData("channels/64090/days/20240923/events/287431128", 4, "channels/64090/days/20240923")]
    [InlineData("bla/A/bla/K", 1, "bla")]
    [InlineData("bla/A/bla/K", 2, "bla/A")]
    [InlineData("bla/A/bla/K", 3, "bla/A/bla")] // Only two segments available in default parent ("bla/A")
    public void FindParentOfEntity_WithLevel_ShouldReturnCorrectParent(string input, int level, string? expected)
    {
        // Act
        var result = DatasetHelper.FindParentOfEntity(input, level);
        // Assert
        result.Should().Be(expected);
    }


    [Fact]
    public void DeleteImplicitly_Should_Return_Entities_Not_In_ReceivedNames_And_Not_Deleted()
    {
        // Arrange
        ImmutableDictionary<string, ImmutableSnapshotEntity> entities = new Dictionary<string, ImmutableSnapshotEntity>
        {
            { "parent1/child1/bla/1", new ImmutableSnapshotEntity(0ul, 0, 0) },
            { "parent1/child2/bla/2", new ImmutableSnapshotEntity(0ul, 0, 0) },
            { "parent2/child1/bla/1", new ImmutableSnapshotEntity(1ul, 0, 1) },
            { "parent2/child2/bla/2", new ImmutableSnapshotEntity(0ul, 0, 0) },
            { "parent3/child1/bla/1", new ImmutableSnapshotEntity(1ul, 0, 1) },
            { "parent3/child1/bla/2", new ImmutableSnapshotEntity(0ul, 0, 0) },
            { "parent3/child2/bla/1", new ImmutableSnapshotEntity(0ul, 0, 0) },
            { "parent3/child3/bla/1", new ImmutableSnapshotEntity(0ul, 0, 0) },
        }.ToImmutableDictionary();

        var index = ImmutableDatasetState.FromEntities(entities, 0, 0);

        var receivedNames = new HashSet<string> { "parent2/child1/bla/1", "parent3/child1/bla/1" };

        // Act
        var result = DatasetHelper.GetImplicitDeletes(index, receivedNames);

        // Assert
        result.Should().BeEquivalentTo("parent1/child2/bla/2", "parent1/child1/bla/1", "parent2/child2/bla/2", "parent3/child1/bla/2", "parent3/child2/bla/1", "parent3/child3/bla/1");
    }

    [Fact]
    public void DeleteUsingParents_Should_Return_Entities_Of_Parents_Not_In_ReceivedNames_And_Not_Deleted()
    {
        // Arrange
        var entities = new Dictionary<string, ImmutableSnapshotEntity>
        {
            { "parent1/child1/bla/1", new ImmutableSnapshotEntity(0ul, 0, 0) },
            { "parent2/child1/bla/1", new ImmutableSnapshotEntity(0ul, 0, 0) },
            { "parent2/child1/bla/2", new ImmutableSnapshotEntity(0ul, 0, 0) },
            { "parent2/child1/bla/3", new ImmutableSnapshotEntity(0ul, 0, 0) },
            { "parent2/child2/bla/1", new ImmutableSnapshotEntity(0ul, 0, 0) },
            { "parent3/child1/bla/1", new ImmutableSnapshotEntity(0ul, 0, 0) },
            { "parent3/child1/bla/2", new ImmutableSnapshotEntity(0ul, 0, 0) },
            { "parent3/child2/bla/1", new ImmutableSnapshotEntity(0ul, 0, 0) },
        }.ToImmutableDictionary();

        var index = ImmutableDatasetState.FromEntities(entities, 0, 0);

        var receivedNames = new HashSet<string>
            { "parent2/child1/bla/1", "parent3/child1/bla/1" }; // all siblings of those should be deleted
        var allParents = new HashSet<string> { "parent2/child1", "parent3/child1" };

        // Act
        var result = DatasetHelper.GetImplicitDeletesForParentNames(index, receivedNames, allParents);

        // Assert
        result.Should().BeEquivalentTo("parent2/child1/bla/2", "parent2/child1/bla/3", "parent3/child1/bla/2");
    }
}
