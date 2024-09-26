using FluentAssertions;
using RxDatasets.Models;
using System.Collections.Immutable;

namespace RxDatasets.StorageTests.Common;

public class DatasetHelperTests
{
    [Theory]
    [InlineData("channels/64090/days/20240923/events/287431128", "channels/64090/days/20240923")]
    [InlineData("bla/A/bla/K", "bla/A")]
    [InlineData("prov/bbc.nl/pack/BBCC1234/video/123/singlechild", "prov/bbc.nl/pack/BBCC1234/video/123")]
    [InlineData("root/child", null)]
    [InlineData("singleword", null)] // No parent exists, so should return null
    [InlineData("parent/child/", null)] // Trailing slash case
    public void FindParentOfEntity_ShouldReturnCorrectParent(string input, string? expected)
    {
        // Act
        var result = DatasetHelper.FindParentOfEntity(input);

        // Assert
        result.Should().Be(expected);
    }

    [Fact]
    public void DeleteImplicitly_Should_Return_Entities_Not_In_ReceivedNames_And_Not_Deleted()
    {
        // Arrange
        ImmutableDictionary<string, ImmutableEntity> entities = new Dictionary<string, ImmutableEntity>
        {
            { "parent1/child1/bla/1", new ImmutableEntity([0ul], [0ul], 0ul, 0, 0) },
            { "parent1/child2/bla/2", new ImmutableEntity([0ul], [0ul], 0ul, 0, 0) },
            { "parent2/child1/bla/1", new ImmutableEntity([0ul], [1ul], 1ul, 0, 1) },
            { "parent2/child2/bla/2", new ImmutableEntity([0ul], [0ul], 0ul, 0, 0) },
            { "parent3/child1/bla/1", new ImmutableEntity([0ul], [1ul], 1ul, 0, 1) },
            { "parent3/child1/bla/2", new ImmutableEntity([0ul], [0ul], 0ul, 0, 0) },
            { "parent3/child2/bla/1", new ImmutableEntity([0ul], [0ul], 0ul, 0, 0) },
            { "parent3/child3/bla/1", new ImmutableEntity([0ul], [0ul], 0ul, 0, 0) },
        }.ToImmutableDictionary();

        var index = new ImmutableDatasetState(entities, 0, 0);

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
        var entities = new Dictionary<string, ImmutableEntity>
        {
            { "parent1/child1/bla/1", new ImmutableEntity([0ul], [0ul], 0ul, 0, 0) },
            { "parent2/child1/bla/1", new ImmutableEntity([0ul], [0ul], 0ul, 0, 0) },
            { "parent2/child1/bla/2", new ImmutableEntity([0ul], [0ul], 0ul, 0, 0) },
            { "parent2/child1/bla/3", new ImmutableEntity([0ul], [0ul], 0ul, 0, 0) },
            { "parent2/child2/bla/1", new ImmutableEntity([0ul], [0ul], 0ul, 0, 0) },
            { "parent3/child1/bla/1", new ImmutableEntity([0ul], [0ul], 0ul, 0, 0) },
            { "parent3/child1/bla/2", new ImmutableEntity([0ul], [0ul], 0ul, 0, 0) },
            { "parent3/child2/bla/1", new ImmutableEntity([0ul], [0ul], 0ul, 0, 0) },
        }.ToImmutableDictionary();

        var index = new ImmutableDatasetState(entities, 0, 0);

        var receivedNames = new HashSet<string>
            { "parent2/child1/bla/1", "parent3/child1/bla/1" }; // all siblings of those should be deleted
        var allParents = new HashSet<string> { "parent2/child1", "parent3/child1" };

        // Act
        var result = DatasetHelper.GetImplicitDeletesForParentNames(index, receivedNames, allParents);

        // Assert
        result.Should().BeEquivalentTo("parent2/child1/bla/2", "parent2/child1/bla/3", "parent3/child1/bla/2");
    }
}