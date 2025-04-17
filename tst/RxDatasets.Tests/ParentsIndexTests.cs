using System.Collections.Concurrent;
using System.Collections.Immutable;
using RxDatasets.Models;

namespace RxDatasets.Tests;

public class ParentsIndexTests
{
    private static ImmutableDatasetState CreateDataset(params string[] keys)
    {
        var entities = keys.ToDictionary(
            k => k,
            k => new ImmutableSnapshotEntity([], [], 0, 0, 0)
        );
        return new ImmutableDatasetState(entities.ToImmutableDictionary(), 0, 0);
    }

    private static IEnumerable<string> GetActiveEntityKeys(ImmutableDatasetState dataset)
    {
        return dataset.Entities.Keys;
    }

    [Fact]
    public async Task GetOrCreateChildrenIndex_Should_Return_Children_Under_Same_Parent()
    {
        var index = new ParentsIndex();
        var dataset = CreateDataset(
            "channels/64090/days/20240923/1",
            "channels/64090/days/20240923/2"
        );

        var result = await index.GetOrCreateChildrenIndex("channels/64090/days/20240923", GetActiveEntityKeys(dataset));

        result.Should().BeEquivalentTo(new[]
        {
            "channels/64090/days/20240923/1",
            "channels/64090/days/20240923/2"
        });
    }

    [Fact]
    public async Task GetOrCreateChildrenIndex_Should_Separate_Children_By_Different_Parents()
    {
        var index = new ParentsIndex();
        var dataset = CreateDataset(
            "channels/1/days/20240101/1",
            "channels/2/days/20240101/1"
        );

        var children1 = await index.GetOrCreateChildrenIndex("channels/1/days/20240101", GetActiveEntityKeys(dataset));
        var children2 = await index.GetOrCreateChildrenIndex("channels/2/days/20240101", GetActiveEntityKeys(dataset));

        children1.Should().BeEquivalentTo(new[] { "channels/1/days/20240101/1" });
        children2.Should().BeEquivalentTo(new[] { "channels/2/days/20240101/1" });
    }

    [Fact]
    public async Task GetOrCreateChildrenIndex_Should_Work_With_RequestLevel_Zero()
    {
        var index = new ParentsIndex();
        var dataset = CreateDataset(
            "programs/1",
            "programs/2"
        );

        var children = await index.GetOrCreateChildrenIndex("programs", GetActiveEntityKeys(dataset));
        children.Should().BeEquivalentTo(new[] { "programs/1", "programs/2" });
    }

    [Fact]
    public async Task GetOrCreateChildrenIndex_Should_Return_Empty_If_No_Children()
    {
        var index = new ParentsIndex();
        var dataset = CreateDataset("programs/1");

        var children = await index.GetOrCreateChildrenIndex("other", GetActiveEntityKeys(dataset));
        children.Should().BeEmpty();
    }

    [Fact]
    public async Task OnNew_Should_Add_Child_To_Existing_Parent_Group()
    {
        var index = new ParentsIndex();
        var dataset = CreateDataset("channels/1/days/20240101/1");

        await index.GetOrCreateChildrenIndex("channels/1/days/20240101", GetActiveEntityKeys(dataset));
        await index.OnNew("channels/1/days/20240101/2");

        var children = await index.GetOrCreateChildrenIndex("channels/1/days/20240101", GetActiveEntityKeys(dataset));
        children.Should().Contain("channels/1/days/20240101/2");
    }

    [Fact]
    public async Task OnNew_Should_Create_New_Parent_Group_If_Not_Exists()
    {
        var index = new ParentsIndex();
        var dataset = CreateDataset("x/y/z/1");

        // Initial population to prime internal state
        await index.GetOrCreateChildrenIndex("x/y/z", GetActiveEntityKeys(dataset));

        await index.OnNew("a/b/c/1");

        var result = await index.GetOrCreateChildrenIndex("a/b/c", GetActiveEntityKeys(dataset));
        result.Should().Contain("a/b/c/1");
    }

    [Fact]
    public async Task OnDelete_Should_Remove_Child_From_Index()
    {
        var index = new ParentsIndex();
        var dataset = CreateDataset("channels/5/days/20240101/1", "channels/5/days/20240101/2");

        await index.GetOrCreateChildrenIndex("channels/5/days/20240101", GetActiveEntityKeys(dataset));
        await index.OnDelete("channels/5/days/20240101/2");

        var children = await index.GetOrCreateChildrenIndex("channels/5/days/20240101", GetActiveEntityKeys(dataset));
        children.Should().NotContain("channels/5/days/20240101/2");
        children.Should().Contain("channels/5/days/20240101/1");
    }

    [Fact]
    public async Task OnDelete_Should_Not_Throw_If_Child_Not_In_Index()
    {
        var index = new ParentsIndex();
        var dataset = CreateDataset("channels/9/days/20240101/1");

        await index.GetOrCreateChildrenIndex("channels/9/days/20240101", GetActiveEntityKeys(dataset));

        var act = async () => await index.OnDelete("channels/9/days/20240101/999");
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task CleanIndex_Should_Remove_Expired_Entries()
    {
        var index = new ParentsIndex();
        var dataset = CreateDataset("channels/77/days/20240101/1");

        // Prime index
        await index.GetOrCreateChildrenIndex("channels/77/days/20240101", GetActiveEntityKeys(dataset));

        // Force-expire via reflection (using the updated _index type)
        var field = typeof(ParentsIndex)
            .GetField("_index", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        var indexValue = (ConcurrentDictionary<int, (DateTimeOffset, ImmutableDictionary<string, ImmutableHashSet<string>>)>)
            field!.GetValue(index)!;

        var expiredIndex = new ConcurrentDictionary<int, (DateTimeOffset, ImmutableDictionary<string, ImmutableHashSet<string>>)>(
            indexValue.ToDictionary(
                kvp => kvp.Key,
                kvp => (DateTimeOffset.UtcNow.AddHours(-2), kvp.Value.Item2)
            ));
        field.SetValue(index, expiredIndex);

        var result = await index.CleanIndex();
        result.Should().BeFalse();

    }

    [Fact]
    public async Task CleanIndex_Should_Keep_Valid_Entries()
    {
        var index = new ParentsIndex();
        var dataset = CreateDataset("channels/78/days/20240101/1");

        await index.GetOrCreateChildrenIndex("channels/78/days/20240101", GetActiveEntityKeys(dataset));

        var result = await index.CleanIndex();
        result.Should().BeTrue();
    }
}
