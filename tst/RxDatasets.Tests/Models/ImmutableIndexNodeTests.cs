using RxDatasets.Models;

namespace RxDatasets.Tests.Models;

public class ImmutableIndexNodeTests
{
    private static ImmutableSnapshotEntity Live(long position = 0) => new([], [1], 0, position, 10);

    private static ImmutableSnapshotEntity Deleted() => new([2], [1], 0, 0, 0);

    private static ImmutableIndexNode<ImmutableSnapshotEntity> Build(params (string Name, ImmutableSnapshotEntity Entity)[] entities)
    {
        var builder = ImmutableIndexNode<ImmutableSnapshotEntity>.Empty.ToBuilder();
        foreach (var (name, entity) in entities)
            builder.Set(name, entity);
        return builder.ToImmutable();
    }

    [Fact]
    public void Empty_HasNoLeavesChildrenOrCount()
    {
        var empty = ImmutableIndexNode<ImmutableSnapshotEntity>.Empty;

        empty.Leaves.Should().BeEmpty();
        empty.Children.Should().BeEmpty();
        empty.LiveCount.Should().Be(0);
        empty.EnumerateAll().Should().BeEmpty();
    }

    [Fact]
    public void Set_StoresFixedPartsOncePerNode()
    {
        var root = Build(
            ("t/T/s/S/items/I1/values", Live()),
            ("t/T/s/S/items/I2/values", Live()),
            ("t/T/tenant", Live()));

        root.Leaves.Keys.Should().Equal("t/{}/tenant");
        var store = root.Children["t".AsMemory()]["T".AsMemory()].Children["s".AsMemory()]["S".AsMemory()];
        store.Leaves.Keys.Should().Equal("items/{}/values");
        store.Leaves["items/{}/values"].Keys.Select(k => k.ToString()).Should().BeEquivalentTo("I1", "I2");
    }

    [Fact]
    public void TryGetValue_FindsEveryName_AndNothingElse()
    {
        var names = new[] { "single", "esltypes/E", "esltypes/E/driver", "t/T/s/S", "t/T/s/S/eccparameters", "t/T/s/S/items/I/values" };
        var root = Build(names.Select((n, i) => (n, Live(i))).ToArray());

        for (int i = 0; i < names.Length; i++)
        {
            root.TryGetValue(names[i], out var entity).Should().BeTrue(names[i]);
            entity.Position.Should().Be(i);
        }

        foreach (var missing in new[] { "", "t", "t/T", "t/T/s", "t/T/s/S/items", "t/T/s/S/items/I", "t/T/s/S/items/I/curated", "esltypes/X" })
            root.TryGetValue(missing, out _).Should().BeFalse(missing);
    }

    [Fact]
    public void TryGetValue_DoesNotAllocate()
    {
        var root = Build(("t/T/s/S/items/I/values", Live()));
        const string name = "t/T/s/S/items/I/values";
        root.TryGetValue(name, out _);

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 100; i++)
            root.TryGetValue(name, out _);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        allocated.Should().Be(0);
    }

    [Fact]
    public void Builder_CopiesOnlyTheTouchedPath()
    {
        var original = Build(("t/T1/s/S/items/I/values", Live(1)), ("t/T2/s/S/items/I/values", Live(2)));
        var otherTenant = original.Children["t".AsMemory()]["T2".AsMemory()];

        var builder = original.ToBuilder();
        builder.Set("t/T1/s/S/items/I/values", Live(3));
        var updated = builder.ToImmutable();

        original.GetValueOrDefault("t/T1/s/S/items/I/values")!.Position.Should().Be(1, "readers keep the old root");
        updated.GetValueOrDefault("t/T1/s/S/items/I/values")!.Position.Should().Be(3);
        updated.Children["t".AsMemory()]["T2".AsMemory()].Should().BeSameAs(otherTenant);
        original.ToBuilder().ToImmutable().Should().BeSameAs(original);
    }

    [Fact]
    public void LiveCount_FollowsInsertsDeletesAndReinserts()
    {
        var root = Build(("t/T/s/S/items/I1/values", Live()), ("t/T/s/S/items/I2/values", Live()), ("t/T/tenant", Live()));
        root.LiveCount.Should().Be(3);

        var builder = root.ToBuilder();
        builder.Set("t/T/s/S/items/I1/values", Deleted());
        var afterDelete = builder.ToImmutable();
        afterDelete.LiveCount.Should().Be(2);
        afterDelete.CountLive("t/T/s/S").Should().Be(1);

        builder = afterDelete.ToBuilder();
        builder.Set("t/T/s/S/items/I1/values", Live());
        builder.Set("t/T/s/S/items/I1/values", Live());
        builder.ToImmutable().LiveCount.Should().Be(3);
    }

    [Fact]
    public void EnumerateAll_RebuildsFullNames()
    {
        var names = new[] { "single", "esltypes/E", "t/T/s/S/eccparameters", "t/T/s/S/esls/B/pages/1/studioimage" };
        var root = Build(names.Select(n => (n, Live())).ToArray());

        root.EnumerateAll().Select(e => e.Key).Should().BeEquivalentTo(names);
        root.EnumerateAll("/").Select(e => e.Key).Should().BeEquivalentTo(names);
    }

    public static readonly TheoryData<string> Parents =
    [
        "", "/", "t", "t/", "t/T", "t/T/", "t/T/s", "t/T/s/S", "t/T/s/S/", "t/T/s/S/items", "t/T/s/S/items/I1",
        "t/T/s/S/items/I1/values", "t/T/s/S/items/I1/values/x", "t/T/s/S/esls", "t/T/s/S/esls/B", "t/T/s/S/esls/B/pages",
        "t/T/s/S/esls/B/pages/1", "t/T/s/S/esls/B/pages/1/studioimage", "t/T/tenant", "t/T/te", "esltypes", "esltypes/E",
        "esltypes/E/driver", "single", "sing", "/t/T", "t/T/s/S2", "x/y/z/w", "t/T/designs/D/templates",
    ];

    private static readonly string[] Names =
    [
        "single", "esltypes/E", "esltypes/E/driver", "esltypes/F", "t/T/tenant", "t/T/s/S", "t/T/s/S/eccparameters",
        "t/T/s/S/items/I1/values", "t/T/s/S/items/I1/curated", "t/T/s/S/items/I2/values", "t/T/s/S/items/I3/values",
        "t/T/s/S/esls/B/link", "t/T/s/S/esls/B/link/studio", "t/T/s/S/esls/B/pages/1/studioimage",
        "t/T/s/S/esls/B/pages/2/studioimage", "t/T/s/S2/items/I1/values", "t/T/designs/D/templates/SHA",
        "t/U/s/S/items/I1/values", "a/b/", "a/b/c/d/e/f/g/h/i",
    ];

    /// <summary>What develop's List did: ParentsIndex, keyed by the parent's level.</summary>
    private static IEnumerable<string> DevelopList(IEnumerable<string> liveNames, string parent)
    {
        if (string.IsNullOrEmpty(parent) || parent is "/")
            return liveNames;

        parent = parent.TrimEnd('/');
        int level = parent.Count(c => c == '/') + 1;
        return liveNames.Where(name => DatasetHelper.FindParentOfEntity(name, level) == parent);
    }

    [Theory]
    [MemberData(nameof(Parents))]
    public void EnumerateAllAndCountLive_MatchDevelopList(string parent)
    {
        var deleted = new HashSet<string> { "t/T/s/S/items/I3/values", "esltypes/F" };
        var root = Build(Names.Select(n => (n, deleted.Contains(n) ? Deleted() : Live())).ToArray());
        var expected = DevelopList(Names.Where(n => !deleted.Contains(n)), parent).ToList();

        root.EnumerateAll(parent, e => e.IsLive).Select(e => e.Key).Should().BeEquivalentTo(expected);
        root.CountLive(parent).Should().Be(expected.Count);
    }

    [Fact]
    public void EnumerateAll_WithoutFilter_IncludesDeleted()
    {
        var root = Build(("t/T/s/S/items/I1/values", Live()), ("t/T/s/S/items/I2/values", Deleted()));

        root.EnumerateAll("t/T/s/S/items").Should().HaveCount(2);
        root.CountLive("t/T/s/S/items").Should().Be(1);
    }

    [Fact]
    public void Builder_KeepsOriginalValues_ForUntouchedGroups()
    {
        var root = Build(("t/T/s/S/items/I1/values", Live(1)));
        var builder = root.ToBuilder();

        builder.Child("t".AsMemory(), "T".AsMemory()).Child("s".AsMemory(), "S".AsMemory())
            .GetLeaf("items/{}/values", "I1".AsMemory())!.Position.Should().Be(1);
        builder.ToImmutable().GetValueOrDefault("t/T/s/S/items/I1/values")!.Position.Should().Be(1);
    }
}
