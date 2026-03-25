using System.Collections.Immutable;
using FluentAssertions;
using RxDatasets.Models;

namespace RxDatasets.Tests;

public class EntityTrieTests
{
    private static ImmutableSnapshotEntity MakeEntity(ulong insertedVersion = 1, long position = 0, int length = 10)
        => new([], [insertedVersion], 0, position, length, 42);

    private static ImmutableSnapshotEntity MakeDeletedEntity(ulong deletedVersion = 2)
        => new([deletedVersion], [1], 0, 0, 0, 0);

    #region Empty trie

    [Fact]
    public void Empty_HasCountZero()
    {
        EntityTrie.Empty.Count.Should().Be(0);
    }

    [Fact]
    public void Empty_TryGetValue_ReturnsFalse()
    {
        EntityTrie.Empty.TryGetValue("any/key", out _).Should().BeFalse();
    }

    [Fact]
    public void Empty_ContainsKey_ReturnsFalse()
    {
        EntityTrie.Empty.ContainsKey("foo/bar").Should().BeFalse();
    }

    [Fact]
    public void Empty_Enumeration_YieldsNothing()
    {
        EntityTrie.Empty.Should().BeEmpty();
    }

    [Fact]
    public void Empty_ListDescendants_YieldsNothing()
    {
        EntityTrie.Empty.ListDescendants("foo").Should().BeEmpty();
    }

    [Fact]
    public void Empty_CountDescendants_ReturnsZero()
    {
        EntityTrie.Empty.CountDescendants("foo").Should().Be(0);
    }

    #endregion

    #region SetItem / TryGetValue

    [Fact]
    public void SetItem_SingleSegmentName()
    {
        var entity = MakeEntity();
        var trie = EntityTrie.Empty.SetItem("root", entity);

        trie.Count.Should().Be(1);
        trie.TryGetValue("root", out var found).Should().BeTrue();
        found.Should().Be(entity);
    }

    [Fact]
    public void SetItem_MultiSegmentName()
    {
        var entity = MakeEntity();
        var trie = EntityTrie.Empty.SetItem("tenants/abc/stores/def/items/123", entity);

        trie.Count.Should().Be(1);
        trie.TryGetValue("tenants/abc/stores/def/items/123", out var found).Should().BeTrue();
        found.Should().Be(entity);
    }

    [Fact]
    public void SetItem_MultipleEntities_UnderSamePrefix()
    {
        var e1 = MakeEntity(1);
        var e2 = MakeEntity(2);
        var e3 = MakeEntity(3);

        var trie = EntityTrie.Empty
            .SetItem("t/a/s/d/items/1", e1)
            .SetItem("t/a/s/d/items/2", e2)
            .SetItem("t/a/s/d/items/3", e3);

        trie.Count.Should().Be(3);
        trie["t/a/s/d/items/1"].Should().Be(e1);
        trie["t/a/s/d/items/2"].Should().Be(e2);
        trie["t/a/s/d/items/3"].Should().Be(e3);
    }

    [Fact]
    public void SetItem_MultipleEntities_DifferentPrefixes()
    {
        var e1 = MakeEntity(1);
        var e2 = MakeEntity(2);

        var trie = EntityTrie.Empty
            .SetItem("a/b/c/1", e1)
            .SetItem("x/y/z/2", e2);

        trie.Count.Should().Be(2);
        trie["a/b/c/1"].Should().Be(e1);
        trie["x/y/z/2"].Should().Be(e2);
    }

    [Fact]
    public void SetItem_OverwriteExisting_DoesNotChangeCount()
    {
        var e1 = MakeEntity(1);
        var e2 = MakeEntity(2);

        var trie = EntityTrie.Empty
            .SetItem("a/b", e1)
            .SetItem("a/b", e2);

        trie.Count.Should().Be(1);
        trie["a/b"].Should().Be(e2);
    }

    [Fact]
    public void SetItem_IsImmutable_OriginalUnchanged()
    {
        var e1 = MakeEntity(1);
        var original = EntityTrie.Empty.SetItem("a/b", e1);
        var modified = original.SetItem("c/d", MakeEntity(2));

        original.Count.Should().Be(1);
        original.ContainsKey("c/d").Should().BeFalse();

        modified.Count.Should().Be(2);
        modified.ContainsKey("c/d").Should().BeTrue();
    }

    [Fact]
    public void TryGetValue_NonExistentKey_ReturnsFalse()
    {
        var trie = EntityTrie.Empty.SetItem("a/b", MakeEntity());
        trie.TryGetValue("a/c", out _).Should().BeFalse();
    }

    [Fact]
    public void TryGetValue_PartialPrefix_ReturnsFalse()
    {
        var trie = EntityTrie.Empty.SetItem("a/b/c", MakeEntity());
        // "a/b" exists as an intermediate node, but has no value
        trie.TryGetValue("a/b", out _).Should().BeFalse();
    }

    [Fact]
    public void Indexer_NonExistentKey_Throws()
    {
        var trie = EntityTrie.Empty.SetItem("a/b", MakeEntity());
        var act = () => trie["x/y"];
        act.Should().Throw<KeyNotFoundException>();
    }

    #endregion

    #region Enumeration

    [Fact]
    public void Enumeration_ReturnsAllEntities_WithFullNames()
    {
        var e1 = MakeEntity(1);
        var e2 = MakeEntity(2);
        var e3 = MakeEntity(3);

        var trie = EntityTrie.Empty
            .SetItem("a/b/c", e1)
            .SetItem("a/b/d", e2)
            .SetItem("x/y", e3);

        var all = trie.ToList();
        all.Should().HaveCount(3);
        all.Should().Contain(kvp => kvp.Key == "a/b/c" && kvp.Value == e1);
        all.Should().Contain(kvp => kvp.Key == "a/b/d" && kvp.Value == e2);
        all.Should().Contain(kvp => kvp.Key == "x/y" && kvp.Value == e3);
    }

    [Fact]
    public void Keys_ReturnsAllNames()
    {
        var trie = EntityTrie.Empty
            .SetItem("a/1", MakeEntity())
            .SetItem("b/2", MakeEntity());

        trie.Keys.Should().BeEquivalentTo(["a/1", "b/2"]);
    }

    [Fact]
    public void Values_ReturnsAllEntities()
    {
        var e1 = MakeEntity(1);
        var e2 = MakeEntity(2);
        var trie = EntityTrie.Empty
            .SetItem("a/1", e1)
            .SetItem("b/2", e2);

        trie.Values.Should().BeEquivalentTo([e1, e2]);
    }

    #endregion

    #region ListDescendants / CountDescendants

    [Fact]
    public void ListDescendants_ReturnsAllUnderParent()
    {
        var trie = EntityTrie.Empty
            .SetItem("t/a/items/1", MakeEntity())
            .SetItem("t/a/items/2", MakeEntity())
            .SetItem("t/b/items/3", MakeEntity());

        var descendants = trie.ListDescendants("t/a").ToList();
        descendants.Should().BeEquivalentTo(["t/a/items/1", "t/a/items/2"]);
    }

    [Fact]
    public void ListDescendants_ExcludesDeletedEntities()
    {
        var trie = EntityTrie.Empty
            .SetItem("t/a/items/1", MakeEntity())
            .SetItem("t/a/items/2", MakeDeletedEntity())
            .SetItem("t/a/items/3", MakeEntity());

        var descendants = trie.ListDescendants("t/a").ToList();
        descendants.Should().BeEquivalentTo(["t/a/items/1", "t/a/items/3"]);
    }

    [Fact]
    public void ListDescendants_NonExistentParent_ReturnsEmpty()
    {
        var trie = EntityTrie.Empty.SetItem("a/b/c", MakeEntity());
        trie.ListDescendants("x/y").Should().BeEmpty();
    }

    [Fact]
    public void ListDescendants_EmptyParent_ReturnsAll()
    {
        var trie = EntityTrie.Empty
            .SetItem("a/1", MakeEntity())
            .SetItem("b/2", MakeEntity());

        trie.ListDescendants("").Should().HaveCount(2);
    }

    [Fact]
    public void CountDescendants_CountsNonDeletedOnly()
    {
        var trie = EntityTrie.Empty
            .SetItem("p/a/items/1", MakeEntity())
            .SetItem("p/a/items/2", MakeDeletedEntity())
            .SetItem("p/a/items/3", MakeEntity())
            .SetItem("p/b/items/4", MakeEntity());

        trie.CountDescendants("p/a").Should().Be(2);
        trie.CountDescendants("p/b").Should().Be(1);
        trie.CountDescendants("p").Should().Be(3);
    }

    [Fact]
    public void CountDescendants_NonExistentParent_ReturnsZero()
    {
        var trie = EntityTrie.Empty.SetItem("a/b", MakeEntity());
        trie.CountDescendants("x/y").Should().Be(0);
    }

    [Fact]
    public void ListDescendants_DeepHierarchy()
    {
        var trie = EntityTrie.Empty
            .SetItem("a/b/c/d/e/f", MakeEntity())
            .SetItem("a/b/c/d/e/g", MakeEntity())
            .SetItem("a/b/c/x/y/z", MakeEntity());

        trie.ListDescendants("a/b/c/d").Should().HaveCount(2);
        trie.ListDescendants("a/b/c").Should().HaveCount(3);
        trie.ListDescendants("a/b/c/d/e").Should().HaveCount(2);
    }

    #endregion

    #region Builder

    [Fact]
    public void Builder_AddsMultipleEntities()
    {
        var builder = new EntityTrie.Builder();
        builder.Add("a/1", MakeEntity(1));
        builder.Add("a/2", MakeEntity(2));
        builder.Add("b/1", MakeEntity(3));
        var trie = builder.Build();

        trie.Count.Should().Be(3);
        trie.ContainsKey("a/1").Should().BeTrue();
        trie.ContainsKey("a/2").Should().BeTrue();
        trie.ContainsKey("b/1").Should().BeTrue();
    }

    [Fact]
    public void Builder_EmptyBuild_ProducesEmptyTrie()
    {
        var trie = new EntityTrie.Builder().Build();
        trie.Count.Should().Be(0);
        trie.Should().BeEmpty();
    }

    #endregion

    #region FromDictionary

    [Fact]
    public void FromDictionary_CreatesTrieFromKVPs()
    {
        var dict = new Dictionary<string, ImmutableSnapshotEntity>
        {
            ["p/a/1"] = MakeEntity(1),
            ["p/a/2"] = MakeEntity(2),
            ["p/b/3"] = MakeEntity(3),
        };

        var trie = EntityTrie.FromDictionary(dict);

        trie.Count.Should().Be(3);
        trie["p/a/1"].Should().Be(dict["p/a/1"]);
        trie["p/a/2"].Should().Be(dict["p/a/2"]);
        trie["p/b/3"].Should().Be(dict["p/b/3"]);
    }

    [Fact]
    public void FromDictionary_EmptyDictionary_ProducesEmptyTrie()
    {
        var trie = EntityTrie.FromDictionary(new Dictionary<string, ImmutableSnapshotEntity>());
        trie.Count.Should().Be(0);
    }

    #endregion

    #region LINQ compatibility (since IReadOnlyDictionary)

    [Fact]
    public void Where_FiltersEntities()
    {
        var trie = EntityTrie.Empty
            .SetItem("a/1", MakeEntity(1))
            .SetItem("a/2", MakeDeletedEntity())
            .SetItem("a/3", MakeEntity(3));

        var nonDeleted = trie
            .Where(x => x.Value.GetLatestChangeType() != Abstractions.ChangeType.Deleted)
            .Select(x => x.Key)
            .ToList();

        nonDeleted.Should().BeEquivalentTo(["a/1", "a/3"]);
    }

    [Fact]
    public void Count_WithPredicate_Works()
    {
        var trie = EntityTrie.Empty
            .SetItem("a/1", MakeEntity())
            .SetItem("a/2", MakeDeletedEntity())
            .SetItem("a/3", MakeEntity());

        trie.Count(x => x.Value.GetLatestChangeType() != Abstractions.ChangeType.Deleted).Should().Be(2);
    }

    #endregion
}
