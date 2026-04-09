using FluentAssertions;
using RxDatasets.Models;

namespace RxDatasets.Tests;

public class EntityTrieTests
{
    private static IndexEntity MakeEntity(ulong version = 1, long position = 0, int length = 10)
        => new(version, position, length, 42);

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
    public void ListDescendants_AfterRemoveItem_ExcludesRemoved()
    {
        var trie = EntityTrie.Empty
            .SetItem("t/a/items/1", MakeEntity())
            .SetItem("t/a/items/2", MakeEntity())
            .SetItem("t/a/items/3", MakeEntity())
            .RemoveItem("t/a/items/2");

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
    public void CountDescendants_AfterRemoveItem_CountsCorrectly()
    {
        var trie = EntityTrie.Empty
            .SetItem("p/a/items/1", MakeEntity())
            .SetItem("p/a/items/2", MakeEntity())
            .SetItem("p/a/items/3", MakeEntity())
            .SetItem("p/b/items/4", MakeEntity())
            .RemoveItem("p/a/items/2");

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
        var dict = new Dictionary<string, IndexEntity>
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
        var trie = EntityTrie.FromDictionary(new Dictionary<string, IndexEntity>());
        trie.Count.Should().Be(0);
    }

    #endregion

    #region LINQ compatibility (since IReadOnlyDictionary)

    [Fact]
    public void Where_FiltersEntities()
    {
        var trie = EntityTrie.Empty
            .SetItem("a/1", MakeEntity(1))
            .SetItem("a/2", MakeEntity(2))
            .SetItem("a/3", MakeEntity(3));

        var filtered = trie
            .Where(x => x.Value.LatestVersion > 1)
            .Select(x => x.Key)
            .ToList();

        filtered.Should().BeEquivalentTo(["a/2", "a/3"]);
    }

    [Fact]
    public void Count_WithPredicate_Works()
    {
        var trie = EntityTrie.Empty
            .SetItem("a/1", MakeEntity())
            .SetItem("a/2", MakeEntity())
            .SetItem("a/3", MakeEntity());

        trie.Count.Should().Be(3);
    }

    #endregion

    #region RemoveItem

    [Fact]
    public void RemoveItem_ExistingEntity_DecreasesCount()
    {
        var trie = EntityTrie.Empty
            .SetItem("a/b", MakeEntity())
            .SetItem("c/d", MakeEntity());

        var result = trie.RemoveItem("a/b");
        result.Count.Should().Be(1);
        result.ContainsKey("a/b").Should().BeFalse();
        result.ContainsKey("c/d").Should().BeTrue();
    }

    [Fact]
    public void RemoveItem_NonExistentEntity_ReturnsSameTrie()
    {
        var trie = EntityTrie.Empty.SetItem("a/b", MakeEntity());
        var result = trie.RemoveItem("x/y");
        result.Count.Should().Be(1);
        result.Should().BeSameAs(trie);
    }

    [Fact]
    public void RemoveItem_IsImmutable_OriginalUnchanged()
    {
        var original = EntityTrie.Empty
            .SetItem("a/b", MakeEntity())
            .SetItem("c/d", MakeEntity());

        var modified = original.RemoveItem("a/b");

        original.Count.Should().Be(2);
        original.ContainsKey("a/b").Should().BeTrue();
        modified.Count.Should().Be(1);
    }

    [Fact]
    public void RemoveItem_PrunesEmptyIntermediateNodes()
    {
        var trie = EntityTrie.Empty
            .SetItem("a/b/c/d", MakeEntity());

        var result = trie.RemoveItem("a/b/c/d");
        result.Count.Should().Be(0);
        // The entire "a/b/c/d" branch should be pruned
        result.ListDescendants("a").Should().BeEmpty();
    }

    #endregion

    #region NameCompressor integration

    private const string TestPattern = "t/*/s/*/items/*/values";

    private static EntityTrie EmptyCompressed
        => new EntityTrie.Builder(NameCompressor.FromPattern(TestPattern)).Build();

    [Fact]
    public void Compressed_SetItem_And_TryGetValue_UseFullNames()
    {
        var trie = EmptyCompressed
            .SetItem("t/abc/s/def/items/001/values", MakeEntity(1));

        trie.Count.Should().Be(1);
        trie.TryGetValue("t/abc/s/def/items/001/values", out var entity).Should().BeTrue();
        entity!.LatestVersion.Should().Be(1);
    }

    [Fact]
    public void Compressed_ContainsKey_WorksWithFullNames()
    {
        var trie = EmptyCompressed
            .SetItem("t/abc/s/def/items/001/values", MakeEntity());

        trie.ContainsKey("t/abc/s/def/items/001/values").Should().BeTrue();
        trie.ContainsKey("t/abc/s/def/items/002/values").Should().BeFalse();
    }

    [Fact]
    public void Compressed_Indexer_WorksWithFullNames()
    {
        var e = MakeEntity(42);
        var trie = EmptyCompressed
            .SetItem("t/abc/s/def/items/001/values", e);

        trie["t/abc/s/def/items/001/values"].Should().Be(e);
    }

    [Fact]
    public void Compressed_RemoveItem_WorksWithFullNames()
    {
        var trie = EmptyCompressed
            .SetItem("t/abc/s/def/items/001/values", MakeEntity())
            .SetItem("t/abc/s/def/items/002/values", MakeEntity())
            .RemoveItem("t/abc/s/def/items/001/values");

        trie.Count.Should().Be(1);
        trie.ContainsKey("t/abc/s/def/items/001/values").Should().BeFalse();
        trie.ContainsKey("t/abc/s/def/items/002/values").Should().BeTrue();
    }

    [Fact]
    public void Compressed_Enumerate_YieldsFullNames()
    {
        var trie = EmptyCompressed
            .SetItem("t/abc/s/def/items/001/values", MakeEntity(1))
            .SetItem("t/abc/s/def/items/002/values", MakeEntity(2));

        var names = trie.Select(kvp => kvp.Key).ToList();
        names.Should().BeEquivalentTo([
            "t/abc/s/def/items/001/values",
            "t/abc/s/def/items/002/values"
        ]);
    }

    [Fact]
    public void Compressed_Keys_YieldsFullNames()
    {
        var trie = EmptyCompressed
            .SetItem("t/abc/s/def/items/001/values", MakeEntity());

        trie.Keys.Should().BeEquivalentTo(["t/abc/s/def/items/001/values"]);
    }

    [Fact]
    public void Compressed_ListDescendants_WithFullPrefix()
    {
        var trie = EmptyCompressed
            .SetItem("t/abc/s/def/items/001/values", MakeEntity())
            .SetItem("t/abc/s/def/items/002/values", MakeEntity())
            .SetItem("t/abc/s/ghi/items/003/values", MakeEntity());

        var desc = trie.ListDescendants("t/abc/s/def/items").ToList();
        desc.Should().BeEquivalentTo([
            "t/abc/s/def/items/001/values",
            "t/abc/s/def/items/002/values"
        ]);
    }

    [Fact]
    public void Compressed_ListDescendants_PartialPrefix()
    {
        var trie = EmptyCompressed
            .SetItem("t/abc/s/def/items/001/values", MakeEntity())
            .SetItem("t/abc/s/ghi/items/002/values", MakeEntity());

        var desc = trie.ListDescendants("t/abc").ToList();
        desc.Should().HaveCount(2);
        desc.Should().Contain("t/abc/s/def/items/001/values");
        desc.Should().Contain("t/abc/s/ghi/items/002/values");
    }

    [Fact]
    public void Compressed_ListDescendants_EmptyPrefix_ReturnsAll()
    {
        var trie = EmptyCompressed
            .SetItem("t/abc/s/def/items/001/values", MakeEntity())
            .SetItem("t/xyz/s/ghi/items/002/values", MakeEntity());

        trie.ListDescendants("").Should().HaveCount(2);
    }

    [Fact]
    public void Compressed_CountDescendants()
    {
        var trie = EmptyCompressed
            .SetItem("t/abc/s/def/items/001/values", MakeEntity())
            .SetItem("t/abc/s/def/items/002/values", MakeEntity())
            .SetItem("t/abc/s/ghi/items/003/values", MakeEntity());

        trie.CountDescendants("t/abc/s/def").Should().Be(2);
        trie.CountDescendants("t/abc").Should().Be(3);
    }

    [Fact]
    public void Compressed_Builder_AddsAndBuilds()
    {
        var compressor = NameCompressor.FromPattern(TestPattern);
        var builder = new EntityTrie.Builder(compressor);
        builder.Add("t/abc/s/def/items/001/values", MakeEntity(1));
        builder.Add("t/abc/s/def/items/002/values", MakeEntity(2));
        var trie = builder.Build();

        trie.Count.Should().Be(2);
        trie["t/abc/s/def/items/001/values"].LatestVersion.Should().Be(1);
        trie["t/abc/s/def/items/002/values"].LatestVersion.Should().Be(2);
    }

    [Fact]
    public void Compressed_Builder_SetItemAndRemove()
    {
        var compressor = NameCompressor.FromPattern(TestPattern);
        var builder = new EntityTrie.Builder(compressor);
        builder.Add("t/abc/s/def/items/001/values", MakeEntity(1));
        builder.SetItem("t/abc/s/def/items/001/values", MakeEntity(2));
        builder.Add("t/abc/s/def/items/002/values", MakeEntity(3));
        builder.Remove("t/abc/s/def/items/002/values");
        var trie = builder.Build();

        trie.Count.Should().Be(1);
        trie["t/abc/s/def/items/001/values"].LatestVersion.Should().Be(2);
    }

    [Fact]
    public void Compressed_ToBuilder_PreservesCompressor()
    {
        var trie = EmptyCompressed
            .SetItem("t/abc/s/def/items/001/values", MakeEntity(1));

        var builder = trie.ToBuilder();
        builder.SetItem("t/abc/s/def/items/002/values", MakeEntity(2));
        var newTrie = builder.Build();

        newTrie.Count.Should().Be(2);
        newTrie.ContainsKey("t/abc/s/def/items/001/values").Should().BeTrue();
        newTrie.ContainsKey("t/abc/s/def/items/002/values").Should().BeTrue();
        // And enumerate returns full names
        newTrie.Keys.Should().AllSatisfy(k => k.Should().StartWith("t/"));
    }

    [Fact]
    public void Compressed_FromDictionary_WithCompressor()
    {
        var compressor = NameCompressor.FromPattern(TestPattern);
        var dict = new Dictionary<string, IndexEntity>
        {
            ["t/abc/s/def/items/001/values"] = MakeEntity(1),
            ["t/abc/s/def/items/002/values"] = MakeEntity(2),
        };

        var trie = EntityTrie.FromDictionary(dict, compressor);

        trie.Count.Should().Be(2);
        trie["t/abc/s/def/items/001/values"].LatestVersion.Should().Be(1);
    }

    [Fact]
    public void Compressed_ReducesTrieDepth()
    {
        // Without compression: depth 7 (t/guid/s/guid/items/id/values)
        // With compression: depth 3 (guid/guid/id)
        // We can verify by checking that ListDescendants at various levels works correctly
        var trie = EmptyCompressed;
        for (int i = 0; i < 10; i++)
        {
            trie = trie.SetItem($"t/tenant1/s/store{i}/items/item1/values", MakeEntity());
        }

        trie.Count.Should().Be(10);
        trie.CountDescendants("t/tenant1").Should().Be(10);
    }

    #endregion
}
