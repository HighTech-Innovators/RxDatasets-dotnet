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

    private const string TestPattern = "t/{tenantid}/s/{storeid}/items/{itemid}/values";

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

    #region Nested delta merge (DeltaBucket tree → trie tree)

    private static DeltaBucket BuildNestedDelta(
        IEnumerable<(string name, long position, int length, uint crc32)>? newEntities = null,
        IEnumerable<(string name, long position, int length, uint crc32)>? changedEntities = null,
        IEnumerable<string>? deletedEntities = null,
        IEnumerable<string>? deletedNames = null)
    {
        // Build a flat DeltaIndex, then nest it to get a proper DeltaBucket tree
        var delta = new DeltaIndex { DeltaVersion = 99 };

        if (newEntities != null)
            foreach (var (name, position, length, crc32) in newEntities)
                delta.NewEntities.Add(new DeltaEntity { Name = name, Position = position, Length = length, Crc32 = crc32 });

        if (changedEntities != null)
            foreach (var (name, position, length, crc32) in changedEntities)
                delta.ChangedEntities.Add(new DeltaEntity { Name = name, Position = position, Length = length, Crc32 = crc32 });

        if (deletedEntities != null)
            foreach (var name in deletedEntities)
                delta.DeletedEntities.Add(new DeletedEntity { Name = name });

        if (deletedNames != null)
            foreach (var name in deletedNames)
                delta.DeletedNames.Add(name);

        NestedIndexHelper.NestDeltaIndex(delta);
        return delta.Nested!;
    }

    [Fact]
    public void Builder_MergeNestedDelta_AddsNewEntities()
    {
        var builder = new EntityTrie.Builder();
        builder.Add("a/b/existing", MakeEntity(1));

        var bucket = BuildNestedDelta(
            newEntities: [("a/b/new1", 100, 10, 1u), ("a/c/new2", 200, 20, 2u)]);

        builder.MergeNestedDelta(bucket, 5);
        var trie = builder.Build();

        trie.Count.Should().Be(3);
        trie["a/b/existing"].LatestVersion.Should().Be(1);
        trie["a/b/new1"].Should().Be(new IndexEntity(5, 100, 10, 1));
        trie["a/c/new2"].Should().Be(new IndexEntity(5, 200, 20, 2));
    }

    [Fact]
    public void Builder_MergeNestedDelta_UpdatesExisting()
    {
        var builder = new EntityTrie.Builder();
        builder.Add("a/b/item", MakeEntity(1, 0, 10));

        var bucket = BuildNestedDelta(
            changedEntities: [("a/b/item", 500, 50, 99u)]);

        builder.MergeNestedDelta(bucket, 7);
        var trie = builder.Build();

        trie.Count.Should().Be(1);
        trie["a/b/item"].Should().Be(new IndexEntity(7, 500, 50, 99));
    }

    [Fact]
    public void Builder_MergeNestedDelta_DeletesEntities()
    {
        var builder = new EntityTrie.Builder();
        builder.Add("a/b/keep", MakeEntity(1));
        builder.Add("a/b/remove", MakeEntity(1));
        builder.Add("a/c/also_remove", MakeEntity(1));

        var bucket = BuildNestedDelta(
            deletedEntities: ["a/b/remove"],
            deletedNames: ["a/c/also_remove"]);

        builder.MergeNestedDelta(bucket, 3);
        var trie = builder.Build();

        trie.Count.Should().Be(1);
        trie.ContainsKey("a/b/keep").Should().BeTrue();
        trie.ContainsKey("a/b/remove").Should().BeFalse();
        trie.ContainsKey("a/c/also_remove").Should().BeFalse();
    }

    [Fact]
    public void Builder_MergeNestedDelta_MixedOperations()
    {
        var builder = new EntityTrie.Builder();
        builder.Add("x/y/old", MakeEntity(1, 0, 10));
        builder.Add("x/y/update_me", MakeEntity(1, 10, 20));
        builder.Add("x/y/delete_me", MakeEntity(1, 30, 30));

        var bucket = BuildNestedDelta(
            newEntities: [("x/y/brand_new", 100, 5, 1u)],
            changedEntities: [("x/y/update_me", 200, 25, 2u)],
            deletedEntities: ["x/y/delete_me"]);

        builder.MergeNestedDelta(bucket, 10);
        var trie = builder.Build();

        trie.Count.Should().Be(3);
        trie["x/y/old"].LatestVersion.Should().Be(1);
        trie["x/y/brand_new"].Should().Be(new IndexEntity(10, 100, 5, 1));
        trie["x/y/update_me"].Should().Be(new IndexEntity(10, 200, 25, 2));
        trie.ContainsKey("x/y/delete_me").Should().BeFalse();
    }

    [Fact]
    public void Immutable_MergeNestedDelta_AddsAndUpdates()
    {
        var trie = EntityTrie.FromDictionary(new Dictionary<string, IndexEntity>
        {
            ["a/b/existing"] = MakeEntity(1, 0, 10),
            ["a/b/to_update"] = MakeEntity(1, 10, 20)
        });

        var bucket = BuildNestedDelta(
            newEntities: [("a/b/new_item", 100, 15, 3u)],
            changedEntities: [("a/b/to_update", 200, 25, 4u)]);

        var merged = trie.MergeNestedDelta(bucket, 8);

        merged.Count.Should().Be(3);
        merged["a/b/existing"].LatestVersion.Should().Be(1); // Unchanged
        merged["a/b/new_item"].Should().Be(new IndexEntity(8, 100, 15, 3));
        merged["a/b/to_update"].Should().Be(new IndexEntity(8, 200, 25, 4));

        // Original trie is unchanged (immutable)
        trie.Count.Should().Be(2);
        trie.ContainsKey("a/b/new_item").Should().BeFalse();
    }

    [Fact]
    public void Immutable_MergeNestedDelta_DeletesAndPrunes()
    {
        var trie = EntityTrie.FromDictionary(new Dictionary<string, IndexEntity>
        {
            ["a/b/keep"] = MakeEntity(1),
            ["a/b/remove"] = MakeEntity(1),
            ["a/c/only_child"] = MakeEntity(1)
        });

        var bucket = BuildNestedDelta(
            deletedEntities: ["a/b/remove"],
            deletedNames: ["a/c/only_child"]);

        var merged = trie.MergeNestedDelta(bucket, 5);

        merged.Count.Should().Be(1);
        merged.ContainsKey("a/b/keep").Should().BeTrue();
        merged.ContainsKey("a/b/remove").Should().BeFalse();
        merged.ContainsKey("a/c/only_child").Should().BeFalse();
    }

    [Fact]
    public void Immutable_MergeNestedDelta_SharedSubtree_OnlyAffectedPathsCopied()
    {
        // Build a trie with entities in two subtrees
        var trie = EntityTrie.FromDictionary(new Dictionary<string, IndexEntity>
        {
            ["left/a"] = MakeEntity(1),
            ["left/b"] = MakeEntity(1),
            ["right/c"] = MakeEntity(1),
            ["right/d"] = MakeEntity(1)
        });

        // Delta only touches "left" subtree
        var bucket = BuildNestedDelta(
            changedEntities: [("left/a", 500, 50, 99u)]);

        var merged = trie.MergeNestedDelta(bucket, 3);

        merged.Count.Should().Be(4);
        merged["left/a"].Should().Be(new IndexEntity(3, 500, 50, 99));
        merged["left/b"].LatestVersion.Should().Be(1);
        merged["right/c"].LatestVersion.Should().Be(1);
        merged["right/d"].LatestVersion.Should().Be(1);
    }

    [Fact]
    public void ApplyDelta_UsesNestedMerge_WhenPatternsMatch()
    {
        var compressor = NameCompressor.FromPattern("t/*/items/*/values")!;

        // Build an initial snapshot with compressed names
        // Pattern "t/*/items/*/values" → compressed "abc/001" → nested: abc → leaf "001"
        var snapshot = new SnapshotIndex { SnapshotVersion = 1, IdPattern = compressor.Pattern };
        var bucket = new SnapshotBucket();
        var abcBucket = new SnapshotBucket();
        abcBucket.Leaves["001"] = new SnapshotEntity
        {
            LastInsertedVersion = { 1 }, Position = 0, Length = 10, Crc32 = 42
        };
        bucket.Children["abc"] = abcBucket;
        snapshot.Nested = bucket;

        var state = ImmutableDatasetState.FromProto(snapshot, compressor);
        state.Entities.Count.Should().Be(1);

        // Build a delta with the same pattern — should use nested merge
        var delta = new DeltaIndex { DeltaVersion = 2, IdPattern = compressor.Pattern };
        delta.NewEntities.Add(new DeltaEntity
            { Name = "t/abc/items/002/values", Position = 10, Length = 20, Crc32 = 5 });

        // Serialize+parse to get nested structure
        var bytes = NestedIndexHelper.SerializeDeltaIndex(delta, compressor);
        var parsed = NestedIndexHelper.ParseDeltaIndex(bytes);

        // parsed.Nested should be preserved for tree merge
        parsed.Nested.Should().NotBeNull();
        parsed.IdPattern.Should().Be(compressor.Pattern);

        var newState = state.ApplyDelta(parsed);
        newState.Entities.Count.Should().Be(2);
        newState.Entities.ContainsKey("t/abc/items/001/values").Should().BeTrue();
        newState.Entities.ContainsKey("t/abc/items/002/values").Should().BeTrue();
    }

    [Fact]
    public void ApplyDeltas_UsesNestedMerge_ForBulkLoad()
    {
        var compressor = NameCompressor.FromPattern("t/*/s/*/items")!;
        var state = new ImmutableDatasetState(
            EntityTrie.FromDictionary([], compressor), 0, 0);

        // Create two deltas, serialize with compression, parse back
        var delta1 = new DeltaIndex { DeltaVersion = 1, IdPattern = compressor.Pattern };
        delta1.NewEntities.Add(new DeltaEntity { Name = "t/a/s/b/items", Position = 0, Length = 10 });
        var bytes1 = NestedIndexHelper.SerializeDeltaIndex(delta1, compressor);
        var parsed1 = NestedIndexHelper.ParseDeltaIndex(bytes1);

        var delta2 = new DeltaIndex { DeltaVersion = 2, IdPattern = compressor.Pattern };
        delta2.NewEntities.Add(new DeltaEntity { Name = "t/a/s/c/items", Position = 10, Length = 20 });
        delta2.ChangedEntities.Add(new DeltaEntity { Name = "t/a/s/b/items", Position = 30, Length = 15 });
        var bytes2 = NestedIndexHelper.SerializeDeltaIndex(delta2, compressor);
        var parsed2 = NestedIndexHelper.ParseDeltaIndex(bytes2);

        var newState = state.ApplyDeltas([parsed1, parsed2]);

        newState.Entities.Count.Should().Be(2);
        newState.Entities["t/a/s/b/items"].LatestVersion.Should().Be(2);
        newState.Entities["t/a/s/b/items"].Position.Should().Be(30);
        newState.Entities["t/a/s/c/items"].LatestVersion.Should().Be(2);
        newState.LatestVersion.Should().Be(2);
    }

    #endregion
}
