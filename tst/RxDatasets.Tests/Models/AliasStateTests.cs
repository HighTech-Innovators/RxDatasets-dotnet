using Google.Protobuf;
using RxDatasets.Models;

namespace RxDatasets.Tests.Models;

/// <summary>Aliases in the index: diffs in deltas, full sets in snapshots, reverse maps in the state.</summary>
public class AliasStateTests
{
    private const string Link1 = "t/T/s/S/links/L1";
    private const string Link2 = "t/T/s/S/links/L2";
    private const string Esl = "t/T/s/S/esls/B/link";

    private static AliasRef Alias(string value, string name = "by_storeesl") => new() { Name = name, Value = value };

    private static DeltaIndex Delta(ulong version) => new() { DeltaVersion = version, AliasDefinitionsHash = "h" };

    private static ImmutableDatasetState WithLink1OwningEsl()
    {
        var delta = Delta(1);
        delta.NewEntities.Add(new DeltaEntity { Name = Link1, Crc32 = 1, AddedAliases = { Alias(Esl) } });
        delta.NewEntities.Add(new DeltaEntity { Name = Link2, Crc32 = 2 });
        delta.ConvertToTree();
        return ImmutableDatasetState.Empty(0, 0).ApplyDelta(delta);
    }

    [Fact]
    public void ApplyDelta_AddsAliasesToTheEntityAndTheReverseMap()
    {
        var state = WithLink1OwningEsl();

        state.ResolveAlias("by_storeesl", Esl).Should().Be(Link1);
        state.GetEntity(Link1).Aliases.Should().Equal([("by_storeesl", Esl)]);
        state.ListAliases("by_storeesl", "t/T/s/S").Should().Equal(Esl);
        state.AliasDefinitionsHash.Should().Be("h");
    }

    [Fact]
    public void ApplyDelta_MovesAnAliasBetweenEntitiesInOneDelta()
    {
        var delta = Delta(2);
        // The claim comes before the release, so the release must leave the new owner alone
        delta.ChangedEntities.Add(new DeltaEntity { Name = Link2, Crc32 = 3, AddedAliases = { Alias(Esl) } });
        delta.ChangedEntities.Add(new DeltaEntity { Name = Link1, Crc32 = 4, RemovedAliases = { Alias(Esl) } });
        delta.ConvertToTree();

        var state = WithLink1OwningEsl().ApplyDelta(delta);

        state.ResolveAlias("by_storeesl", Esl).Should().Be(Link2);
        state.GetEntity(Link1).Aliases.Should().BeEmpty();
    }

    [Fact]
    public void ApplyDelta_DeleteReleasesTheAliases()
    {
        var delta = Delta(2);
        delta.DeletedEntities.Add(new DeletedEntity { Name = Link1, RemovedAliases = { Alias(Esl) } });
        delta.ConvertToTree();

        var before = WithLink1OwningEsl();
        var state = before.ApplyDelta(delta);

        state.ResolveAlias("by_storeesl", Esl).Should().BeNull();
        state.ListAliases("by_storeesl", "").Should().BeEmpty();
        state.TryGetEntity(Link1, out _).Should().BeFalse("a deleted entity leaves the index");
        before.ResolveAlias("by_storeesl", Esl).Should().Be(Link1, "the earlier state is untouched");
    }

    [Fact]
    public void ApplyDelta_AppliesAliasOnlyChanges()
    {
        var delta = Delta(2);
        delta.AliasChanges.Add(new AliasOnlyChange
        {
            Name = Link1,
            RemovedAliases = { Alias(Esl) },
            AddedAliases = { Alias("t/T/s/S/items/I/links/L1", "by_item") }
        });

        var state = WithLink1OwningEsl().ApplyDelta(delta);

        state.ResolveAlias("by_storeesl", Esl).Should().BeNull();
        state.ResolveAlias("by_item", "t/T/s/S/items/I/links/L1").Should().Be(Link1);
        state.GetEntity(Link1).Aliases.Should().Equal([("by_item", "t/T/s/S/items/I/links/L1")]);
        state.GetEntity(Link1).Crc32.Should().Be(1, "only the aliases changed");
    }

    [Fact]
    public void Snapshot_KeepsTheAliasesAndTheHash()
    {
        var proto = WithLink1OwningEsl().ToProto();
        var state = ImmutableDatasetState.FromProto(SnapshotIndex.Parser.ParseFrom(proto.ToByteArray()));

        proto.AliasDefinitionsHash.Should().Be("h");
        state.AliasDefinitionsHash.Should().Be("h");
        state.ResolveAlias("by_storeesl", Esl).Should().Be(Link1);
        state.GetEntity(Link1).Aliases.Should().Equal([("by_storeesl", Esl)]);
    }

    [Fact]
    public void IndexNode_Remove_TakesTheValueOutOfTheCount()
    {
        var builder = ImmutableIndexNode<string>.Empty.ToBuilder();
        builder.Set("a/1/b", "x");
        builder.Set("a/2/b", "y");
        var full = builder.ToImmutable();

        var removing = full.ToBuilder();
        removing.Remove("a/1/b");
        removing.Remove("a/9/b");
        var removed = removing.ToImmutable();

        removed.TryGetValue("a/1/b", out _).Should().BeFalse();
        removed.TryGetValue("a/2/b", out var value).Should().BeTrue();
        value.Should().Be("y");
        removed.LiveCount.Should().Be(1);
        full.LiveCount.Should().Be(2);
    }
}
