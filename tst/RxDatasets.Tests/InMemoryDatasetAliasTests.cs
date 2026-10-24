using System.Text;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.InMemoryDataset;

namespace RxDatasets.Tests;

/// <summary>
/// <see cref="InMemoryDataset{T}"/> writes and checks aliases like <see cref="DatasetWriter{T}"/>. An entity's payload
/// is the list of its <c>by_storeesl</c> aliases, separated by ';'.
/// </summary>
public class InMemoryDatasetAliasTests
{
    private const string StoreEsl = "by_storeesl";
    private const string L1 = "t/T/s/S/links/L1";
    private const string L2 = "t/T/s/S/links/L2";
    private const string B1 = "t/T/s/S/esls/B1/link";
    private const string B2 = "t/T/s/S/esls/B2/link";

    private readonly InMemoryDataset<object> _dataset = new(
    [
        new AliasDefinition(StoreEsl,
            (_, bytes) => Encoding.UTF8.GetString(bytes.Span).Split(';', StringSplitOptions.RemoveEmptyEntries), "v1")
    ]);

    private static byte[] Payload(params string[] aliases) => Encoding.UTF8.GetBytes(string.Join(';', aliases));

    private static IAsyncEnumerable<(string, byte[]?, string?, DateTimeOffset?)> Changes(
        params (string Name, byte[]? Bytes)[] changes) =>
        changes.Select(c => (c.Name, c.Bytes, (string?)null, (DateTimeOffset?)null)).ToAsyncEnumerable();

    private static IAsyncEnumerable<(string, byte[], string?, DateTimeOffset?)> Puts(params (string Name, byte[] Bytes)[] puts) =>
        puts.Select(p => (p.Name, p.Bytes, (string?)null, (DateTimeOffset?)null)).ToAsyncEnumerable();

    [Fact]
    public async Task TwoEntitiesClaimingOneAlias_IsRejected_AndNothingIsStored()
    {
        await _dataset.Put(L1, Payload(B1), null, null);
        ulong version = await _dataset.GetLatestVersion();

        var claimTwice = () => _dataset.PutMany(Puts((L2, Payload(B2)), ("t/T/s/S/links/L3", Payload(B2))));

        await claimTwice.Should().ThrowAsync<AliasConflictException>();
        (await _dataset.GetLatestVersion()).Should().Be(version);
        _dataset.ResolveAlias(StoreEsl, B2).Should().BeNull();
    }

    [Fact]
    public async Task AnAliasFreedEarlierInTheStream_MayBeClaimed_ButNotOneFreedLater()
    {
        await _dataset.PutMany(Puts((L1, Payload(B1)), (L2, Payload(B2))));

        var claimThenFree = () => _dataset.PutOrDeleteMany(Changes(("t/T/s/S/links/L3", Payload(B2)), (L2, null)));
        await _dataset.PutOrDeleteMany(Changes((L1, null), ("t/T/s/S/links/L3", Payload(B1))));

        await claimThenFree.Should().ThrowAsync<AliasConflictException>();
        _dataset.ResolveAlias(StoreEsl, B1).Should().Be("t/T/s/S/links/L3");
        _dataset.ResolveAlias(StoreEsl, B2).Should().Be(L2);
    }

    [Fact]
    public async Task PutAll_ReleasesTheAliasesOfTheEntitiesItImplicitlyDeletes()
    {
        await _dataset.Put(L1, Payload(B1), null, null);

        await _dataset.PutAll(Puts((L2, Payload(B1))));

        (await _dataset.Get(L1)).Should().BeNull();
        _dataset.ResolveAlias(StoreEsl, B1).Should().Be(L2);
    }

    [Fact]
    public async Task PutAllChildren_ImplicitlyDeletesOnlyTheDirectChildrenOfTheParent()
    {
        const string descendant = "t/T/s/S/links/L2/items/I1";
        const string otherParentsChild = "t/T/s/S2/links/L9";
        var dataset = new InMemoryDataset<object>();
        byte[] bytes = [1];
        await dataset.PutMany(Puts((L1, bytes), (L2, bytes), (descendant, bytes), (otherParentsChild, bytes)));

        await dataset.PutAllChildren(Puts((L1, bytes)));

        (await dataset.Get(L2)).Should().BeNull();
        (await dataset.Get(L1)).Should().NotBeNull();
        (await dataset.Get(descendant)).Should().NotBeNull("it is not a direct child");
        (await dataset.Get(otherParentsChild)).Should().NotBeNull("its parent only starts with the same text");
    }
}
