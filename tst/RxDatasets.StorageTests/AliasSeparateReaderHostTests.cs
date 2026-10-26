using FluentAssertions;
using Hightechinnovators.Dtos.DemodtoV1;
using Microsoft.Extensions.Logging;
using Moq;
using RxDatasets.TestImplementations.InMemoryProvider;
using static RxDatasets.StorageTests.AliasTestHost;

namespace RxDatasets.StorageTests;

/// <summary>
/// Aliases with the default separate DatasetReader and DatasetWriter: the reader keeps its own copy of the aliases, and
/// catches up on a write in the background.
/// </summary>
public class AliasSeparateReaderHostTests
{
    private const string A = "things/A";
    private const string B = "things/B";

    private readonly LowLevelInMemoryStorage<Demodto> _storage =
        new(Mock.Of<ILogger<LowLevelInMemoryStorage<Demodto>>>());

    private static Task OwnerBecomes(AliasTestHost host, string alias, string? owner) =>
        Eventually(async () => (await host.Alias(ByValue).Get(alias))?.name == owner, $"{alias} is owned by {owner}");

    [Fact]
    public async Task TheReadersAliases_FollowTheWrites()
    {
        await using var host = await AliasTestHost.Start(_storage, new AliasHostOptions { Shared = false });

        await host.Writer.Put(A, Entity(A, "x"), null, null);
        await OwnerBecomes(host, "values/x", A);

        await host.Writer.Delete(A, null, null);
        await OwnerBecomes(host, "values/x", null);

        await host.Writer.Put(B, Entity(B, "x"), null, null);
        await OwnerBecomes(host, "values/x", B);
    }

    [Fact]
    public async Task TheReader_GetsTheRebuildOfChangedDefinitions()
    {
        ulong version;
        await using (var host = await AliasTestHost.Start(_storage))
            version = (await host.Writer.PutMany(Puts((A, "x"), (B, "y")))).NewDatasetVersion;

        await using var rebuilt = await AliasTestHost.Start(_storage,
            new AliasHostOptions { Shared = false, Aliases = [(ByValue, "v2/{.some_string}"), DefaultAliases[1]] });

        await Eventually(async () => (await rebuilt.Reader.GetChangedNames(0)).ToVersion == version + 1,
            "the reader has the rebuild delta");
        (await rebuilt.Alias(ByValue).Get("v2/x"))!.Value.name.Should().Be(A);
        (await rebuilt.Alias(ByValue).Get("v2/y"))!.Value.name.Should().Be(B);
        (await rebuilt.Alias(ByValue).Get("values/x")).Should().BeNull();
    }
}
