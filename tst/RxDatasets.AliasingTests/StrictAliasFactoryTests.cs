using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using RxDatasets.Abstractions;
using RxDatasets.Aliasing;
using RxDatasets.AliasingTests.Types.Relation;
using RxDatasets.InMemoryDataset;

namespace RxDatasets.AliasingTests;

/// <summary>
/// The writer-side alias factory and its registration, with a relation standing in for a link: one relation per
/// device, like <c>by_storeesl</c> (<c>{.id/[..4]}/esls/{.single.esl.barcode}/link</c>) allows one link per ESL.
/// </summary>
public class StrictAliasFactoryTests
{
    private static readonly string[] IdPatterns = ["t/{tenantid}/s/{storeid}/relations/{relationid}"];
    private const string ByDevice = "{.id/[..4]}/devices/{.single.device.device_id}/relation";

    private static Relation OnDevice(string id, string deviceId) =>
        new() { Id = id, Single = new Relation.Types.Single { Device = new Relation.Types.Device { DeviceId = deviceId } } };

    [Fact]
    public void StrictFactory_EvaluatesTheExpression_WithTheIdVariablesOfTheEntityName()
    {
        var factory = PatternsAndExpressions.CreateStrictAliasFactory(IdPatterns,
            [ByDevice, "t/{tenantid}/s/{storeid}/all/{relationid}"], Relation.Parser);

        var aliases = factory("t/T/s/S/relations/R1", OnDevice("t/T/s/S/relations/R1", "D1").ToByteArray());

        aliases.Should().BeEquivalentTo("t/T/s/S/devices/D1/relation", "t/T/s/S/all/R1");
    }

    [Fact]
    public void StrictFactory_Throws_OnBytesThatDontParse()
    {
        var factory = PatternsAndExpressions.CreateStrictAliasFactory(IdPatterns, [ByDevice], Relation.Parser);

        var evaluate = () => factory("t/T/s/S/relations/R1", new byte[] { 0xFF, 0xFF, 0xFF }).ToList();

        evaluate.Should().Throw<InvalidProtocolBufferException>();
    }

    [Fact]
    public void DefinitionHash_IgnoresTheOrder_ButNotThePatterns()
    {
        var hash = PatternsAndExpressions.DefinitionHash("by_device", ["a/{x}", "b/{y}"], [ByDevice, "c"]);

        PatternsAndExpressions.DefinitionHash("by_device", ["b/{y}", "a/{x}"], ["c", ByDevice]).Should().Be(hash);
        PatternsAndExpressions.DefinitionHash("by_device", ["a/{x}"], [ByDevice, "c"]).Should().NotBe(hash);
        PatternsAndExpressions.DefinitionHash("by_other", ["a/{x}", "b/{y}"], [ByDevice, "c"]).Should().NotBe(hash);
    }

    [Fact]
    public async Task RegisteredAliases_AreCheckedOnWrite_AndResolvedAtOnce()
    {
        var services = new ServiceCollection();
        RxDatasets.InMemoryDataset.DI.ConfigureServices<Relation>(services);
        services.AddDatasetAliases(Relation.Parser, IdPatterns, ("by_device", ByDevice));
        var serviceProvider = services.BuildServiceProvider();
        var writer = serviceProvider.GetRequiredService<IDatasetWriter<Relation>>();
        var byDevice = serviceProvider.GetDatasetReader<Relation>("by_device")!;

        await writer.Put("t/T/s/S/relations/R1", OnDevice("t/T/s/S/relations/R1", "D1").ToByteArray(), null, null);
        var secondOnDevice = () =>
            writer.Put("t/T/s/S/relations/R2", OnDevice("t/T/s/S/relations/R2", "D1").ToByteArray(), null, null);
        var garbage = () => writer.Put("t/T/s/S/relations/R3", [0xFF, 0xFF, 0xFF], null, null);

        (await byDevice.Get("t/T/s/S/devices/D1/relation"))!.Value.name.Should().Be("t/T/s/S/relations/R1");
        await secondOnDevice.Should().ThrowAsync<AliasConflictException>();
        await garbage.Should().ThrowAsync<ArgumentException>();
        serviceProvider.GetRequiredService<InMemoryDataset<Relation>>().ResolveAlias("by_device", "t/T/s/S/devices/D1/relation")
            .Should().Be("t/T/s/S/relations/R1");
    }
}
