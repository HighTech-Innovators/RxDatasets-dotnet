using Google.Protobuf;
using RxDatasets.Aliasing;
using RxDatasets.AliasingTests.Types;

namespace RxDatasets.AliasingTests;

public class IdSegmentTests
{
    private static readonly string[] IdPatterns = ["t/{tenantid}/s/{storeid}/items/{itemid}"];
    private static readonly TestEntity Item = new() { Id = "t/T/s/S/items/I1", Aliases = { "951a/b/c", "plain" } };

    [Fact]
    public void Encode_EncodesEveryByteOutsideTheUnreservedSet_AsLowercaseHex()
    {
        IdSegment.Encode("2'2вам'abc/''?*_%-+")
            .Should().Be("2~272~d0~b2~d0~b0~d0~bc~27abc~2f~27~27~3f~2a_~25-~2b");
    }

    // Not idempotent: a ~ in a raw value is data like any other byte.
    [Theory]
    [InlineData("~", "~7e")]
    [InlineData("~2f", "~7e2f")]
    [InlineData("~2F", "~7e2F")]
    [InlineData("10810283~~BOX", "10810283~7e~7eBOX")]
    public void Encode_EscapesTilde(string value, string encoded) => IdSegment.Encode(value).Should().Be(encoded);

    [Fact]
    public void AliasPattern_EncodesTildeFieldPlaceholders_PerValue()
    {
        var factory = PatternsAndExpressions.CreateStrictAliasFactory(IdPatterns,
            ["t/{tenantid}/s/{storeid}/itemkeys/{~.aliases[]}/values"], TestEntity.Parser);

        factory(Item.Id, Item.ToByteArray()).Should().BeEquivalentTo(
            "t/T/s/S/itemkeys/951a~2fb~2fc/values", "t/T/s/S/itemkeys/plain/values");
    }

    [Fact]
    public void AliasPattern_DoesNotEncodePlainFieldPlaceholders()
    {
        var factory = PatternsAndExpressions.CreateAliasFactoryDelegate(IdPatterns,
            ["t/{tenantid}/s/{storeid}/itemkeys/{.aliases[]}/values"], TestEntity.Parser);

        factory(Item.ToByteArray()).Should().BeEquivalentTo(
            "t/T/s/S/itemkeys/951a/b/c/values", "t/T/s/S/itemkeys/plain/values");
    }
}
