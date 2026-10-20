using RxDatasets.Models;

namespace RxDatasets.Tests.Models;

public class IndexKeysTests
{
    /// <summary>
    /// Every <c>__idpattern__</c> and <c>__aliaspattern__</c> in the DTOflow protos (data-platform/protos/*.proto).
    /// </summary>
    public static readonly TheoryData<string> RealPatterns =
    [
        "canvastypes/{canvastypeid}",
        "esltypes/{esltypeid}",
        "esltypes/{esltypeid}/driver",
        "palettes/{paletteid}",
        "t/{tenantid}/canvasdesigns/{canvasdesignid}/templates/{sha}",
        "t/{tenantid}/canvasdesigns/{designid}",
        "t/{tenantid}/canvasdesigns/{designid}/s/{storeid}/links/{linkid}",
        "t/{tenantid}/canvasdesigns/{designid}/s/{storeid}/links/{linkid}/designer",
        "t/{tenantid}/canvasdesigns/{designid}/s/{storeid}/links/{linkid}/studio",
        "t/{tenantid}/communicationpacks/{communicationpackid}",
        "t/{tenantid}/communicationpacks/{communicationpackid}/stores/{storeid}/studioconfig",
        "t/{tenantid}/curatedproperties/{propertykey}",
        "t/{tenantid}/curatedpropertyconfig",
        "t/{tenantid}/designimages/{imageid}",
        "t/{tenantid}/designimagetags/{tag}/designimages/{imageid}",
        "t/{tenantid}/designs/{designid}",
        "t/{tenantid}/designs/{designid}/s/{storeid}/links/{linkid}",
        "t/{tenantid}/designs/{designid}/s/{storeid}/links/{linkid}/designer",
        "t/{tenantid}/designs/{designid}/s/{storeid}/links/{linkid}/studio",
        "t/{tenantid}/designs/{designid}/templates/{sha}",
        "t/{tenantid}/eccfonts/{fontid}",
        "t/{tenantid}/fonts/{fontid}",
        "t/{tenantid}/s/{externalid}",
        "t/{tenantid}/s/{storeid}",
        "t/{tenantid}/s/{storeid}/eccimages/{imageid}",
        "t/{tenantid}/s/{storeid}/eccmodels/{eccmodelid}/links/{linkid}/ecc",
        "t/{tenantid}/s/{storeid}/eccmodels/{modelid}",
        "t/{tenantid}/s/{storeid}/eccparameters",
        "t/{tenantid}/s/{storeid}/esls/{barcode}",
        "t/{tenantid}/s/{storeid}/esls/{barcode}/link",
        "t/{tenantid}/s/{storeid}/esls/{barcode}/link/ecc",
        "t/{tenantid}/s/{storeid}/esls/{barcode}/link/studio",
        "t/{tenantid}/s/{storeid}/esls/{barcode}/pages/{pagenum}/eccimage",
        "t/{tenantid}/s/{storeid}/esls/{barcode}/pages/{pagenum}/image",
        "t/{tenantid}/s/{storeid}/esls/{barcode}/pages/{pagenum}/renderedimage",
        "t/{tenantid}/s/{storeid}/esls/{barcode}/pages/{pagenum}/segmentstate",
        "t/{tenantid}/s/{storeid}/esls/{barcode}/pages/{pagenum}/studioimage",
        "t/{tenantid}/s/{storeid}/esls/{barcode}/status",
        "t/{tenantid}/s/{storeid}/itemapiparameters",
        "t/{tenantid}/s/{storeid}/itemprocessingparameters",
        "t/{tenantid}/s/{storeid}/itemproperties",
        "t/{tenantid}/s/{storeid}/items/{itemid}/curated",
        "t/{tenantid}/s/{storeid}/items/{itemid}/links/{linkid}",
        "t/{tenantid}/s/{storeid}/items/{itemid}/links/{linkid}/designer",
        "t/{tenantid}/s/{storeid}/items/{itemid}/links/{linkid}/ecc",
        "t/{tenantid}/s/{storeid}/items/{itemid}/links/{linkid}/studio",
        "t/{tenantid}/s/{storeid}/items/{itemid}/values",
        "t/{tenantid}/s/{storeid}/itemsics/{sic}/values",
        "t/{tenantid}/s/{storeid}/links/{linkid}",
        "t/{tenantid}/s/{storeid}/links/{linkid}/designer",
        "t/{tenantid}/s/{storeid}/links/{linkid}/ecc",
        "t/{tenantid}/s/{storeid}/links/{linkid}/studio",
        "t/{tenantid}/s/{storeid}/studioconfig",
        "t/{tenantid}/tasks/{taskid}",
        "t/{tenantid}/tenant",
    ];

    private static string Instantiate(string pattern, string valuePrefix) =>
        string.Join('/', pattern.Split('/').Select((segment, i) => IsVariable(segment) ? $"{valuePrefix}{i}" : segment));

    private static bool IsVariable(string segment) => segment.StartsWith('{');

    /// <summary>Rebuilds a name from its key the way the index enumerators do.</summary>
    private static string ToName(IndexKeys.Key key)
    {
        string path = "";
        for (int i = 0; i < key.Path.Length; i += 2)
            path = IndexKeys.Join(path, key.Path[i], key.Path[i + 1]);
        return IndexKeys.Join(path, IndexKeys.LeafName(key.Template, key.Variable));
    }

    [Theory]
    [MemberData(nameof(RealPatterns))]
    public void Split_RoundTrips_ForEveryRealPattern(string pattern)
    {
        var name = Instantiate(pattern, "v");

        var key = IndexKeys.Split(name);

        ToName(key).Should().Be(name);
        // Node path plus leaf key, joined again, is the name
        var nodeKeys = key.Path.Chunk(2).Select(pair => string.Join('/', pair));
        string.Join('/', nodeKeys.Append(IndexKeys.LeafName(key.Template, key.Variable))).Should().Be(name);
    }

    [Theory]
    [MemberData(nameof(RealPatterns))]
    public void Split_SharesEveryFixedPart_ForNamesOfTheSamePattern(string pattern)
    {
        var segments = pattern.Split('/');
        var first = IndexKeys.Split(Instantiate(pattern, "a"));
        var second = IndexKeys.Split(Instantiate(pattern, "b"));

        first.Path.Should().HaveSameCount(second.Path);
        for (int i = 0; i < first.Path.Length; i++)
            (first.Path[i] == second.Path[i]).Should().Be(!IsVariable(segments[i]), $"segment {i} of {pattern}");

        first.Template.Should().Be(second.Template, "the template holds only fixed parts");
        int variableSegment = segments.Length == 1 ? 0 : first.Path.Length + 1;
        (first.Variable == second.Variable).Should().Be(!IsVariable(segments[variableSegment]));
    }

    [Theory]
    [InlineData("t/T/s/S/items/I/values", new[] { "t", "T", "s", "S" }, "items/{}/values", "I")]
    [InlineData("t/T/s/S/esls/B/pages/P/studioimage", new[] { "t", "T", "s", "S", "esls", "B" }, "pages/{}/studioimage", "P")]
    [InlineData("t/T/s/S/eccparameters", new[] { "t", "T" }, "s/{}/eccparameters", "S")]
    [InlineData("t/T/designs/D/templates/SHA", new[] { "t", "T", "designs", "D" }, "templates/{}", "SHA")]
    [InlineData("t/T/tenant", new string[0], "t/{}/tenant", "T")]
    [InlineData("esltypes/E", new string[0], "esltypes/{}", "E")]
    [InlineData("esltypes/E/driver", new string[0], "esltypes/{}/driver", "E")]
    [InlineData("t/T/s/S/esls/B/link/studio", new[] { "t", "T", "s", "S", "esls", "B" }, "link/{}", "studio")]
    [InlineData("single", new string[0], "{}", "single")]
    [InlineData("a/b/", new string[0], "a/{}/", "b")]
    public void Split_FollowsThePairRule(string name, string[] path, string template, string variable)
    {
        var key = IndexKeys.Split(name);

        key.Path.Should().Equal(path);
        key.Template.Should().Be(template);
        key.Variable.Should().Be(variable);
        ToName(key).Should().Be(name);
    }
}
