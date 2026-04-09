using FluentAssertions;
using RxDatasets.Models;

namespace RxDatasets.Tests;

public class NameCompressorTests
{
    private const string Pattern = "t/{tenantid}/s/{storeid}/items/{itemid}/values";

    [Fact]
    public void FromPattern_ReturnsNull_WhenNullOrEmpty()
    {
        NameCompressor.FromPattern(null).Should().BeNull();
        NameCompressor.FromPattern("").Should().BeNull();
        NameCompressor.FromPattern("   ").Should().BeNull();
    }

    [Fact]
    public void FromPattern_ReturnsNull_WhenNoFixedSegments()
    {
        NameCompressor.FromPattern("*/*/*/*").Should().BeNull();
    }

    [Fact]
    public void FromPattern_ReturnsNull_WhenNoVariableSegments()
    {
        NameCompressor.FromPattern("t/s/items/values").Should().BeNull();
    }

    [Fact]
    public void FromPattern_AcceptsStar_And_NamedPlaceholders()
    {
        var star = NameCompressor.FromPattern("t/*/s/*/items/*/values");
        var named = NameCompressor.FromPattern("t/{tenantid}/s/{storeid}/items/{itemid}/values");

        star.Should().NotBeNull();
        named.Should().NotBeNull();
        star!.VariableSegmentCount.Should().Be(3);
        named!.VariableSegmentCount.Should().Be(3);

        // Both produce the same compression
        star.Compress("t/abc/s/def/items/001/values")
            .Should().Be(named.Compress("t/abc/s/def/items/001/values"));
    }

    [Fact]
    public void FromPattern_AcceptsMixedPlaceholders()
    {
        var c = NameCompressor.FromPattern("t/{tenantid}/s/*/items/{itemid}/values")!;
        c.VariableSegmentCount.Should().Be(3);
        c.Compress("t/abc/s/def/items/001/values").Should().Be("abc/def/001");
    }

    [Fact]
    public void FromPattern_ReturnsCompressor_WhenMixed()
    {
        var c = NameCompressor.FromPattern(Pattern);
        c.Should().NotBeNull();
        c!.VariableSegmentCount.Should().Be(3);
    }

    [Theory]
    [InlineData("t/abc/s/def/items/001/values", "abc/def/001")]
    [InlineData("t/TENANT-1/s/STORE-2/items/0000000001/values", "TENANT-1/STORE-2/0000000001")]
    public void Compress_ExtractsVariableSegments(string full, string expected)
    {
        var c = NameCompressor.FromPattern(Pattern)!;
        c.Compress(full).Should().Be(expected);
    }

    [Fact]
    public void Compress_ReturnsOriginal_WhenSegmentCountMismatch()
    {
        var c = NameCompressor.FromPattern(Pattern)!;
        c.Compress("too/few").Should().Be("too/few");
        c.Compress("way/too/many/segments/here/for/this/pattern").Should().Be("way/too/many/segments/here/for/this/pattern");
    }

    [Theory]
    [InlineData("abc/def/001", "t/abc/s/def/items/001/values")]
    [InlineData("TENANT-1/STORE-2/0000000001", "t/TENANT-1/s/STORE-2/items/0000000001/values")]
    public void Expand_RestoresFullName(string compressed, string expected)
    {
        var c = NameCompressor.FromPattern(Pattern)!;
        c.Expand(compressed).Should().Be(expected);
    }

    [Fact]
    public void Expand_ReturnsOriginal_WhenSegmentCountMismatch()
    {
        var c = NameCompressor.FromPattern(Pattern)!;
        c.Expand("only/two").Should().Be("only/two");
    }

    [Fact]
    public void Compress_Expand_RoundTrip()
    {
        var c = NameCompressor.FromPattern(Pattern)!;
        var original = "t/abc/s/def/items/001/values";
        c.Expand(c.Compress(original)).Should().Be(original);
    }

    [Theory]
    [InlineData("", "")]
    [InlineData("t/abc/s/def/items", "abc/def")]
    [InlineData("t/abc/s", "abc")]
    [InlineData("t/abc", "abc")]
    public void CompressPrefix_StripsFixedSegments(string fullPrefix, string expected)
    {
        var c = NameCompressor.FromPattern(Pattern)!;
        c.CompressPrefix(fullPrefix).Should().Be(expected);
    }

    [Theory]
    [InlineData("", "")]
    [InlineData("abc", "t/abc/s")]
    [InlineData("abc/def", "t/abc/s/def/items")]
    public void ExpandPrefix_RestoresFixedSegments(string compressed, string expected)
    {
        var c = NameCompressor.FromPattern(Pattern)!;
        c.ExpandPrefix(compressed).Should().Be(expected);
    }

    [Fact]
    public void CompressPrefix_ExpandPrefix_RoundTrip()
    {
        var c = NameCompressor.FromPattern(Pattern)!;
        var prefix = "t/abc/s/def/items";
        c.ExpandPrefix(c.CompressPrefix(prefix)).Should().Be(prefix);
    }

    [Fact]
    public void SingleVariableSegment_Pattern()
    {
        var c = NameCompressor.FromPattern("items/*/values")!;
        c.VariableSegmentCount.Should().Be(1);
        c.Compress("items/abc/values").Should().Be("abc");
        c.Expand("abc").Should().Be("items/abc/values");
    }

    [Fact]
    public void LeadingVariable_Pattern()
    {
        // Pattern where the first segment is variable
        var c = NameCompressor.FromPattern("*/items/*/values")!;
        c.VariableSegmentCount.Should().Be(2);
        c.Compress("tenant1/items/001/values").Should().Be("tenant1/001");
        c.Expand("tenant1/001").Should().Be("tenant1/items/001/values");
    }

    [Fact]
    public void ConsecutiveVariables_Pattern()
    {
        var c = NameCompressor.FromPattern("prefix/*/*/suffix")!;
        c.VariableSegmentCount.Should().Be(2);
        c.Compress("prefix/a/b/suffix").Should().Be("a/b");
        c.Expand("a/b").Should().Be("prefix/a/b/suffix");
    }
}
