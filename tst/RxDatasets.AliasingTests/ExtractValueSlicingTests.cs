using RxDatasets.Aliasing;

namespace RxDatasets.AliasingTests;

/// <summary>
/// Unit tests for the ExtractValueSlicing method.
/// Tests the value slice operator extraction logic in isolation.
/// </summary>
public class ExtractValueSlicingTests
{
    [Fact]
    public void Should_Extract_Leading_Slice_Pattern()
    {
        // Pattern: /[..N] - from start to N
        var result = DI.TestExtractValueSlicing("id/[..4]");
        
        result.path.Should().Be("id");
        result.sliceStart.Should().Be(0);
        result.sliceEnd.Should().Be(4);
    }

    [Fact]
    public void Should_Extract_Range_Slice_Pattern_With_Both_Bounds()
    {
        // Pattern: /[M..N] - from M to N
        var result = DI.TestExtractValueSlicing("path/[1..3]");
        
        result.path.Should().Be("path");
        result.sliceStart.Should().Be(1);
        result.sliceEnd.Should().Be(3);
    }

    [Fact]
    public void Should_Extract_Range_Slice_Pattern_With_Start_Only()
    {
        // Pattern: /[M..] - from M to end
        var result = DI.TestExtractValueSlicing("field/[2..]");
        
        result.path.Should().Be("field");
        result.sliceStart.Should().Be(2);
        result.sliceEnd.Should().BeNull();
    }

    [Fact]
    public void Should_Extract_Single_Index_Slice_Pattern()
    {
        // Pattern: /[N] - single index N
        var result = DI.TestExtractValueSlicing("value/[3]");
        
        result.path.Should().Be("value");
        result.sliceStart.Should().Be(3);
        result.sliceEnd.Should().Be(4);
    }

    [Fact]
    public void Should_Return_Original_Path_When_No_Slice_Pattern()
    {
        var result = DI.TestExtractValueSlicing("simple.path");
        
        result.path.Should().Be("simple.path");
        result.sliceStart.Should().BeNull();
        result.sliceEnd.Should().BeNull();
    }

    [Fact]
    public void Should_Handle_Complex_Nested_Path_With_Slice()
    {
        var result = DI.TestExtractValueSlicing("multi.product.related_products.product_id/[..2]");
        
        result.path.Should().Be("multi.product.related_products.product_id");
        result.sliceStart.Should().Be(0);
        result.sliceEnd.Should().Be(2);
    }

    [Fact]
    public void Should_Handle_Zero_Index_Slice()
    {
        var result = DI.TestExtractValueSlicing("id/[0]");
        
        result.path.Should().Be("id");
        result.sliceStart.Should().Be(0);
        result.sliceEnd.Should().Be(1);
    }

    [Fact]
    public void Should_Handle_Leading_Slice_With_Zero()
    {
        var result = DI.TestExtractValueSlicing("id/[..0]");
        
        result.path.Should().Be("id");
        result.sliceStart.Should().Be(0);
        result.sliceEnd.Should().Be(0);
    }

    [Fact]
    public void Should_Handle_Large_Index_Values()
    {
        var result = DI.TestExtractValueSlicing("field/[100..500]");
        
        result.path.Should().Be("field");
        result.sliceStart.Should().Be(100);
        result.sliceEnd.Should().Be(500);
    }

    [Fact]
    public void Should_Not_Extract_Brackets_In_Middle_Of_Path()
    {
        // Brackets in the middle (path indexing) should not be extracted
        // This tests that we only extract value slicing at the END
        var result = DI.TestExtractValueSlicing("products[0].value");
        
        result.path.Should().Be("products[0].value");
        result.sliceStart.Should().BeNull();
        result.sliceEnd.Should().BeNull();
    }

    [Fact]
    public void Should_Handle_Complex_Path_With_Array_Indexing_And_Value_Slicing()
    {
        // Both array operator AND value slicing - value slicing is at the end
        var result = DI.TestExtractValueSlicing("products[..2].id/[..4]");
        
        result.path.Should().Be("products[..2].id");
        result.sliceStart.Should().Be(0);
        result.sliceEnd.Should().Be(4);
    }
}
