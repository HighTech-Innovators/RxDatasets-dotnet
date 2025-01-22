namespace RxDatasets.Tests;

public class SharedMethodsTest
{
    [Fact]
    public void BasicExtractNumberTest()
    {
        SharedMethods.ExtractNumber("deeplinks/deltas/13312/data.pbs").Should().Be(13312);
    }

    [Fact]
    public void V1ExtractNumberTest()
    {
        SharedMethods.ExtractNumber("deeplinks.v1/deltas/14/data.pbs").Should().Be(14);
    }
    [Fact]
    public void MoreNumbersExtractNumberTest()
    {
        SharedMethods.ExtractNumber("customRootFolder/Demodto/snapshots/14/").Should().Be(14);
    }
    
}