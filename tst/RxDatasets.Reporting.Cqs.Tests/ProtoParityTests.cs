using System.Runtime.CompilerServices;
using Xunit;

namespace RxDatasets.Reporting.Cqs.Tests;

public class ProtoParityTests
{
    [Theory]
    [InlineData("change_queue.proto")]
    [InlineData("change_queue_ingress.proto")]
    public void ReporterContractIsByteIdenticalToCqsSource(string fileName)
    {
        string? sourceOverride = Environment.GetEnvironmentVariable("CQS_SOURCE_ROOT");
        string sourceRoot = string.IsNullOrWhiteSpace(sourceOverride)
            ? Path.GetFullPath(Path.Combine(GetTestSourceDirectory(), "..", "..", "..", "ChangeQueueService-rust"))
            : Path.GetFullPath(sourceOverride);
        string source = Path.Combine(sourceRoot, "protos", fileName);
        string copy = Path.Combine(AppContext.BaseDirectory, "Protos", fileName);
        File.Exists(source).Should().BeTrue(
            $"CQS source proto not found: {source}. Check out sibling ChangeQueueService-rust or set CQS_SOURCE_ROOT to its root.");
        File.Exists(copy).Should().BeTrue($"Reporter proto copy not found: {copy}. Build the test project to copy its contracts.");
        File.ReadAllBytes(source).Should().Equal(File.ReadAllBytes(copy),
            $"Reporter proto differs byte-for-byte from CQS source: {fileName}. Refresh the reporter contract copies.");
    }

    private static string GetTestSourceDirectory([CallerFilePath] string sourceFile = "") => Path.GetDirectoryName(sourceFile)!;
}