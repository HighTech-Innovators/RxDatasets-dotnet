using System.Runtime.CompilerServices;
using Xunit;

namespace RxDatasets.Reporting.Cqs.Tests;

/// <summary>
/// A <see cref="TheoryAttribute"/> that skips itself when the CQS checkout is not available. This repository
/// builds on its own in CI, where the sibling ChangeQueueService-rust is not there; wherever it is (a developer
/// checkout, or <c>CQS_SOURCE_ROOT</c>), the test runs and fails on any drift.
/// </summary>
public sealed class CqsSourceTheoryAttribute : TheoryAttribute
{
    public CqsSourceTheoryAttribute()
    {
        if (!Directory.Exists(CqsSource.Root))
        {
            Skip = $"CQS source not found at {CqsSource.Root}. " +
                "Check out the sibling ChangeQueueService-rust or set CQS_SOURCE_ROOT.";
        }
    }
}

internal static class CqsSource
{
    public static string Root
    {
        get
        {
            string? sourceOverride = Environment.GetEnvironmentVariable("CQS_SOURCE_ROOT");
            return string.IsNullOrWhiteSpace(sourceOverride)
                ? Path.GetFullPath(Path.Combine(TestSourceDirectory(), "..", "..", "..", "ChangeQueueService-rust"))
                : Path.GetFullPath(sourceOverride);
        }
    }

    private static string TestSourceDirectory([CallerFilePath] string sourceFile = "") => Path.GetDirectoryName(sourceFile)!;
}

public class ProtoParityTests
{
    [CqsSourceTheory]
    [InlineData("change_queue.proto")]
    [InlineData("change_queue_ingress.proto")]
    public void ReporterContractIsIdenticalToCqsSource(string fileName)
    {
        string source = Path.Combine(CqsSource.Root, "protos", fileName);
        string copy = Path.Combine(AppContext.BaseDirectory, "Protos", fileName);
        File.Exists(source).Should().BeTrue($"CQS source proto not found: {source}.");
        File.Exists(copy).Should().BeTrue(
            $"Reporter proto copy not found: {copy}. Build the test project to copy its contracts.");

        // Line endings are whatever git checked out on that machine; the contract is the text.
        Normalized(source).Should().Be(Normalized(copy),
            $"Reporter proto differs from the CQS source: {fileName}. Refresh the reporter contract copies.");
    }

    private static string Normalized(string path) => File.ReadAllText(path).Replace("\r\n", "\n");
}
