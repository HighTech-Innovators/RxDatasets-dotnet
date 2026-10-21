using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging.Abstractions;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.StorageProviders;
using Xunit;

namespace RxDatasets.Reporting.Cqs.Tests;

/// <summary>
/// The batching plugin is a sibling of the standard <see cref="DatasetUpdateReportingPlugin{TDto,TReporter}"/>,
/// so that plugin and its reporters stay untouched. With a limit of 1 the two must make the same reporter
/// calls in the same order; these tests hold them to it. The one documented difference has its own test.
/// </summary>
public class ParityWithStandardPluginTests
{
    private delegate Task<InMemoryStorageProvider<Timestamp>> StorageFactory();

    [Fact]
    public async Task AllDeltasPresentGiveTheSameCalls()
    {
        (Outcome standard, Outcome batching) = await RunBoth(() => BatchingDatasetUpdateReportingPluginTests.CreateStorage(8));

        batching.Should().BeEquivalentTo(standard);
        standard.Calls.Should().HaveCount(8 * 3).And.NotContain("rollback");
        standard.Cursor.Should().Be(8ul);
    }

    [Fact]
    public async Task AMissingDataFileRollsBackAndSkipsTheDeltaInBothPlugins()
    {
        // Delta 2 has two entities; reading the second fails after the first was already reported.
        (Outcome standard, Outcome batching) = await RunBoth(
            () => BatchingDatasetUpdateReportingPluginTests.CreateStorage(3, twoEntitiesVersion: 2),
            failRead: (version, position) => version == 2 && position > 0);

        standard.Calls.Should().Equal(
            "open 1", "new entity/1", "commit",
            "open 2", "new entity/2", "rollback",
            "open 3", "deleted entity/3", "commit");
        batching.Should().BeEquivalentTo(standard);
        standard.Cursor.Should().Be(3ul);
    }

    [Fact]
    public async Task ADeltaWithoutAnIndexFileIsTheOneDocumentedDifference()
    {
        (Outcome standard, Outcome batching) = await RunBoth(
            () => BatchingDatasetUpdateReportingPluginTests.CreateStorage(3, missingVersion: 2));

        // The standard plugin never opens a transaction for a delta it skips. The batching plugin has already
        // opened the transaction that delta sits in, and commits it empty.
        standard.Calls.Should().Equal("open 1", "new entity/1", "commit", "open 3", "deleted entity/3", "commit");
        batching.Calls.Should().Equal(
            "open 1", "new entity/1", "commit", "open 2", "commit", "open 3", "deleted entity/3", "commit");
        batching.Cursor.Should().Be(standard.Cursor).And.Be(3ul);
    }

    /// <summary>Each plugin gets its own storage, since both read and write the processed-version cursor.</summary>
    private static async Task<(Outcome Standard, Outcome Batching)> RunBoth(
        StorageFactory createStorage, Func<ulong, long, bool>? failRead = null)
    {
        Outcome standard = await Run(await createStorage(), failRead, (storage, reporter) =>
            new DatasetUpdateReportingPlugin<Timestamp, CallLogReporter>(storage,
                NullLogger<DatasetUpdateReportingPlugin<Timestamp, CallLogReporter>>.Instance, reporter));
        Outcome batching = await Run(await createStorage(), failRead, (storage, reporter) =>
            new BatchingDatasetUpdateReportingPlugin<Timestamp, CallLogReporter>(storage,
                NullLogger<BatchingDatasetUpdateReportingPlugin<Timestamp, CallLogReporter>>.Instance, reporter));
        return (standard, batching);
    }

    private static async Task<Outcome> Run(InMemoryStorageProvider<Timestamp> storage, Func<ulong, long, bool>? failRead,
        Func<IStorageProvider<Timestamp>, CallLogReporter, IDatasetUpdateConsumer<Timestamp>> createPlugin)
    {
        IStorageProvider<Timestamp> observed =
            BatchingDatasetUpdateReportingPluginTests.StorageProbe.Create(storage, out var probe);
        probe.FailRead = failRead;
        CallLogReporter reporter = new();

        await createPlugin(observed, reporter).HandleNewDeltas(CancellationToken.None);

        return new Outcome(reporter.Calls, await storage.GetProcessedVersion(reporter.ReportingIdentifier));
    }

    private sealed record Outcome(List<string> Calls, ulong Cursor);

    private sealed class CallLogReporter : IBatchedDatasetUpdateReporting<Timestamp>
    {
        public List<string> Calls { get; } = [];
        public int MaxDeltasPerTransaction => 1;
        public string ReportingIdentifier => "parity-test-reporter";

        public Task<object?> OpenDeltaTransaction(ulong deltaVersion)
        {
            Calls.Add($"open {deltaVersion}");
            return Task.FromResult<object?>(deltaVersion);
        }

        public Task CommitDeltaTransaction(object state) => Log("commit");
        public Task RollBackDeltaTransaction(object state) => Log("rollback");

        public Task OnNewDto(string name, byte[] newProtoBytes, string? trackingId, DateTimeOffset? slaTimestamp,
            object? optionalTransaction) => Log($"new {name}");

        public Task OnChangedDto(string name, byte[] newProtoBytes, string? trackingId, DateTimeOffset? slaTimestamp,
            object? optionalTransaction) => Log($"changed {name}");

        public Task OnDeletedDto(string name, string? trackingId, DateTimeOffset? slaTimestamp,
            object? optionalTransaction) => Log($"deleted {name}");

        private Task Log(string call)
        {
            Calls.Add(call);
            return Task.CompletedTask;
        }
    }
}
