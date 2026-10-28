using Hightechinnovators.Dtos.DemodtoV1;
using Microsoft.Extensions.Logging.Abstractions;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.StorageProviders;
using Xunit;

namespace RxDatasets.Reporting.Cqs.Tests;

/// <summary>
/// A reporter that does not implement <see cref="IBatchedDatasetUpdateReporting{T}"/> gets one delta per transaction
/// from <see cref="DatasetUpdateReportingPlugin{TDto,TReporter}"/>, with the calls the plugin made before it could
/// batch; a batched reporter with a limit of 1 gets the same calls. These tests hold both to the expected calls.
/// </summary>
public class OneDeltaPerTransactionTests
{
    private delegate Task<InMemoryStorageProvider<Demodto>> StorageFactory();

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AllDeltasPresentGiveTheSameCalls(bool tree)
    {
        (Outcome standard, Outcome batching) =
            await RunBoth(() => DatasetUpdateReportingPluginTests.CreateStorage(8, tree: tree));

        batching.Should().BeEquivalentTo(standard);
        standard.Calls.Should().HaveCount(8 * 3).And.NotContain("rollback");
        standard.Cursor.Should().Be(8ul);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AMissingDataFileRollsBackAndSkipsTheDelta(bool tree)
    {
        // Delta 2 has two entities; reading the second fails after the first was already reported.
        (Outcome standard, Outcome batching) = await RunBoth(
            () => DatasetUpdateReportingPluginTests.CreateStorage(3, twoEntitiesVersion: 2, tree: tree),
            failRead: (version, position) => version == 2 && position > 0);

        standard.Calls.Should().Equal(
            "open 1", "new entity/1", "commit",
            "open 2", "new entity/2", "rollback",
            "open 3", "deleted entity/3", "commit");
        batching.Should().BeEquivalentTo(standard);
        standard.Cursor.Should().Be(3ul);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AReporterThatNeedsNoBytesGetsEveryDeltaWithoutADataRead(bool tree)
    {
        // Every data read would fail: a plugin that read the data would roll delta 2 back and skip it.
        (Outcome standard, Outcome batching) = await RunBoth(
            () => DatasetUpdateReportingPluginTests.CreateStorage(3, twoEntitiesVersion: 2, tree: tree),
            failRead: (_, _) => true, skipsDtoBytes: true);

        standard.Calls.Should().Equal(
            "open 1", "new entity/1 (0 bytes)", "commit",
            "open 2", "new entity/2 (0 bytes)", "new entity/2b (0 bytes)", "commit",
            "open 3", "deleted entity/3", "commit");
        standard.DataReads.Should().Be(0);
        batching.Should().BeEquivalentTo(standard);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AliasChangesAreReportedInTheSameOrder(bool tree)
    {
        (Outcome standard, Outcome batching) = await RunBoth(() => AliasReportingTests.AliasDeltas(tree));

        standard.Calls.Should().Equal(
            "open 1", "new entity/1", "alias by_value values/a entity/1 New", "commit",
            "open 2", "changed entity/2", "alias by_value values/b entity/2 Deleted",
            "alias by_value values/c entity/2 New", "commit",
            "open 3", "deleted entity/3", "alias by_value values/d entity/3 Deleted", "commit",
            "open 4", "alias by_value values/f entity/4 Deleted", "alias by_value values/e entity/4 New", "commit");
        batching.Should().BeEquivalentTo(standard);
    }

    [Fact]
    public async Task ADeltaWithoutAnIndexFileOpensNoTransaction()
    {
        (Outcome standard, Outcome batching) = await RunBoth(
            () => DatasetUpdateReportingPluginTests.CreateStorage(3, missingVersion: 2));

        standard.Calls.Should().Equal("open 1", "new entity/1", "commit", "open 3", "deleted entity/3", "commit");
        batching.Should().BeEquivalentTo(standard);
        standard.Cursor.Should().Be(3ul);
    }

    /// <summary>
    /// The plugin with a reporter that does not batch (<c>Standard</c>) and with one that batches with a limit of 1
    /// (<c>Batching</c>). Each gets its own storage, since both read and write the processed-version cursor.
    /// </summary>
    private static async Task<(Outcome Standard, Outcome Batching)> RunBoth(
        StorageFactory createStorage, Func<ulong, long, bool>? failRead = null, bool skipsDtoBytes = false)
    {
        Outcome standard = await Run(await createStorage(), failRead, new CallLogReporter { SkipsDtoBytes = skipsDtoBytes });
        Outcome batching = await Run(await createStorage(), failRead,
            new BatchedCallLogReporter { SkipsDtoBytes = skipsDtoBytes });
        return (standard, batching);
    }

    private static async Task<Outcome> Run<TReporter>(InMemoryStorageProvider<Demodto> storage,
        Func<ulong, long, bool>? failRead, TReporter reporter) where TReporter : CallLogReporter
    {
        IStorageProvider<Demodto> observed =
            DatasetUpdateReportingPluginTests.StorageProbe.Create(
                storage, out DatasetUpdateReportingPluginTests.StorageProbe probe);
        probe.FailRead = failRead;

        await new DatasetUpdateReportingPlugin<Demodto, TReporter>(observed,
                NullLogger<DatasetUpdateReportingPlugin<Demodto, TReporter>>.Instance, reporter)
            .HandleNewDeltas(CancellationToken.None);

        return new Outcome(reporter.Calls, await storage.GetProcessedVersion(reporter.ReportingIdentifier),
            probe.DataReads);
    }

    private sealed record Outcome(List<string> Calls, ulong Cursor, int DataReads);

    /// <summary>Implements only the members a reporter that does not batch implements.</summary>
    private class CallLogReporter : IDatasetUpdateReporting<Demodto>
    {
        public List<string> Calls { get; } = [];
        public string ReportingIdentifier => "one-delta-test-reporter";
        public bool SkipsDtoBytes { get; init; }

        public Task<object?> OpenDeltaTransaction(ulong deltaVersion)
        {
            Calls.Add($"open {deltaVersion}");
            return Task.FromResult<object?>(deltaVersion);
        }

        public Task OnAliasChanged(string aliasName, string alias, string entityName, ChangeType change,
            string? trackingId, DateTimeOffset? slaTimestamp, object? optionalTransaction) =>
            Log($"alias {aliasName} {alias} {entityName} {change}");

        public Task CommitDeltaTransaction(object state) => Log("commit");
        public Task RollBackDeltaTransaction(object state) => Log("rollback");

        public Task OnNewDto(string name, byte[] newProtoBytes, string? trackingId, DateTimeOffset? slaTimestamp,
            object? optionalTransaction) => Log(SkipsDtoBytes ? $"new {name} ({newProtoBytes.Length} bytes)" : $"new {name}");

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

    private sealed class BatchedCallLogReporter : CallLogReporter, IBatchedDatasetUpdateReporting<Demodto>
    {
        public int MaxDeltasPerTransaction => 1;
    }
}
