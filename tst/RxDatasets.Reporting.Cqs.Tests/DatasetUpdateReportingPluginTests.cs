using System.Reflection;
using System.Runtime.ExceptionServices;
using Google.Protobuf.WellKnownTypes;
using Google.Protobuf;
using Grpc.Core;
using Hti.RxDatasets.Cqs.Ingress.V1;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProviders;
using Xunit;

namespace RxDatasets.Reporting.Cqs.Tests;

public class BatchingDatasetUpdateReportingPluginTests
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(20);

    [Fact]
    public async Task TwoHundredQueuedDeltasUseOneRequestAndAdvanceCursorOnlyAfterDurableResponse()
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        InMemoryStorageProvider<Timestamp> storage = await CreateStorage(200);
        CqsReporter<Timestamp> reporter = CreateReporter(server);
        TaskCompletionSource arrived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<PushBatchResponse> response = new(TaskCreationOptions.RunContinuationsAsynchronously);
        server.Service.Handler = (request, context) =>
        {
            arrived.TrySetResult();
            return response.Task;
        };
        using CancellationTokenSource cancellation = new(TestTimeout);
        Task processing = CreatePlugin(storage, reporter).HandleNewDeltas(cancellation.Token);
        try
        {
            await arrived.Task.WaitAsync(TestTimeout);
            processing.IsCompleted.Should().BeFalse();
            (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(0ul);
            AssertTransaction(server.Service.Requests.Should().ContainSingle().Which, 1, 200);
        }
        finally
        {
            response.TrySetResult(new PushBatchResponse { Durable = true });
            await processing.WaitAsync(TestTimeout);
        }

        (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(200ul);
        server.Service.Requests.Should().ContainSingle();
        await CreatePlugin(storage, reporter).HandleNewDeltas(cancellation.Token);
        server.Service.Requests.Should().ContainSingle();
    }

    [Fact]
    public async Task ReporterWithALimitOfOneUsesOneTransactionPerDelta()
    {
        InMemoryStorageProvider<Timestamp> storage = await CreateStorage(4);
        RecordingReporter reporter = new();
        IStorageProvider<Timestamp> observedStorage = StorageProbe.Create(storage, out StorageProbe probe);

        await CreatePlugin(observedStorage, reporter).HandleNewDeltas(CancellationToken.None);

        reporter.OpenedVersions.Should().Equal(1ul, 2ul, 3ul, 4ul);
        reporter.CommittedStates.Should().Equal(reporter.States);
        reporter.RolledBackStates.Should().BeEmpty();
        reporter.Names.Should().Equal("entity/1", "entity/2", "entity/3", "entity/4");
        probe.CursorWriteAttempts.Should().Equal(1ul, 2ul, 3ul, 4ul);
        (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(4ul);
    }

    [Fact]
    public async Task MissingIndexIsSkippedAndRemainingDeltasCommitTogether()
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        InMemoryStorageProvider<Timestamp> storage = await CreateStorage(3, missingVersion: 2);
        CqsReporter<Timestamp> reporter = CreateReporter(server);

        await CreatePlugin(storage, reporter).HandleNewDeltas(CancellationToken.None);

        PushBatchRequest request = server.Service.Requests.Should().ContainSingle().Which;
        request.Notifications.Select(notification => notification.DtoId).Should().Equal("entity/1", "entity/3");
        (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(3ul);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NondurableOrUnavailableResponseRetriesAllTwoHundredDeltasWithoutAdvancingCursor(bool unavailable)
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        InMemoryStorageProvider<Timestamp> storage = await CreateStorage(200);
        CqsReporter<Timestamp> reporter = CreateReporter(server);
        IStorageProvider<Timestamp> observedStorage = StorageProbe.Create(storage, out StorageProbe probe);
        TaskCompletionSource firstArrived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource retryArrived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseFailure = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<PushBatchResponse> retryResponse = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int attempts = 0;
        server.Service.Handler = async (request, context) =>
        {
            if (Interlocked.Increment(ref attempts) == 1)
            {
                firstArrived.TrySetResult();
                await releaseFailure.Task;
                if (unavailable)
                {
                    throw new RpcException(new Status(StatusCode.Unavailable, "temporarily unavailable"));
                }

                return new PushBatchResponse { Durable = false };
            }

            retryArrived.TrySetResult();
            return await retryResponse.Task;
        };
        using CancellationTokenSource cancellation = new(TestTimeout);
        Task processing = CreatePlugin(observedStorage, reporter).HandleNewDeltas(cancellation.Token);
        try
        {
            await firstArrived.Task.WaitAsync(TestTimeout);
            (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(0ul);
            probe.CursorWriteAttempts.Should().BeEmpty();
            releaseFailure.TrySetResult();
            await retryArrived.Task.WaitAsync(TestTimeout);
            processing.IsCompleted.Should().BeFalse();
            (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(0ul);
            probe.CursorWriteAttempts.Should().BeEmpty();
            PushBatchRequest[] requests = server.Service.Requests.ToArray();
            requests.Should().HaveCount(2);
            AssertTransaction(requests[0], 1, 200);
            requests[1].Should().Be(requests[0]);
        }
        finally
        {
            releaseFailure.TrySetResult();
            retryResponse.TrySetResult(new PushBatchResponse { Durable = true });
            await processing.WaitAsync(TestTimeout);
        }

        probe.CursorWriteAttempts.Should().Equal(200ul);
        (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(200ul);
    }

    [Fact]
    public async Task CursorWriteFailureReplaysTheAlreadyDurableTransactionBeforeAdvancing()
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        InMemoryStorageProvider<Timestamp> storage = await CreateStorage(200);
        CqsReporter<Timestamp> reporter = CreateReporter(server);
        IStorageProvider<Timestamp> observedStorage = StorageProbe.Create(storage, out StorageProbe probe);
        probe.BeforeStore = (who, version) => probe.CursorWriteAttempts.Count == 1
            ? Task.FromException(new IOException("cursor write failed"))
            : Task.CompletedTask;
        TaskCompletionSource retryArrived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<PushBatchResponse> retryResponse = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int attempts = 0;
        server.Service.Handler = (request, context) =>
        {
            if (Interlocked.Increment(ref attempts) == 1)
            {
                return Task.FromResult(new PushBatchResponse { Durable = true });
            }

            retryArrived.TrySetResult();
            return retryResponse.Task;
        };
        using CancellationTokenSource cancellation = new(TestTimeout);
        Task processing = CreatePlugin(observedStorage, reporter).HandleNewDeltas(cancellation.Token);
        try
        {
            await retryArrived.Task.WaitAsync(TestTimeout);
            (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(0ul);
            probe.CursorWriteAttempts.Should().Equal(200ul);
            PushBatchRequest[] requests = server.Service.Requests.ToArray();
            requests.Should().HaveCount(2);
            AssertTransaction(requests[0], 1, 200);
            requests[1].Should().Be(requests[0]);
        }
        finally
        {
            retryResponse.TrySetResult(new PushBatchResponse { Durable = true });
            await processing.WaitAsync(TestTimeout);
        }

        probe.CursorWriteAttempts.Should().Equal(200ul, 200ul);
        (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(200ul);
    }

    [Fact]
    public async Task ConfiguredTransactionLimitResumesAfterCursorAndPersistsOnlyEachTransactionEnd()
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        InMemoryStorageProvider<Timestamp> storage = await CreateStorage(7);
        CqsReporter<Timestamp> reporter = CreateReporter(server, transactionLimit: 2);
        await storage.StoreProcessedVersion(reporter.ReportingIdentifier, 2);
        IStorageProvider<Timestamp> observedStorage = StorageProbe.Create(storage, out StorageProbe probe);

        await CreatePlugin(observedStorage, reporter).HandleNewDeltas(CancellationToken.None);

        PushBatchRequest[] requests = server.Service.Requests.ToArray();
        requests.Should().HaveCount(3);
        AssertTransaction(requests[0], 3, 2);
        AssertTransaction(requests[1], 5, 2);
        AssertTransaction(requests[2], 7, 1);
        probe.CursorWriteAttempts.Should().Equal(4ul, 6ul, 7ul);
        (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(7ul);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CallbackOrCommitFailureRollsBackAndRetriesTheDeltaWithAFreshTransaction(bool failCommit)
    {
        InMemoryStorageProvider<Timestamp> storage = await CreateStorage(1);
        RecordingReporter reporter = new();
        Func<Task> failFirstTransaction = () => reporter.OpenedVersions.Count == 1
            ? Task.FromException(new IOException("reporting failed"))
            : Task.CompletedTask;
        if (failCommit)
        {
            reporter.BeforeCommit = failFirstTransaction;
        }
        else
        {
            reporter.AfterRecord = failFirstTransaction;
        }

        using CancellationTokenSource cancellation = new(TestTimeout);

        await CreatePlugin(storage, reporter).HandleNewDeltas(cancellation.Token);

        reporter.OpenedVersions.Should().Equal(1ul, 1ul);
        reporter.RolledBackStates.Should().ContainSingle().Which.Should().BeSameAs(reporter.States[0]);
        reporter.CommittedStates.Should().ContainSingle().Which.Should().BeSameAs(reporter.States[1]);
        reporter.States[1].Should().NotBeSameAs(reporter.States[0]);
        reporter.Names.Should().Equal("entity/1", "entity/1");
        (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(1ul);
    }

    [Fact]
    public async Task CancellationDuringDeltaRollsBackAndLeavesCursorForFullReplay()
    {
        InMemoryStorageProvider<Timestamp> storage = await CreateStorage(1);
        DeltaIndex index = DeltaIndex.Parser.ParseFrom((await storage.ReadDeltaIndex(1)).Span);
        index.DeletedNames.Add("second-entity");
        await storage.StoreDelta(1, index.ToByteArray(), new Timestamp { Seconds = 1 }.ToByteArray());
        using CancellationTokenSource cancellation = new(TestTimeout);
        RecordingReporter reporter = new()
        {
            AfterRecord = () =>
            {
                cancellation.Cancel();
                return Task.CompletedTask;
            }
        };

        await FluentActions.Awaiting(() => CreatePlugin(storage, reporter).HandleNewDeltas(cancellation.Token))
            .Should().ThrowAsync<OperationCanceledException>();

        reporter.Names.Should().Equal("entity/1");
        reporter.RolledBackStates.Should().ContainSingle().Which
            .Should().BeSameAs(reporter.States.Should().ContainSingle().Which);
        reporter.CommittedStates.Should().BeEmpty();
        (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(0ul);
        reporter.AfterRecord = null;

        await CreatePlugin(storage, reporter).HandleNewDeltas(CancellationToken.None);

        reporter.OpenedVersions.Should().Equal(1ul, 1ul);
        reporter.CommittedStates.Should().ContainSingle().Which.Should().BeSameAs(reporter.States[1]);
        reporter.Names.Should().Equal("entity/1", "entity/1", "second-entity");
        (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(1ul);
    }

    [Fact]
    public async Task CancellationDuringRetryBackoffPropagatesAfterRollbackWithoutWritingCursor()
    {
        InMemoryStorageProvider<Timestamp> storage = await CreateStorage(1);
        using CancellationTokenSource cancellation = new(TestTimeout);
        RecordingReporter reporter = new()
        {
            AfterRecord = () =>
            {
                cancellation.Cancel();
                throw new IOException("reporting failed while stopping");
            }
        };

        await FluentActions.Awaiting(() => CreatePlugin(storage, reporter).HandleNewDeltas(cancellation.Token))
            .Should().ThrowAsync<OperationCanceledException>();

        reporter.States.Should().ContainSingle();
        reporter.RolledBackStates.Should().ContainSingle().Which.Should().BeSameAs(reporter.States[0]);
        reporter.CommittedStates.Should().BeEmpty();
        (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(0ul);
    }

    [Fact]
    public async Task MissingDataFileRollsBackThatDeltaAndSkipsItWhileTheOthersCommit()
    {
        // The default limit of 1: what the delta had already handed to the reporter is discarded, as before.
        InMemoryStorageProvider<Timestamp> storage = await CreateStorage(3, twoEntitiesVersion: 2);
        RecordingReporter reporter = new();
        IStorageProvider<Timestamp> observedStorage = StorageProbe.Create(storage, out StorageProbe probe);
        probe.FailRead = (version, position) => version == 2 && position > 0;

        await CreatePlugin(observedStorage, reporter).HandleNewDeltas(CancellationToken.None);

        reporter.OpenedVersions.Should().Equal(1ul, 2ul, 3ul);
        reporter.CommittedStates.Should().Equal(reporter.States[0], reporter.States[2]);
        reporter.RolledBackStates.Should().Equal(reporter.States[1]);
        probe.CursorWriteAttempts.Should().Equal(1ul, 2ul, 3ul);
        (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(3ul);
    }

    [Fact]
    public async Task MissingDataFileInsideABatchCommitsTheEarlierDeltasAndSkipsOnlyTheBrokenOne()
    {
        InMemoryStorageProvider<Timestamp> storage = await CreateStorage(4, twoEntitiesVersion: 3);
        RecordingReporter reporter = new() { MaxDeltasPerTransaction = 10 };
        IStorageProvider<Timestamp> observedStorage = StorageProbe.Create(storage, out StorageProbe probe);
        probe.FailRead = (version, position) => version == 3 && position > 0;

        await CreatePlugin(observedStorage, reporter).HandleNewDeltas(CancellationToken.None);

        // First try 1-3 fails and is rolled back; 1-2 commit; 3 alone is skipped; 4 commits.
        reporter.OpenedVersions.Should().Equal(1ul, 1ul, 3ul, 4ul);
        reporter.CommittedStates.Should().Equal(reporter.States[1], reporter.States[3]);
        reporter.RolledBackStates.Should().Equal(reporter.States[0], reporter.States[2]);
        probe.CursorWriteAttempts.Should().Equal(2ul, 3ul, 4ul);
        (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(4ul);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task EveryDeltaFormatIsReportedWithTheEntitiesFullNames(bool tree)
    {
        // A tree delta leaves each entity's own name empty: the position in the tree is the name.
        InMemoryStorageProvider<Timestamp> storage = await CreateStorage(4, tree: tree);
        RecordingReporter reporter = new() { MaxDeltasPerTransaction = 10 };

        await CreatePlugin(storage, reporter).HandleNewDeltas(CancellationToken.None);

        reporter.Names.Should().Equal("entity/1", "entity/2", "entity/3", "entity/4");
        (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(4ul);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task EmptyOrAlreadyProcessedStorageDoesNotOpenATransaction(bool alreadyProcessed)
    {
        InMemoryStorageProvider<Timestamp> storage = await CreateStorage(alreadyProcessed ? 2 : 0);
        RecordingReporter reporter = new();
        if (alreadyProcessed)
        {
            await storage.StoreProcessedVersion(reporter.ReportingIdentifier, 2);
        }

        IStorageProvider<Timestamp> observedStorage = StorageProbe.Create(storage, out StorageProbe probe);

        await CreatePlugin(observedStorage, reporter).HandleNewDeltas(CancellationToken.None);

        reporter.OpenedVersions.Should().BeEmpty();
        reporter.Names.Should().BeEmpty();
        probe.CursorWriteAttempts.Should().BeEmpty();
        (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(alreadyProcessed ? 2ul : 0ul);
    }

    private static BatchingDatasetUpdateReportingPlugin<Timestamp, TReporter> CreatePlugin<TReporter>(
        IStorageProvider<Timestamp> storage, TReporter reporter) where TReporter : IBatchedDatasetUpdateReporting<Timestamp> =>
        new(storage, NullLogger<BatchingDatasetUpdateReportingPlugin<Timestamp, TReporter>>.Instance, reporter);

    private static CqsReporter<Timestamp> CreateReporter(FakeCqsServer server, int transactionLimit = 1000) =>
        new(server.Client, Options.Create(new CqsReportingConfig
        {
            Endpoint = server.Endpoint,
            MaxDeltasPerTransaction = transactionLimit,
            MaxNotificationsPerPush = 10000,
            DurabilityTimeout = TimeSpan.FromSeconds(5)
        }));

    /// <summary>Uses the production in-memory provider for real index/data reads and persisted reporting cursors.</summary>
    internal static async Task<InMemoryStorageProvider<Timestamp>> CreateStorage(int count, ulong? missingVersion = null,
        ulong? twoEntitiesVersion = null, bool tree = false)
    {
        InMemoryStorageProvider<Timestamp> storage = new(
            NullLogger<InMemoryStorageProvider<Timestamp>>.Instance, new NotificationsHub<Timestamp>());
        for (ulong version = 1; version <= (ulong)count; version++)
        {
            if (version == missingVersion)
            {
                continue;
            }

            byte[] data = new Timestamp { Seconds = (long)version }.ToByteArray();
            DeltaIndex index = new() { DeltaVersion = version };
            DeltaEntity entity = new() { Name = $"entity/{version}", Position = 0, Length = data.Length };
            switch (version % 4)
            {
                case 1:
                    index.NewEntities.Add(entity);
                    break;
                case 2:
                    index.ChangedEntities.Add(entity);
                    break;
                case 3:
                    index.DeletedNames.Add(entity.Name);
                    break;
                default:
                    index.DeletedEntities.Add(new DeletedEntity { Name = entity.Name });
                    break;
            }

            if (version == twoEntitiesVersion)
            {
                // Two new entities in one delta, the second at a non-zero position in the data file.
                index = new DeltaIndex { DeltaVersion = version };
                index.NewEntities.Add(new DeltaEntity { Name = $"entity/{version}", Position = 0, Length = data.Length });
                index.NewEntities.Add(new DeltaEntity
                {
                    Name = $"entity/{version}b", Position = data.Length, Length = data.Length
                });
                data = [.. data, .. data];
            }

            if (tree)
            {
                // What a writer stores: the entities in the index tree, with their names emptied.
                index.ConvertToTree();
            }

            await storage.StoreDelta(version, index.ToByteArray(), data);
        }

        return storage;
    }

    private static void AssertTransaction(PushBatchRequest request, int first, int count)
    {
        request.FireAndForget.Should().BeFalse();
        request.Notifications.Select(notification => notification.DtoId)
            .Should().Equal(Enumerable.Range(first, count).Select(version => $"entity/{version}"));
        request.Notifications.Should().AllSatisfy(
            notification => notification.DtoType.Should().Be(Timestamp.Descriptor.FullName));
    }

    private sealed class RecordingReporter : IBatchedDatasetUpdateReporting<Timestamp>
    {
        public int MaxDeltasPerTransaction { get; init; } = 1;
        public string ReportingIdentifier => "plugin-test-reporter";
        public List<ulong> OpenedVersions { get; } = [];
        public List<object> States { get; } = [];
        public List<object> CommittedStates { get; } = [];
        public List<object> RolledBackStates { get; } = [];
        public List<string> Names { get; } = [];
        public Func<Task>? AfterRecord { get; set; }
        public Func<Task>? BeforeCommit { get; set; }

        public Task<object?> OpenDeltaTransaction(ulong deltaVersion)
        {
            object state = new();
            OpenedVersions.Add(deltaVersion);
            States.Add(state);
            return Task.FromResult<object?>(state);
        }

        public async Task CommitDeltaTransaction(object state)
        {
            if (BeforeCommit != null)
            {
                await BeforeCommit();
            }

            CommittedStates.Add(state);
        }

        public Task RollBackDeltaTransaction(object state)
        {
            RolledBackStates.Add(state);
            return Task.CompletedTask;
        }

        public Task OnNewDto(string name, byte[] newProtoBytes, string? trackingId, DateTimeOffset? slaTimestamp,
            object? optionalTransaction) => Record(name, optionalTransaction);

        public Task OnChangedDto(string name, byte[] newProtoBytes, string? trackingId, DateTimeOffset? slaTimestamp,
            object? optionalTransaction) => Record(name, optionalTransaction);

        public Task OnDeletedDto(string name, string? trackingId, DateTimeOffset? slaTimestamp,
            object? optionalTransaction) => Record(name, optionalTransaction);

        private Task Record(string name, object? state)
        {
            state.Should().BeSameAs(States.Last());
            Names.Add(name);
            return AfterRecord?.Invoke() ?? Task.CompletedTask;
        }
    }

    /// <summary>Forwards to real in-memory storage, injecting only cursor-write failures and recording write attempts.</summary>
    public class StorageProbe : DispatchProxy
    {
        private InMemoryStorageProvider<Timestamp> _storage = null!;
        public List<ulong> CursorWriteAttempts { get; } = [];
        public Func<string, ulong, Task>? BeforeStore { get; set; }
        public Func<ulong, long, bool>? FailRead { get; set; }

        public static IStorageProvider<Timestamp> Create(InMemoryStorageProvider<Timestamp> storage, out StorageProbe probe)
        {
            IStorageProvider<Timestamp> proxy = Create<IStorageProvider<Timestamp>, StorageProbe>();
            probe = (StorageProbe)proxy;
            probe._storage = storage;
            return proxy;
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            ArgumentNullException.ThrowIfNull(targetMethod);
            if (targetMethod.Name == nameof(IStorageProvider.StoreProcessedVersion))
            {
                return StoreProcessedVersion((string)args![0]!, (ulong)args[1]!);
            }

            if (targetMethod.Name == nameof(IStorageProvider.ReadDataFromSources) && FailRead != null
                && args![0] is List<(ObjectNameTypeEnum type, ulong version, long position)> sources
                && sources.Any(source => FailRead(source.version, source.position)))
            {
                return Task.FromException<ReadOnlyMemory<byte>>(new FileNotFoundException("simulated missing data file"));
            }

            try
            {
                return targetMethod.Invoke(_storage, args);
            }
            catch (TargetInvocationException exception) when (exception.InnerException != null)
            {
                ExceptionDispatchInfo.Capture(exception.InnerException).Throw();
                throw;
            }
        }

        private async Task StoreProcessedVersion(string who, ulong version)
        {
            CursorWriteAttempts.Add(version);
            if (BeforeStore != null)
            {
                await BeforeStore(who, version);
            }

            await _storage.StoreProcessedVersion(who, version);
        }
    }
}