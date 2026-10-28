using System.Reflection;
using System.Runtime.ExceptionServices;
using Google.Protobuf;
using Grpc.Core;
using Hightechinnovators.Dtos.DemodtoV1;
using Hti.RxDatasets.Cqs.Ingress.V1;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.BackgroundServices;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProviders;
using Xunit;

namespace RxDatasets.Reporting.Cqs.Tests;

public class DatasetUpdateReportingPluginTests
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(20);
    // The plugin's wait before a retry, in every test that does not measure it: 5 s by default
    private static readonly TimeSpan FastRetry = TimeSpan.FromMilliseconds(20);

    [Fact]
    public async Task TwoHundredQueuedDeltasUseOneRequestAndAdvanceCursorOnlyAfterDurableResponse()
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        InMemoryStorageProvider<Demodto> storage = await CreateStorage(200);
        CqsReporter<Demodto> reporter = CreateReporter(server);
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
    public async Task StoppingCancelsTheOutstandingPushInsteadOfWaitingForTheDurableAnswer()
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        InMemoryStorageProvider<Demodto> storage = await CreateStorage(3);
        // The production timeout, 35 s, outlasts the host's default shutdown timeout of 30 s.
        CqsReporter<Demodto> reporter = new(server.Client,
            Options.Create(new CqsReportingConfig { Endpoint = server.Endpoint, ReplayRetainedHistory = true }),
            NullLogger<CqsReporter<Demodto>>.Instance);
        TaskCompletionSource arrived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        server.Service.Handler = async (request, context) =>
        {
            arrived.TrySetResult();
            await Task.Delay(Timeout.Infinite, context.CancellationToken);
            return new PushBatchResponse { Durable = true };
        };
        using CancellationTokenSource stopping = new();
        Task processing = CreatePlugin(storage, reporter).HandleNewDeltas(stopping.Token);
        await arrived.Task.WaitAsync(TestTimeout);

        await stopping.CancelAsync();
        await FluentActions.Awaiting(() => processing.WaitAsync(TimeSpan.FromSeconds(5)))
            .Should().ThrowAsync<OperationCanceledException>();
        (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(0ul);
    }

    [Fact]
    public async Task FailuresInARowAreRetriedWithAGrowingDelay()
    {
        TimeSpan firstDelay = TimeSpan.FromMilliseconds(500);
        await using FakeCqsServer server = await FakeCqsServer.Start();
        InMemoryStorageProvider<Demodto> storage = await CreateStorage(1);
        CqsReporter<Demodto> reporter = CreateReporter(server);
        List<DateTime> attempts = [];
        server.Service.Handler = (request, context) =>
        {
            lock (attempts)
            {
                attempts.Add(DateTime.UtcNow);
                return Task.FromResult(new PushBatchResponse { Durable = attempts.Count == 3 });
            }
        };
        using CancellationTokenSource cancellation = new(TimeSpan.FromSeconds(30));

        await CreatePlugin(storage, reporter, firstDelay).HandleNewDeltas(cancellation.Token);

        attempts.Should().HaveCount(3);
        (attempts[1] - attempts[0]).TotalSeconds.Should().BeInRange(0.45, 1.5, "the first retry waits the first delay");
        (attempts[2] - attempts[1]).TotalSeconds.Should().BeInRange(0.95, 2.5, "the second failure in a row doubles it");
        (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(1ul);
    }

    [Fact]
    public async Task APushThatFailsAfterAnEarlierFlushReplaysTheWholeTransactionWithoutMovingTheCursor()
    {
        // Five deltas in one transaction, pushed two notifications at a time: 1-2 and 3-4 as they fill, 5 on commit.
        await using FakeCqsServer server = await FakeCqsServer.Start();
        InMemoryStorageProvider<Demodto> storage = await CreateStorage(5);
        CqsReporter<Demodto> reporter = CreateReporter(server, maxNotificationsPerPush: 2);
        IStorageProvider<Demodto> observedStorage = StorageProbe.Create(storage, out StorageProbe probe);
        int attempts = 0;
        server.Service.Handler = (request, context) => Interlocked.Increment(ref attempts) == 2
            ? throw new RpcException(new Status(StatusCode.Unavailable, "temporarily unavailable"))
            : Task.FromResult(new PushBatchResponse { Durable = true, AcceptedCount = request.Notifications.Count });
        using CancellationTokenSource cancellation = new(TestTimeout);

        await CreatePlugin(observedStorage, reporter).HandleNewDeltas(cancellation.Token);

        // 1-2 was durable when 3-4 failed, but the cursor covers only committed transactions: all of it is pushed again.
        PushBatchRequest[] requests = server.Service.Requests.ToArray();
        requests.Should().HaveCount(5);
        AssertTransaction(requests[0], 1, 2);
        AssertTransaction(requests[1], 3, 2);
        AssertTransaction(requests[2], 1, 2);
        AssertTransaction(requests[3], 3, 2);
        AssertTransaction(requests[4], 5, 1);
        probe.CursorWriteAttempts.Should().Equal(5ul);
        (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(5ul);
    }

    [Fact]
    public void AReporterWithANonPositiveTransactionLimitIsRefusedWhenThePluginIsCreated()
    {
        RecordingReporter reporter = new() { MaxDeltasPerTransaction = 0 };

        FluentActions.Invoking(() => CreatePlugin(new InMemoryStorageProvider<Demodto>(
                NullLogger<InMemoryStorageProvider<Demodto>>.Instance, new NotificationsHub<Demodto>()), reporter))
            .Should().Throw<ArgumentOutOfRangeException>().WithMessage("*MaxDeltasPerTransaction must be positive*");
    }

    [Fact]
    public async Task AFailedAssertionInAReporterCallbackEndsTheRunInsteadOfBeingRetried()
    {
        InMemoryStorageProvider<Demodto> storage = await CreateStorage(1);
        RecordingReporter reporter = new() { AfterRecord = () => throw new Xunit.Sdk.XunitException("callback assertion") };

        // Without a token: if the plugin retried it, this would never return.
        await FluentActions.Awaiting(() => CreatePlugin(storage, reporter).HandleNewDeltas(CancellationToken.None))
            .Should().ThrowAsync<Xunit.Sdk.XunitException>().WithMessage("callback assertion");
    }

    [Fact]
    public async Task ReporterWithALimitOfOneUsesOneTransactionPerDelta()
    {
        InMemoryStorageProvider<Demodto> storage = await CreateStorage(4);
        RecordingReporter reporter = new();
        IStorageProvider<Demodto> observedStorage = StorageProbe.Create(storage, out StorageProbe probe);

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
        InMemoryStorageProvider<Demodto> storage = await CreateStorage(3, missingVersion: 2);
        CqsReporter<Demodto> reporter = CreateReporter(server);

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
        InMemoryStorageProvider<Demodto> storage = await CreateStorage(200);
        CqsReporter<Demodto> reporter = CreateReporter(server);
        IStorageProvider<Demodto> observedStorage = StorageProbe.Create(storage, out StorageProbe probe);
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
        InMemoryStorageProvider<Demodto> storage = await CreateStorage(200);
        CqsReporter<Demodto> reporter = CreateReporter(server);
        IStorageProvider<Demodto> observedStorage = StorageProbe.Create(storage, out StorageProbe probe);
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
    public async Task ACancellationThatIsNotTheStoppingTokensIsRetriedLikeAnyFailure()
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        InMemoryStorageProvider<Demodto> storage = await CreateStorage(2);
        CqsReporter<Demodto> reporter = CreateReporter(server);
        IStorageProvider<Demodto> observedStorage = StorageProbe.Create(storage, out StorageProbe probe);
        // What an HttpClient timeout in a storage SDK throws, while the plugin itself is not stopping
        probe.FailIndexRead = version => version == 2 && probe.IndexReads == 2
            ? new TaskCanceledException("The request was canceled due to the configured HttpClient.Timeout")
            : null;
        using CancellationTokenSource cancellation = new(TestTimeout);

        await CreatePlugin(observedStorage, reporter).HandleNewDeltas(cancellation.Token);

        AssertTransaction(server.Service.Requests.Should().ContainSingle().Which, 1, 2);
        probe.CursorWriteAttempts.Should().Equal(2ul);
    }

    [Fact]
    public async Task AfterALostLockTheRetryReadsTheCursorAgainInsteadOfMovingItBack()
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        InMemoryStorageProvider<Demodto> storage = await CreateStorage(3);
        CqsReporter<Demodto> reporter = CreateReporter(server);
        IStorageProvider<Demodto> observedStorage = StorageProbe.Create(storage, out StorageProbe probe);
        probe.BeforeStore = async (who, version) =>
        {
            if (probe.CursorWriteAttempts.Count != 1)
            {
                return;
            }

            // Meanwhile the lock moved to another instance, which stored deltas 4 and 5 and reported up to 5.
            for (ulong other = 4; other <= 5; other++)
            {
                await storage.StoreDelta(other, new DeltaIndex { DeltaVersion = other }.ToByteArray(), []);
            }

            await storage.StoreProcessedVersion(who, 5);
            throw new SynchronizationLockException("We can't write to storage as we have to get a lock first");
        };
        using CancellationTokenSource cancellation = new(TestTimeout);

        await CreatePlugin(observedStorage, reporter).HandleNewDeltas(cancellation.Token);

        AssertTransaction(server.Service.Requests.Should().ContainSingle().Which, 1, 3);
        probe.CursorWriteAttempts.Should().Equal(3ul);
        (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(5ul);
    }

    [Fact]
    public async Task ConfiguredTransactionLimitResumesAfterCursorAndPersistsOnlyEachTransactionEnd()
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        InMemoryStorageProvider<Demodto> storage = await CreateStorage(7);
        CqsReporter<Demodto> reporter = CreateReporter(server, transactionLimit: 2);
        await storage.StoreProcessedVersion(reporter.ReportingIdentifier, 2);
        IStorageProvider<Demodto> observedStorage = StorageProbe.Create(storage, out StorageProbe probe);

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
        InMemoryStorageProvider<Demodto> storage = await CreateStorage(1);
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
        InMemoryStorageProvider<Demodto> storage = await CreateStorage(1);
        DeltaIndex index = DeltaIndex.Parser.ParseFrom((await storage.ReadDeltaIndex(1)).Span);
        index.DeletedNames.Add("second-entity");
        await storage.StoreDelta(1, index.ToByteArray(), new Demodto { SomeInt = 1 }.ToByteArray());
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
        InMemoryStorageProvider<Demodto> storage = await CreateStorage(1);
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
        InMemoryStorageProvider<Demodto> storage = await CreateStorage(3, twoEntitiesVersion: 2);
        RecordingReporter reporter = new();
        IStorageProvider<Demodto> observedStorage = StorageProbe.Create(storage, out StorageProbe probe);
        probe.FailRead = (version, position) => version == 2 && position > 0;

        await CreatePlugin(observedStorage, reporter).HandleNewDeltas(CancellationToken.None);

        reporter.OpenedVersions.Should().Equal(1ul, 2ul, 3ul);
        reporter.CommittedStates.Should().Equal(reporter.States[0], reporter.States[2]);
        reporter.RolledBackStates.Should().Equal(reporter.States[1]);
        probe.CursorWriteAttempts.Should().Equal(1ul, 2ul, 3ul);
        (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(3ul);
    }

    [Fact]
    public async Task CqsReporterNeedsNoDataSoAMissingDataFileDoesNotSkipTheDelta()
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        InMemoryStorageProvider<Demodto> storage = await CreateStorage(3, twoEntitiesVersion: 2);
        CqsReporter<Demodto> reporter = CreateReporter(server);
        IStorageProvider<Demodto> observedStorage = StorageProbe.Create(storage, out StorageProbe probe);
        probe.FailRead = (_, _) => true;

        await CreatePlugin(observedStorage, reporter).HandleNewDeltas(CancellationToken.None);

        probe.DataReads.Should().Be(0);
        server.Service.Requests.Should().ContainSingle().Which.Notifications.Select(notification => notification.DtoId)
            .Should().Equal("entity/1", "entity/2", "entity/2b", "entity/3");
        (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(3ul);
    }

    [Fact]
    public async Task ByDefaultANewReporterStartsAtTheNewestDeltaInsteadOfReplayingTheHistory()
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        InMemoryStorageProvider<Demodto> storage = await CreateStorage(3);
        CqsReporter<Demodto> reporter = CreateReporter(server, replayRetainedHistory: false);
        DatasetUpdateReportingPlugin<Demodto, CqsReporter<Demodto>> plugin =
            CreatePlugin(StorageProbe.Create(storage, out StorageProbe probe), reporter);

        await plugin.HandleNewDeltas(CancellationToken.None);
        server.Service.Requests.Should().BeEmpty();
        probe.CursorWriteAttempts.Should().Equal(3ul);

        await AddDelta(storage, 4);
        await plugin.HandleNewDeltas(CancellationToken.None);
        server.Service.Requests.Should().ContainSingle().Which.Notifications.Should().ContainSingle()
            .Which.DtoId.Should().Be("entity/4");
        probe.IndexReads.Should().Be(1, "only delta 4 is read");
        (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(4ul);
    }

    [Fact]
    public async Task ANewReporterOnAnEmptyDatasetStillReportsItsFirstDelta()
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        InMemoryStorageProvider<Demodto> storage = await CreateStorage(0);
        CqsReporter<Demodto> reporter = CreateReporter(server, replayRetainedHistory: false);
        DatasetUpdateReportingPlugin<Demodto, CqsReporter<Demodto>> plugin = CreatePlugin(storage, reporter);

        await plugin.HandleNewDeltas(CancellationToken.None);
        await AddDelta(storage, 1);
        await plugin.HandleNewDeltas(CancellationToken.None);

        server.Service.Requests.Should().ContainSingle().Which.Notifications.Should().ContainSingle()
            .Which.DtoId.Should().Be("entity/1");
        (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(1ul);
    }

    [Theory]
    [InlineData(0ul)]
    [InlineData(1ul)]
    public async Task DeltasThatWereCleanedUpAreNotRead(ulong cursor)
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        InMemoryStorageProvider<Demodto> storage = await CreateStorage(6);
        CqsReporter<Demodto> reporter = CreateReporter(server);
        await storage.StoreProcessedVersion(reporter.ReportingIdentifier, cursor);
        await storage.DeleteOlderThan(3);

        await CreatePlugin(StorageProbe.Create(storage, out StorageProbe probe), reporter)
            .HandleNewDeltas(CancellationToken.None);

        probe.IndexReads.Should().Be(3, "deltas 1 to 3 are gone, so only 4 to 6 are read");
        server.Service.Requests.Should().ContainSingle().Which.Notifications.Select(notification => notification.DtoId)
            .Should().Equal("entity/4", "entity/5", "entity/6");
        (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(6ul);
    }

    [Fact]
    public async Task MissingDataFileInsideABatchCommitsTheEarlierDeltasAndSkipsOnlyTheBrokenOne()
    {
        InMemoryStorageProvider<Demodto> storage = await CreateStorage(4, twoEntitiesVersion: 3);
        RecordingReporter reporter = new() { MaxDeltasPerTransaction = 10 };
        IStorageProvider<Demodto> observedStorage = StorageProbe.Create(storage, out StorageProbe probe);
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
        InMemoryStorageProvider<Demodto> storage = await CreateStorage(4, tree: tree);
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
        InMemoryStorageProvider<Demodto> storage = await CreateStorage(alreadyProcessed ? 2 : 0);
        RecordingReporter reporter = new();
        if (alreadyProcessed)
        {
            await storage.StoreProcessedVersion(reporter.ReportingIdentifier, 2);
        }

        IStorageProvider<Demodto> observedStorage = StorageProbe.Create(storage, out StorageProbe probe);

        await CreatePlugin(observedStorage, reporter).HandleNewDeltas(CancellationToken.None);

        reporter.OpenedVersions.Should().BeEmpty();
        reporter.Names.Should().BeEmpty();
        probe.CursorWriteAttempts.Should().BeEmpty();
        (await storage.GetProcessedVersion(reporter.ReportingIdentifier)).Should().Be(alreadyProcessed ? 2ul : 0ul);
    }

    private static DatasetUpdateReportingPlugin<Demodto, TReporter> CreatePlugin<TReporter>(IStorageProvider<Demodto> storage,
        TReporter reporter, TimeSpan? firstRetryDelay = null) where TReporter : IBatchedDatasetUpdateReporting<Demodto> =>
        new(storage, new RetryLogger<DatasetUpdateReportingPlugin<Demodto, TReporter>>(), reporter,
            firstRetryDelay ?? FastRetry);

    private static CqsReporter<Demodto> CreateReporter(FakeCqsServer server, int transactionLimit = 1000,
        bool replayRetainedHistory = true, int maxNotificationsPerPush = 10000) =>
        new(server.Client, Options.Create(new CqsReportingConfig
        {
            Endpoint = server.Endpoint,
            MaxDeltasPerTransaction = transactionLimit,
            MaxNotificationsPerPush = maxNotificationsPerPush,
            DurabilityTimeout = TimeSpan.FromSeconds(5),
            // The deltas are stored before the plugin first runs: replay them
            ReplayRetainedHistory = replayRetainedHistory
        }), NullLogger<CqsReporter<Demodto>>.Instance);

    /// <summary>A delta with one new entity, <c>entity/{version}</c>.</summary>
    private static async Task AddDelta(InMemoryStorageProvider<Demodto> storage, ulong version)
    {
        byte[] data = new Demodto { SomeInt = (int)version }.ToByteArray();
        DeltaIndex index = new() { DeltaVersion = version };
        index.NewEntities.Add(new DeltaEntity { Name = $"entity/{version}", Position = 0, Length = data.Length });
        await storage.StoreDelta(version, index.ToByteArray(), data);
    }

    /// <summary>Uses the production in-memory provider for real index/data reads and persisted reporting cursors.</summary>
    internal static async Task<InMemoryStorageProvider<Demodto>> CreateStorage(int count, ulong? missingVersion = null,
        ulong? twoEntitiesVersion = null, bool tree = false)
    {
        InMemoryStorageProvider<Demodto> storage = new(
            NullLogger<InMemoryStorageProvider<Demodto>>.Instance, new NotificationsHub<Demodto>());
        for (ulong version = 1; version <= (ulong)count; version++)
        {
            if (version == missingVersion)
            {
                continue;
            }

            byte[] data = new Demodto { SomeInt = (int)version }.ToByteArray();
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
            notification => notification.DtoType.Should().Be("demodto.v1"));
    }

    private sealed class RecordingReporter : IBatchedDatasetUpdateReporting<Demodto>
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

        public Task<object?> OpenDeltaTransaction(ulong deltaVersion, CancellationToken cancellationToken) =>
            OpenDeltaTransaction(deltaVersion);

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
        private InMemoryStorageProvider<Demodto> _storage = null!;
        public List<ulong> CursorWriteAttempts { get; } = [];
        public Func<string, ulong, Task>? BeforeStore { get; set; }
        public Func<ulong, long, bool>? FailRead { get; set; }
        public Func<ulong, Exception?>? FailIndexRead { get; set; }
        public int DataReads { get; private set; }
        public int IndexReads { get; private set; }

        public static IStorageProvider<Demodto> Create(InMemoryStorageProvider<Demodto> storage, out StorageProbe probe)
        {
            IStorageProvider<Demodto> proxy = Create<IStorageProvider<Demodto>, StorageProbe>();
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

            if (targetMethod.Name == nameof(IStorageProvider.ReadDataFromSources))
            {
                DataReads++;
            }

            if (targetMethod.Name == nameof(IStorageProvider.ReadDeltaIndex))
            {
                IndexReads++;
                if (FailIndexRead?.Invoke((ulong)args![0]!) is { } failure)
                {
                    return Task.FromException<ReadOnlyMemory<byte>>(failure);
                }
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