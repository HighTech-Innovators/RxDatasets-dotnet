using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Hightechinnovators.Dtos.DemodtoV1;
using Hti.RxDatasets.Cqs.Ingress.V1;
using Hti.RxDatasets.Cqs.V1;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using Xunit;

namespace RxDatasets.Reporting.Cqs.Tests;

public class CqsReporterTests
{
    [Theory]
    [InlineData("new", true)]
    [InlineData("changed", true)]
    [InlineData("deleted", true)]
    [InlineData("new", false)]
    [InlineData("changed", false)]
    [InlineData("deleted", false)]
    public async Task MapsEveryChangeKindWithoutReadingDtoBytes(string kind, bool includeMetadata)
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        CqsReporter<Demodto> reporter = CreateReporter(server);
        object transaction = (await reporter.OpenDeltaTransaction(42))!;
        DateTimeOffset? sla = includeMetadata
            ? new DateTimeOffset(2026, 10, 20, 13, 14, 15, TimeSpan.FromHours(2)).AddTicks(1234)
            : null;
        string? tracking = includeMetadata ? "tracking-42" : null;

        await Add(reporter, kind, "tenant/entity", tracking, sla, transaction);
        server.Service.Requests.Should().BeEmpty();
        await reporter.CommitDeltaTransaction(transaction);

        PushBatchRequest request = server.Service.Requests.Should().ContainSingle().Which;
        request.FireAndForget.Should().BeFalse();
        request.DurabilityTimeoutMs.Should().Be(35000, "CQS holds the push for the configured timeout");
        DtoChangeNotification notification = request.Notifications.Should().ContainSingle().Which;
        notification.DtoType.Should().Be("demodto.v1");
        notification.DtoId.Should().Be("tenant/entity");
        notification.UrnCase.Should().Be(DtoChangeNotification.UrnOneofCase.DtoId);
        notification.TrackingId.Should().Be(tracking ?? "");
        notification.SlaTimestamp.Should().Be(sla.HasValue ? Timestamp.FromDateTimeOffset(sla.Value) : null);
        notification.AliasName.Should().Be("");
        notification.AckToken.Should().Be("");
        (DateTime deadline, DateTime received) = server.Service.Deadlines.Should().ContainSingle().Which;
        (deadline - received).TotalSeconds.Should().BeInRange(38, 40, "the client waits 5 s longer than CQS");
        reporter.MaxDeltasPerTransaction.Should().Be(1000);
    }

    [Theory]
    [InlineData(-1L, false)]
    [InlineData(0L, false)]
    [InlineData(500L, false)]
    [InlineData(1000L, true)]
    public async Task SendsAnSlaAtOrBeforeTheEpochAsNoSla(long millisecondsAfterEpoch, bool kept)
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        CqsReporter<Demodto> reporter = CreateReporter(server);
        object transaction = (await reporter.OpenDeltaTransaction(1))!;
        DateTimeOffset sla = DateTimeOffset.UnixEpoch.AddMilliseconds(millisecondsAfterEpoch);

        await reporter.OnChangedDto("tenant/entity", [255], null, sla, transaction);
        await reporter.OnChangedDto("tenant/default", [255], null, default(DateTimeOffset), transaction);
        await reporter.CommitDeltaTransaction(transaction);

        PushBatchRequest request = server.Service.Requests.Should().ContainSingle().Which;
        request.Notifications[0].SlaTimestamp.Should().Be(kept ? Timestamp.FromDateTimeOffset(sla) : null,
            "CQS defaults only an absent SLA, so a zero or pre-epoch one would jump every queue");
        request.Notifications[1].SlaTimestamp.Should().BeNull();
    }

    [Fact]
    public async Task AnEntityChangedSeveralTimesInATransactionIsSentOnceWithTheEarliestSla()
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        CqsReporter<Demodto> reporter = CreateReporter(server);
        object transaction = (await reporter.OpenDeltaTransaction(1))!;
        DateTimeOffset noon = new(2026, 10, 28, 12, 0, 0, TimeSpan.Zero);

        await reporter.OnChangedDto("t/a", [], "late", noon.AddMinutes(5), transaction);
        await reporter.OnChangedDto("t/a", [], "early", noon.AddMinutes(1), transaction);
        await reporter.OnDeletedDto("t/a", "later again", noon.AddMinutes(3), transaction);
        await reporter.OnAliasChanged("by_value", "t/a", "t/x", ChangeType.New, "alias", noon, transaction);
        await reporter.OnNewDto("t/b", [], "first", null, transaction);
        await reporter.OnChangedDto("t/b", [], "second", null, transaction);
        await reporter.CommitDeltaTransaction(transaction);

        // The alias notification has its own identity (its alias name); an unset SLA counts as CQS's default, now + 1 h.
        server.Service.Requests.Should().ContainSingle().Which.Notifications
            .Select(n => (n.DtoId, n.AliasName, n.TrackingId, n.SlaTimestamp?.ToDateTimeOffset()))
            .Should().Equal(
                ("t/a", "", "early", noon.AddMinutes(1)),
                ("t/a", "by_value", "alias", noon),
                ("t/b", "", "first", (DateTimeOffset?)null));
    }

    [Fact]
    public async Task NotificationsAlreadyPushedAreNotDeduplicatedAgainstLaterOnes()
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        CqsReporter<Demodto> reporter = CreateReporter(server, maxNotifications: 1);
        object transaction = (await reporter.OpenDeltaTransaction(1))!;

        await reporter.OnChangedDto("t/a", [], null, null, transaction);
        await reporter.OnChangedDto("t/a", [], null, null, transaction);
        await reporter.CommitDeltaTransaction(transaction);

        server.Service.Requests.Should().HaveCount(2)
            .And.AllSatisfy(request => request.Notifications.Should().ContainSingle().Which.DtoId.Should().Be("t/a"));
    }

    [Theory]
    [InlineData(0, true)]
    [InlineData(1, false)]
    [InlineData(2, false)]
    public async Task APushNoQueueAcceptedIsLoggedAsInformation(int accepted, bool logged)
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        server.Service.Handler = (request, context) =>
            Task.FromResult(new PushBatchResponse { Durable = true, AcceptedCount = accepted });
        RecordingLogger logger = new();
        CqsReporter<Demodto> reporter = CreateReporter(server, logger: logger);
        object transaction = (await reporter.OpenDeltaTransaction(1))!;

        await reporter.OnChangedDto("t/a", [], null, null, transaction);
        await reporter.OnChangedDto("t/b", [], null, null, transaction);
        await reporter.CommitDeltaTransaction(transaction);

        logger.Entries.Where(entry => entry.Level >= LogLevel.Information).Select(entry => entry.Message).Should()
            .Equal(logged ? ["CQS routed none of 2 demodto.v1 notifications to a queue"] : []);
        logger.Entries.Count(entry => entry.Level == LogLevel.Debug).Should().Be(logged ? 0 : 1, "otherwise it is Debug");
    }

    [Theory]
    [InlineData(null, "cqs-reporter")]
    [InlineData("cqs-migration", "cqs-migration")]
    public async Task TheCursorNameIsConfigurable(string? configured, string expected)
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();

        CreateReporter(server, reportingIdentifier: configured).ReportingIdentifier.Should().Be(expected);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    public async Task FlushesAtCapAndCommitsOnlyRemainder(int count)
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        CqsReporter<Demodto> reporter = CreateReporter(server, maxNotifications: 2);
        object transaction = (await reporter.OpenDeltaTransaction(1))!;
        for (int index = 0; index < count; index++)
        {
            await reporter.OnNewDto($"entity/{index}", [255], null, null, transaction);
            server.Service.Requests.Count.Should().Be((index + 1) / 2);
        }

        await reporter.CommitDeltaTransaction(transaction);

        server.Service.Requests.Count.Should().Be((count + 1) / 2);
        server.Service.Requests.Where(request =>
            request.Notifications.Count < 1 || request.Notifications.Count > 2).Should().BeEmpty();
        server.Service.Requests.SelectMany(request => request.Notifications).Select(notification => notification.DtoId)
            .Should().Equal(Enumerable.Range(0, count).Select(index => $"entity/{index}"));
    }

    [Fact]
    public async Task RollbackDiscardsPendingChangesAndEmptyRetryMakesNoRpc()
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        CqsReporter<Demodto> reporter = CreateReporter(server);
        object discarded = (await reporter.OpenDeltaTransaction(1))!;
        await reporter.OnDeletedDto("discarded", null, null, discarded);
        await reporter.RollBackDeltaTransaction(discarded);
        await FluentActions.Invoking(() => reporter.CommitDeltaTransaction(discarded))
            .Should().ThrowAsync<InvalidOperationException>();
        object empty = (await reporter.OpenDeltaTransaction(1))!;
        await reporter.CommitDeltaTransaction(empty);
        server.Service.Requests.Should().BeEmpty();
    }

    [Theory]
    [InlineData(false, 1)]
    [InlineData(true, 1)]
    [InlineData(false, 2)]
    [InlineData(true, 2)]
    public async Task FailedPushRequiresRollbackAndWholeTransactionCanBeRetried(bool rpcError, int failedPush)
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        int attempts = 0;
        server.Service.Handler = (request, context) =>
        {
            if (++attempts == failedPush)
            {
                if (rpcError)
                {
                    throw new RpcException(new Status(StatusCode.Unavailable, "temporarily unavailable"));
                }

                return Task.FromResult(new PushBatchResponse { Durable = false });
            }

            return Task.FromResult(new PushBatchResponse { Durable = true });
        };
        CqsReporter<Demodto> reporter = CreateReporter(server, maxNotifications: 2);
        object transaction = (await reporter.OpenDeltaTransaction(1))!;

        async Task Replay(object state)
        {
            await reporter.OnNewDto("first", [255], null, null, state);
            await reporter.OnChangedDto("second", [255], null, null, state);
            await reporter.OnDeletedDto("third", null, null, state);
            await reporter.CommitDeltaTransaction(state);
        }

        if (rpcError)
        {
            RpcException exception = (await FluentActions.Invoking(() => Replay(transaction))
                .Should().ThrowAsync<RpcException>()).Which;
            exception.StatusCode.Should().Be(StatusCode.Unavailable);
        }
        else
        {
            await FluentActions.Invoking(() => Replay(transaction)).Should().ThrowAsync<InvalidOperationException>();
        }

        await FluentActions.Invoking(() => reporter.CommitDeltaTransaction(transaction))
            .Should().ThrowAsync<InvalidOperationException>();
        await reporter.RollBackDeltaTransaction(transaction);
        object retry = (await reporter.OpenDeltaTransaction(1))!;
        await Replay(retry);

        PushBatchRequest[] requests = server.Service.Requests.ToArray();
        requests.Length.Should().Be(failedPush + 2);
        requests.Skip(failedPush).SelectMany(request => request.Notifications).Select(notification => notification.DtoId)
            .Should().Equal("first", "second", "third");
        requests[failedPush].Should().Be(requests[0]);
    }

    [Fact]
    public async Task CommitWaitsForDurableResponse()
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        TaskCompletionSource arrived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<PushBatchResponse> response = new(TaskCreationOptions.RunContinuationsAsynchronously);
        server.Service.Handler = (request, context) =>
        {
            arrived.SetResult();
            return response.Task;
        };
        CqsReporter<Demodto> reporter = CreateReporter(server);
        object transaction = (await reporter.OpenDeltaTransaction(1))!;
        await reporter.OnDeletedDto("entity", null, null, transaction);
        Task commit = reporter.CommitDeltaTransaction(transaction);
        try
        {
            await arrived.Task.WaitAsync(TimeSpan.FromSeconds(5));
            commit.IsCompleted.Should().BeFalse();
        }
        finally
        {
            response.TrySetResult(new PushBatchResponse { Durable = true });
        }

        await commit;
    }

    [Fact]
    public async Task ConfiguredDeadlineIsFreshForEachRpc()
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        CqsReporter<Demodto> reporter = CreateReporter(server, maxNotifications: 1, timeout: TimeSpan.FromSeconds(4));
        object transaction = (await reporter.OpenDeltaTransaction(1))!;
        await reporter.OnDeletedDto("first", null, null, transaction);
        await Task.Delay(100);
        await reporter.OnDeletedDto("second", null, null, transaction);
        await reporter.CommitDeltaTransaction(transaction);
        (DateTime Deadline, DateTime Received)[] deadlines = server.Service.Deadlines.ToArray();
        deadlines.Length.Should().Be(2);
        deadlines[1].Deadline.Should().BeAfter(deadlines[0].Deadline.AddMilliseconds(50));
        deadlines.Should().AllSatisfy(value => (value.Deadline - value.Received).TotalSeconds.Should().BeInRange(7, 9));
        server.Service.Requests.Should().AllSatisfy(request => request.DurabilityTimeoutMs.Should().Be(4000));
    }

    [Fact]
    public async Task AFractionOfAMillisecondIsSentRoundedUpNotAsTheServerDefault()
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        CqsReporter<Demodto> reporter = CreateReporter(server, timeout: TimeSpan.FromTicks(1));
        object transaction = (await reporter.OpenDeltaTransaction(1))!;
        await reporter.OnDeletedDto("entity", null, null, transaction);
        await reporter.CommitDeltaTransaction(transaction);
        server.Service.Requests.Should().ContainSingle().Which.DurabilityTimeoutMs.Should().Be(1);
    }

    [Fact]
    public async Task RpcDeadlineIsEnforcedAndTransactionCanBeRolledBack()
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        // A server that ignores durability_timeout_ms: the deadline, 5 s past the timeout, still ends the call
        server.Service.Handler = async (request, context) =>
        {
            await Task.Delay(Timeout.Infinite, context.CancellationToken);
            return new PushBatchResponse { Durable = true };
        };
        CqsReporter<Demodto> reporter = CreateReporter(server, timeout: TimeSpan.FromMilliseconds(200));
        object transaction = (await reporter.OpenDeltaTransaction(1))!;
        await reporter.OnDeletedDto("entity", null, null, transaction);
        RpcException exception = (await FluentActions.Invoking(() => reporter.CommitDeltaTransaction(transaction))
            .Should().ThrowAsync<RpcException>()).Which;
        exception.StatusCode.Should().Be(StatusCode.DeadlineExceeded);
        await reporter.RollBackDeltaTransaction(transaction);
    }

    [Fact]
    public async Task PushesAreAlsoCappedBySizeSoLongTrackingIdsStayUnderTheMessageLimit()
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        CqsReporter<Demodto> reporter = CreateReporter(server);
        string tracking = new('t', 4000);
        object transaction = (await reporter.OpenDeltaTransaction(1))!;
        for (int i = 0; i < 600; i++)
        {
            await reporter.OnDeletedDto($"entity/{i}", tracking, null, transaction);
        }

        await reporter.CommitDeltaTransaction(transaction);

        const int megabyte = 1024 * 1024;
        PushBatchRequest[] requests = server.Service.Requests.ToArray();
        requests.Should().HaveCountGreaterThan(1);
        requests.SelectMany(request => request.Notifications).Select(notification => notification.DtoId)
            .Should().Equal(Enumerable.Range(0, 600).Select(i => $"entity/{i}"));
        // One notification with its tag and length, plus the request's other fields
        int oneNotification = requests[0].Notifications[0].CalculateSize() + 16;
        requests.Should().AllSatisfy(request => request.CalculateSize().Should().BeLessThan(megabyte + oneNotification));
        requests[..^1].Should().AllSatisfy(request => request.CalculateSize().Should().BeGreaterThanOrEqualTo(megabyte));
    }

    [Theory]
    [InlineData(StatusCode.ResourceExhausted, true)]
    [InlineData(StatusCode.InvalidArgument, true)]
    [InlineData(StatusCode.Unavailable, false)]
    public async Task APushThatRetryingCannotFixIsLoggedAsAnError(StatusCode status, bool permanent)
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        server.Service.Handler = (request, context) => throw new RpcException(new Status(status, "refused"));
        RecordingLogger logger = new();
        CqsReporter<Demodto> reporter = CreateReporter(server, logger: logger);
        object transaction = (await reporter.OpenDeltaTransaction(1))!;
        await reporter.OnDeletedDto("entity", null, null, transaction);

        (await FluentActions.Invoking(() => reporter.CommitDeltaTransaction(transaction))
            .Should().ThrowAsync<RpcException>()).Which.StatusCode.Should().Be(status);

        if (permanent)
        {
            (LogLevel level, string message) = logger.Entries.Should().ContainSingle().Which;
            level.Should().Be(LogLevel.Error);
            message.Should().Contain(status.ToString());
        }
        else
        {
            logger.Entries.Should().BeEmpty("the plugin logs a transient failure as a warning when it retries");
        }
    }

    [Fact]
    public async Task RejectsADtoTypeWithoutANameAndVersionNamespace()
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        FluentActions.Invoking(() => new CqsReporter<Timestamp>(server.Client,
                Options.Create(new CqsReportingConfig { Endpoint = server.Endpoint }),
                NullLogger<CqsReporter<Timestamp>>.Instance))
            .Should().Throw<ArgumentException>()
            .WithMessage("Google.Protobuf.WellKnownTypes.Timestamp has no CQS dto_type*TimestampV<version>.Timestamp*");
    }

    private static CqsReporter<Demodto> CreateReporter(FakeCqsServer server, int maxNotifications = 10000,
        TimeSpan? timeout = null, ILogger<CqsReporter<Demodto>>? logger = null, string? reportingIdentifier = null) =>
        new(server.Client, Options.Create(new CqsReportingConfig
        {
            Endpoint = server.Endpoint,
            MaxNotificationsPerPush = maxNotifications,
            DurabilityTimeout = timeout ?? TimeSpan.FromSeconds(35),
            ReportingIdentifier = reportingIdentifier ?? "cqs-reporter"
        }), logger ?? NullLogger<CqsReporter<Demodto>>.Instance);

    private sealed class RecordingLogger : ILogger<CqsReporter<Demodto>>
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => Entries.Add((logLevel, formatter(state, exception)));
    }

    private static Task Add(CqsReporter<Demodto> reporter, string kind, string name, string? tracking,
        DateTimeOffset? sla, object transaction) => kind switch
        {
            "new" => reporter.OnNewDto(name, [255], tracking, sla, transaction),
            "changed" => reporter.OnChangedDto(name, [255], tracking, sla, transaction),
            "deleted" => reporter.OnDeletedDto(name, tracking, sla, transaction),
            _ => throw new ArgumentOutOfRangeException(nameof(kind))
        };
}