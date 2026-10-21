using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Hti.RxDatasets.Cqs.Ingress.V1;
using Hti.RxDatasets.Cqs.V1;
using Microsoft.Extensions.Options;
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
        CqsReporter<Timestamp> reporter = CreateReporter(server);
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
        request.DurabilityTimeoutMs.Should().Be(0);
        DtoChangeNotification notification = request.Notifications.Should().ContainSingle().Which;
        notification.DtoType.Should().Be(Timestamp.Descriptor.FullName);
        notification.DtoId.Should().Be("tenant/entity");
        notification.UrnCase.Should().Be(DtoChangeNotification.UrnOneofCase.DtoId);
        notification.TrackingId.Should().Be(tracking ?? "");
        notification.SlaTimestamp.Should().Be(sla.HasValue ? Timestamp.FromDateTimeOffset(sla.Value) : null);
        notification.AliasName.Should().Be("");
        notification.AckToken.Should().Be("");
        (DateTime deadline, DateTime received) = server.Service.Deadlines.Should().ContainSingle().Which;
        (deadline - received).TotalSeconds.Should().BeInRange(30, 36);
        reporter.MaxDeltasPerTransaction.Should().Be(1000);
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
        CqsReporter<Timestamp> reporter = CreateReporter(server, maxNotifications: 2);
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
        CqsReporter<Timestamp> reporter = CreateReporter(server);
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
        CqsReporter<Timestamp> reporter = CreateReporter(server, maxNotifications: 2);
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
        CqsReporter<Timestamp> reporter = CreateReporter(server);
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
        CqsReporter<Timestamp> reporter = CreateReporter(server, maxNotifications: 1, timeout: TimeSpan.FromSeconds(4));
        object transaction = (await reporter.OpenDeltaTransaction(1))!;
        await reporter.OnDeletedDto("first", null, null, transaction);
        await Task.Delay(100);
        await reporter.OnDeletedDto("second", null, null, transaction);
        await reporter.CommitDeltaTransaction(transaction);
        (DateTime Deadline, DateTime Received)[] deadlines = server.Service.Deadlines.ToArray();
        deadlines.Length.Should().Be(2);
        deadlines[1].Deadline.Should().BeAfter(deadlines[0].Deadline.AddMilliseconds(50));
        deadlines.Should().AllSatisfy(value => (value.Deadline - value.Received).TotalSeconds.Should().BeInRange(2, 5));
    }

    [Fact]
    public async Task RpcDeadlineIsEnforcedAndTransactionCanBeRolledBack()
    {
        await using FakeCqsServer server = await FakeCqsServer.Start();
        server.Service.Handler = async (request, context) =>
        {
            await Task.Delay(Timeout.Infinite, context.CancellationToken);
            return new PushBatchResponse { Durable = true };
        };
        CqsReporter<Timestamp> reporter = CreateReporter(server, timeout: TimeSpan.FromMilliseconds(200));
        object transaction = (await reporter.OpenDeltaTransaction(1))!;
        await reporter.OnDeletedDto("entity", null, null, transaction);
        RpcException exception = (await FluentActions.Invoking(() => reporter.CommitDeltaTransaction(transaction))
            .Should().ThrowAsync<RpcException>()).Which;
        exception.StatusCode.Should().Be(StatusCode.DeadlineExceeded);
        await reporter.RollBackDeltaTransaction(transaction);
    }

    private static CqsReporter<Timestamp> CreateReporter(FakeCqsServer server, int maxNotifications = 10000,
        TimeSpan? timeout = null) => new(server.Client, Options.Create(new CqsReportingConfig
        {
            Endpoint = server.Endpoint,
            MaxNotificationsPerPush = maxNotifications,
            DurabilityTimeout = timeout ?? TimeSpan.FromSeconds(35)
        }));

    private static Task Add(CqsReporter<Timestamp> reporter, string kind, string name, string? tracking,
        DateTimeOffset? sla, object transaction) => kind switch
        {
            "new" => reporter.OnNewDto(name, [255], tracking, sla, transaction),
            "changed" => reporter.OnChangedDto(name, [255], tracking, sla, transaction),
            "deleted" => reporter.OnDeletedDto(name, tracking, sla, transaction),
            _ => throw new ArgumentOutOfRangeException(nameof(kind))
        };
}