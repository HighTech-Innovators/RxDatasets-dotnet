using System.Diagnostics;

namespace RxDatasets.Reporting.Cqs.Tests;

public sealed class CqsEndToEndFactAttribute : FactAttribute
{
    public CqsEndToEndFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("CQS_TEST_IMAGE")) &&
            string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("CQS_TEST_EXECUTABLE")))
        {
            Skip = "Real CQS required: set CQS_TEST_IMAGE (Docker image) or CQS_TEST_EXECUTABLE (cqs-server binary).";
        }
    }
}

public sealed class CqsEndToEndTests(ITestOutputHelper output)
{
    [CqsEndToEndFact]
    public async Task RealDtosRouteToSeparateQueuesAndResumeWithoutLossAfterServerOutage()
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(160));
        await using RealCqsHost server = new();
        try
        {
            await server.Start(timeout.Token);
            using GrpcChannel channel = GrpcChannel.ForAddress(server.Endpoint);
            CqsIngressPushService.CqsIngressPushServiceClient ingress = new(channel);
            DtoChangeQueueService.DtoChangeQueueServiceClient consumer = new(channel);
            InMemoryStorageProvider<Demodto> first = CreateStorage<Demodto>();
            InMemoryStorageProvider<Demodto2> second = CreateStorage<Demodto2>();
            CqsReporter<Demodto> firstReporter = CreateReporter<Demodto>(ingress, server.Endpoint);
            CqsReporter<Demodto2> secondReporter = CreateReporter<Demodto2>(ingress, server.Endpoint);
            FailureLogger<BatchingDatasetUpdateReportingPlugin<Demodto, CqsReporter<Demodto>>> firstLog = new();
            FailureLogger<BatchingDatasetUpdateReportingPlugin<Demodto2, CqsReporter<Demodto2>>> secondLog = new();
            BatchingDatasetUpdateReportingPlugin<Demodto, CqsReporter<Demodto>> firstPlugin = new(first, firstLog, firstReporter);
            BatchingDatasetUpdateReportingPlugin<Demodto2, CqsReporter<Demodto2>> secondPlugin = new(second, secondLog, secondReporter);

            await Store(first, 1, new Demodto { Name = "demo/before", SomeString = "first payload" });
            await Store(second, 1, new Demodto2 { Name = "demo2/before", SomeInt = 42 });
            await Task.WhenAll(firstPlugin.HandleNewDeltas(timeout.Token), secondPlugin.HandleNewDeltas(timeout.Token));
            (await first.GetProcessedVersion(firstReporter.ReportingIdentifier)).Should().Be(1ul);
            (await second.GetProcessedVersion(secondReporter.ReportingIdentifier)).Should().Be(1ul);
            output.WriteLine("Initial durable pushes: Demodto cursor=1, Demodto2 cursor=1.");

            await server.Stop(timeout.Token);
            await Store(first, 2, new Demodto { Name = "demo/during", SomeString = "queued during outage" });
            await Store(second, 2, new Demodto2 { Name = "demo2/during", SomeInt = 84 });
            Task processing = Task.WhenAll(firstPlugin.HandleNewDeltas(timeout.Token), secondPlugin.HandleNewDeltas(timeout.Token));
            try
            {
                await Task.WhenAll(firstLog.Failed.Task, secondLog.Failed.Task).WaitAsync(timeout.Token);
                processing.IsCompleted.Should().BeFalse();
                (await first.GetProcessedVersion(firstReporter.ReportingIdentifier)).Should().Be(1ul);
                (await second.GetProcessedVersion(secondReporter.ReportingIdentifier)).Should().Be(1ul);
                output.WriteLine("CQS unavailable: both real push RPCs failed; both cursors remain at 1.");

                await server.Start(timeout.Token);
                await processing.WaitAsync(timeout.Token);
            }
            finally
            {
                if (!processing.IsCompleted)
                {
                    await timeout.CancelAsync();
                    try { await processing; }
                    catch (OperationCanceledException) { }
                }
            }

            (await first.GetProcessedVersion(firstReporter.ReportingIdentifier)).Should().Be(2ul);
            (await second.GetProcessedVersion(secondReporter.ReportingIdentifier)).Should().Be(2ul);
            output.WriteLine("Restarted with original storage: both cursors advanced to 2.");
            await VerifyQueue(consumer, "demodto", Demodto.Descriptor.FullName,
                ["demo/before", "demo/during"], timeout.Token);
            await VerifyQueue(consumer, "demodto2", Demodto2.Descriptor.FullName,
                ["demo2/before", "demo2/during"], timeout.Token);
        }
        finally
        {
            output.WriteLine($"Elapsed: {elapsed.Elapsed.TotalSeconds:F1}s. Server log:\n{server.Log}");
        }
    }

    private async Task VerifyQueue(DtoChangeQueueService.DtoChangeQueueServiceClient consumer,
        string queue, string dtoType, string[] expected, CancellationToken cancellation)
    {
        List<DtoChangeNotification> received = [];
        while (received.Count < expected.Length)
        {
            BatchDequeueResponse batch = await consumer.BatchDequeueAsync(new BatchDequeueRequest
            {
                QueueName = queue, MaxNotifications = 100, PrefixDepth = 0, WaitTimeMilliseconds = 200,
                WorkerId = "cqs-end-to-end"
            }, deadline: DateTime.UtcNow.AddSeconds(5), cancellationToken: cancellation);
            batch.Notifications.Should().NotBeEmpty();
            received.AddRange(batch.Notifications);
            BatchAcknowledgeRequest acknowledge = new() { QueueName = queue };
            acknowledge.AckTokens.AddRange(batch.Notifications.Select(notification => notification.AckToken));
            BatchAcknowledgeResponse acknowledged = await consumer.BatchAcknowledgeAsync(acknowledge,
                deadline: DateTime.UtcNow.AddSeconds(5), cancellationToken: cancellation);
            acknowledged.AcknowledgedCount.Should().Be(batch.Notifications.Count);
        }

        received.Should().AllSatisfy(notification =>
        {
            notification.DtoType.Should().Be(dtoType);
            notification.UrnCase.Should().Be(DtoChangeNotification.UrnOneofCase.DtoId);
            notification.AckToken.Should().NotBeEmpty();
        });
        received.Select(notification => notification.DtoId).Should().BeEquivalentTo(expected);
        BatchDequeueResponse empty = await consumer.BatchDequeueAsync(new BatchDequeueRequest
        {
            QueueName = queue, MaxNotifications = 100, WaitTimeMilliseconds = 100
        }, deadline: DateTime.UtcNow.AddSeconds(5), cancellationToken: cancellation);
        empty.Notifications.Should().BeEmpty();
        output.WriteLine($"Queue {queue}: {dtoType} => {string.Join(", ", received.Select(item => item.DtoId))}; acknowledged; empty.");
    }

    private static InMemoryStorageProvider<TDto> CreateStorage<TDto>() where TDto : class, IMessage<TDto>, new() =>
        new(NullLogger<InMemoryStorageProvider<TDto>>.Instance, new NotificationsHub<TDto>());

    private static CqsReporter<TDto> CreateReporter<TDto>(CqsIngressPushService.CqsIngressPushServiceClient client,
        string endpoint) where TDto : class, IMessage<TDto>, new() => new(client, Options.Create(new CqsReportingConfig
        {
            Endpoint = endpoint, DurabilityTimeout = TimeSpan.FromSeconds(3)
        }));

    private static async Task Store<TDto>(InMemoryStorageProvider<TDto> storage, ulong version, TDto dto)
        where TDto : class, IMessage<TDto>, new()
    {
        string name = (string)dto.Descriptor.FindFieldByName("name").Accessor.GetValue(dto);
        byte[] data = dto.ToByteArray();
        DeltaIndex index = new() { DeltaVersion = version };
        index.NewEntities.Add(new DeltaEntity { Name = name, Position = 0, Length = data.Length });
        await storage.StoreDelta(version, index.ToByteArray(), data);
    }

    private sealed class FailureLogger<TCategory> : ILogger<TCategory>
    {
        public TaskCompletionSource Failed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Warning && exception is Grpc.Core.RpcException)
            {
                Failed.TrySetResult();
            }
        }
    }
}