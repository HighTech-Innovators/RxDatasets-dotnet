using System.ComponentModel.DataAnnotations;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Hti.RxDatasets.Cqs.Ingress.V1;
using Hti.RxDatasets.Cqs.V1;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;

namespace RxDatasets.Reporting.Cqs;

public class CqsReporter<T> : IBatchedDatasetUpdateReporting<T> where T : class, IMessage<T>, new()
{
    private static readonly string DtoType = new T().Descriptor.FullName;
    private readonly CqsIngressPushService.CqsIngressPushServiceClient _client;
    private readonly TimeSpan _durabilityTimeout;
    private readonly int _maxNotificationsPerPush;

    public CqsReporter(CqsIngressPushService.CqsIngressPushServiceClient client, IOptions<CqsReportingConfig> config)
    {
        CqsReportingConfig settings = config.Value;
        Validator.ValidateObject(settings, new ValidationContext(settings), true);
        _client = client;
        _durabilityTimeout = settings.DurabilityTimeout;
        _maxNotificationsPerPush = settings.MaxNotificationsPerPush;
        MaxDeltasPerTransaction = settings.MaxDeltasPerTransaction;
    }

    public int MaxDeltasPerTransaction { get; }

    public string ReportingIdentifier => "cqs-entity-writer";

    public Task<object?> OpenDeltaTransaction(ulong deltaVersion) => Task.FromResult<object?>(new Transaction());

    public Task OnNewDto(string name, byte[] newProtoBytes, string? trackingId, DateTimeOffset? slaTimestamp,
        object? optionalTransaction) => Append(name, trackingId, slaTimestamp, optionalTransaction);

    public Task OnChangedDto(string name, byte[] newProtoBytes, string? trackingId, DateTimeOffset? slaTimestamp,
        object? optionalTransaction) => Append(name, trackingId, slaTimestamp, optionalTransaction);

    public Task OnDeletedDto(string name, string? trackingId, DateTimeOffset? slaTimestamp,
        object? optionalTransaction) => Append(name, trackingId, slaTimestamp, optionalTransaction);

    /// <summary>
    /// An alias was gained or lost. It is reported under its alias name, with the alias value as the id: a queue
    /// subscribed to <c>type:alias</c> is told which alias changed and reads it through the alias reader. Gaining and
    /// losing are the same notification; the consumer finds out which by reading. The entity's own change is a
    /// separate notification under the empty alias name.
    /// </summary>
    public Task OnAliasChanged(string aliasName, string alias, string entityName, ChangeType change, string? trackingId,
        DateTimeOffset? slaTimestamp, object? optionalTransaction) =>
        Append(alias, trackingId, slaTimestamp, optionalTransaction, aliasName);

    private async Task Append(string name, string? trackingId, DateTimeOffset? slaTimestamp, object? state,
        string aliasName = "")
    {
        Transaction transaction = GetActiveTransaction(state);
        transaction.Request.Notifications.Add(new DtoChangeNotification
        {
            DtoType = DtoType,
            DtoId = name,
            AliasName = aliasName,
            TrackingId = trackingId ?? "",
            SlaTimestamp = slaTimestamp.HasValue ? Timestamp.FromDateTimeOffset(slaTimestamp.Value) : null
        });

        if (transaction.Request.Notifications.Count >= _maxNotificationsPerPush)
        {
            await Push(transaction);
        }
    }

    public async Task CommitDeltaTransaction(object state)
    {
        Transaction transaction = GetActiveTransaction(state);
        await Push(transaction);
        transaction.Completed = true;
    }

    public Task RollBackDeltaTransaction(object state)
    {
        if (state is not Transaction transaction)
        {
            throw new ArgumentException("A CQS reporting transaction is required.", nameof(state));
        }

        transaction.Request.Notifications.Clear();
        transaction.Completed = true;
        return Task.CompletedTask;
    }

    private async Task Push(Transaction transaction)
    {
        if (transaction.Request.Notifications.Count == 0)
        {
            return;
        }

        try
        {
            using Grpc.Core.AsyncUnaryCall<PushBatchResponse> call = _client.PushBatchAsync(transaction.Request,
                deadline: DateTime.UtcNow.Add(_durabilityTimeout));
            PushBatchResponse response = await call.ResponseAsync;
            if (!response.Durable)
            {
                throw new InvalidOperationException("CQS did not durably persist the notification batch.");
            }

            transaction.Request.Notifications.Clear();
        }
        catch
        {
            transaction.Failed = true;
            throw;
        }
    }

    private static Transaction GetActiveTransaction(object? state)
    {
        if (state is not Transaction transaction)
        {
            throw new ArgumentException("A CQS reporting transaction is required.", nameof(state));
        }

        if (transaction.Completed || transaction.Failed)
        {
            throw new InvalidOperationException("The CQS reporting transaction has completed or failed.");
        }

        return transaction;
    }

    private sealed class Transaction
    {
        public PushBatchRequest Request { get; } = new() { FireAndForget = false, DurabilityTimeoutMs = 0 };
        public bool Failed { get; set; }
        public bool Completed { get; set; }
    }
}