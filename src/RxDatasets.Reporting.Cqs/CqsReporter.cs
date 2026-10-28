using System.Globalization;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Hti.RxDatasets.Cqs.Ingress.V1;
using Hti.RxDatasets.Cqs.V1;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;

namespace RxDatasets.Reporting.Cqs;

public class CqsReporter<T> : IBatchedDatasetUpdateReporting<T> where T : class, IMessage<T>, new()
{
    // How much longer the client waits than CQS holds the push, so CQS's durable = false always arrives first
    private static readonly TimeSpan DeadlineMargin = TimeSpan.FromSeconds(5);
    // A push is sent once its notifications reach this size, whatever their count: gRPC's default limit on a message
    // is 4 MiB on both sides, and long ids or tracking ids make 10 000 notifications larger than that.
    private const int MaxPushBytes = 1024 * 1024;
    private static readonly int NotificationTagSize =
        CodedOutputStream.ComputeTagSize(PushBatchRequest.NotificationsFieldNumber);
    // The SLA CQS gives a notification without one, counted from when it is added: used only to pick, between two
    // notifications for the same entity in one transaction, the one CQS would serve first
    private static readonly TimeSpan DefaultSlaDelay = TimeSpan.FromHours(1);
    // Answers that a retry of the same push cannot change: the reporter logs them as errors before the plugin retries
    private static readonly HashSet<Grpc.Core.StatusCode> PermanentStatusCodes =
    [
        Grpc.Core.StatusCode.InvalidArgument, Grpc.Core.StatusCode.OutOfRange, Grpc.Core.StatusCode.ResourceExhausted,
        Grpc.Core.StatusCode.Unimplemented, Grpc.Core.StatusCode.PermissionDenied, Grpc.Core.StatusCode.Unauthenticated
    ];
    private readonly string _dtoType = ToDtoType(typeof(T));
    private readonly CqsIngressPushService.CqsIngressPushServiceClient _client;
    private readonly ILogger<CqsReporter<T>> _logger;
    private readonly int _durabilityTimeoutMs;
    private readonly TimeSpan _deadline;
    private readonly int _maxNotificationsPerPush;

    /// <exception cref="ArgumentException"><typeparamref name="T"/> is not named <c>&lt;Namespace&gt;.LinkV1.Link</c>.
    /// </exception>
    public CqsReporter(CqsIngressPushService.CqsIngressPushServiceClient client, IOptions<CqsReportingConfig> config,
        ILogger<CqsReporter<T>> logger)
    {
        // Validated at startup by AddValidatedConfigSection
        CqsReportingConfig settings = config.Value;
        _client = client;
        _logger = logger;
        // Rounded up: a fraction of a millisecond must not become 0, which means the server's default
        _durabilityTimeoutMs = (int)Math.Ceiling(settings.DurabilityTimeout.TotalMilliseconds);
        _deadline = TimeSpan.FromMilliseconds(_durabilityTimeoutMs) + DeadlineMargin;
        _maxNotificationsPerPush = settings.MaxNotificationsPerPush;
        MaxDeltasPerTransaction = settings.MaxDeltasPerTransaction;
        SkipsRetainedHistory = !settings.ReplayRetainedHistory;
        ReportingIdentifier = settings.ReportingIdentifier;
    }

    public int MaxDeltasPerTransaction { get; }

    /// <summary>The opposite of <see cref="CqsReportingConfig.ReplayRetainedHistory"/>: true by default.</summary>
    public bool SkipsRetainedHistory { get; }

    /// <summary><see cref="CqsReportingConfig.ReportingIdentifier"/>, <c>cqs-reporter</c> by default.</summary>
    public string ReportingIdentifier { get; }

    /// <summary>True: a notification carries only the entity's name, so the plugin does not read the data.</summary>
    public bool SkipsDtoBytes => true;

    public Task<object?> OpenDeltaTransaction(ulong deltaVersion) => OpenDeltaTransaction(deltaVersion, CancellationToken.None);

    /// <summary>
    /// <paramref name="cancellationToken"/> cancels the transaction's pushes: a shutdown does not wait up to
    /// <see cref="CqsReportingConfig.DurabilityTimeout"/> and the deadline margin for CQS's durable answer. A cancelled
    /// push throws <see cref="OperationCanceledException"/>; the cursor is not written, so the batch is pushed again
    /// after a restart, which CQS deduplicates.
    /// </summary>
    public Task<object?> OpenDeltaTransaction(ulong deltaVersion, CancellationToken cancellationToken) =>
        Task.FromResult<object?>(new Transaction(_durabilityTimeoutMs, cancellationToken));

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
        DtoChangeNotification notification = new()
        {
            DtoType = _dtoType,
            DtoId = name,
            AliasName = aliasName,
            TrackingId = trackingId ?? "",
            SlaTimestamp = ToSla(slaTimestamp)
        };
        DateTimeOffset sla = notification.SlaTimestamp?.ToDateTimeOffset() ?? DateTimeOffset.UtcNow + DefaultSlaDelay;
        // Counted per notification: Request.CalculateSize() would measure every notification again on each add
        int size = NotificationTagSize + CodedOutputStream.ComputeMessageSize(notification);

        // An entity changed many times in one transaction is sent once, with the earliest SLA and that change's tracking
        // id, which is what CQS keeps of duplicates anyway. Only within the notifications not pushed yet.
        (string, string) key = (name, aliasName);
        if (transaction.Pending.TryGetValue(key, out (int Position, DateTimeOffset Sla) pending))
        {
            if (sla < pending.Sla)
            {
                DtoChangeNotification replaced = transaction.Request.Notifications[pending.Position];
                transaction.Request.Notifications[pending.Position] = notification;
                transaction.NotificationBytes += size - NotificationTagSize - CodedOutputStream.ComputeMessageSize(replaced);
                transaction.Pending[key] = (pending.Position, sla);
            }

            return;
        }

        transaction.Pending[key] = (transaction.Request.Notifications.Count, sla);
        transaction.Request.Notifications.Add(notification);
        transaction.NotificationBytes += size;

        if (transaction.Request.Notifications.Count >= _maxNotificationsPerPush ||
            transaction.NotificationBytes >= MaxPushBytes)
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

        transaction.Clear();
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
                deadline: DateTime.UtcNow.Add(_deadline), cancellationToken: transaction.CancellationToken);
            PushBatchResponse response = await call.ResponseAsync;
            if (!response.Durable)
            {
                throw new InvalidOperationException("CQS did not durably persist the notification batch.");
            }

            int sent = transaction.Request.Notifications.Count;
            if (response.AcceptedCount == 0)
            {
                // Every notification was dropped: CQS has no queue for this dto_type (or for these aliases)
                _logger.LogInformation("CQS routed none of {Sent} {DtoType} notifications to a queue", sent, _dtoType);
            }
            else
            {
                _logger.LogDebug("CQS routed {Accepted} of {Sent} {DtoType} notifications to a queue",
                    response.AcceptedCount, sent, _dtoType);
            }

            transaction.Clear();
        }
        catch (Grpc.Core.RpcException e) when (PermanentStatusCodes.Contains(e.StatusCode))
        {
            transaction.Failed = true;
            _logger.LogError(e, "CQS refused a push of {Count} notifications ({Bytes} bytes) with {StatusCode}. Retrying the " +
                "same push cannot help: reporting is stuck at this delta until the cause is fixed",
                transaction.Request.Notifications.Count, transaction.NotificationBytes, e.StatusCode);
            throw;
        }
        catch (Grpc.Core.RpcException e) when (e.StatusCode == Grpc.Core.StatusCode.Cancelled &&
                                               transaction.CancellationToken.IsCancellationRequested)
        {
            transaction.Failed = true;
            throw new OperationCanceledException("The push to CQS was cancelled.", e, transaction.CancellationToken);
        }
        catch
        {
            transaction.Failed = true;
            throw;
        }
    }

    /// <summary>
    /// The CQS dto_type: the DTO name and major version in lowercase, <c>link.v1</c> for <c>&lt;Namespace&gt;.LinkV1.Link</c>.
    /// Queues subscribe with this name. It is read from the CLR type, not the message descriptor, because a raw DTO
    /// type (one that only carries bytes) has no descriptor.
    /// </summary>
    private static string ToDtoType(System.Type type)
    {
        string ns = type.Namespace ?? "";
        string segment = ns[(ns.LastIndexOf('.') + 1)..];
        string prefix = type.Name + "V";
        return segment.StartsWith(prefix, StringComparison.Ordinal) &&
               int.TryParse(segment.AsSpan(prefix.Length), NumberStyles.None, CultureInfo.InvariantCulture, out int version)
            ? $"{type.Name.ToLowerInvariant()}.v{version}"
            : throw new ArgumentException(
                $"{type.FullName} has no CQS dto_type: expected a type named <Namespace>.{type.Name}V<version>.{type.Name}",
                nameof(type));
    }

    /// <summary>
    /// The SLA to send. One at or before the epoch (a zero timestamp, <c>default(DateTimeOffset)</c>) is sent as no SLA:
    /// CQS gives an absent SLA the default of now + 1 hour, but keeps an explicit one, which would put the notification
    /// at the head of every queue.
    /// </summary>
    private static Timestamp? ToSla(DateTimeOffset? slaTimestamp)
    {
        if (slaTimestamp is null)
        {
            return null;
        }

        Timestamp sla = Timestamp.FromDateTimeOffset(slaTimestamp.Value);
        return sla.Seconds > 0 ? sla : null;
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

    private sealed class Transaction(int durabilityTimeoutMs, CancellationToken cancellationToken)
    {
        public CancellationToken CancellationToken { get; } = cancellationToken;
        public PushBatchRequest Request { get; } = new() { FireAndForget = false, DurabilityTimeoutMs = durabilityTimeoutMs };
        // The size of Request's notifications, each with its field tag and length
        public int NotificationBytes { get; set; }
        // Each of Request's notifications by entity and alias name, with its position and the SLA used to compare them
        public Dictionary<(string DtoId, string AliasName), (int Position, DateTimeOffset Sla)> Pending { get; } = [];
        public bool Failed { get; set; }
        public bool Completed { get; set; }

        public void Clear()
        {
            Request.Notifications.Clear();
            Pending.Clear();
            NotificationBytes = 0;
        }
    }
}