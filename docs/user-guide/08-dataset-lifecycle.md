# 8. Inside a dataset

This chapter explains what RxDatasets does with a dataset over its life: how it stores and loads it, writes
snapshots, cleans up, tells reporters about changes, and changes alias definitions. The file format itself is in the
[disk format](../../src/RxDatasets/diskFormat.md).

## Versions, deltas and snapshots

- Every write that changes something is stored as a **delta**, numbered with the next **version**:
  `deltas/{version}/index.pb` (the names, positions and metadata) and `deltas/{version}/data.pbs` (the record bytes,
  one after the other; no data file for a delta that only deletes). The index file is written last; a delta counts
  only once its index exists.
- Every delta stores, per record, the version that changed it before (`previous_version`), its tracking id and SLA,
  and the aliases it gained and lost.
- A **snapshot**, `snapshots/{version}/index.pb` and `data.pbs`, holds the full state at a version. Deleted records
  stay in it as tombstones, so change feeds keep working after old deltas are cleaned.
- Next to them: `DeltaInfo.pb` (a hint at the newest delta), `SnapshotInfo.pb` (the snapshots and when they were
  written) and `ProcessedVersions.pb` (how far each reporter got).

At startup, the reader and writer load the newest snapshot and replay every delta after it into an in-memory index.
The index holds names, versions, positions and aliases. Record bytes are read on demand, by byte range, through the
cache. So memory grows with the number of records, not their size, and startup time grows with the number of deltas
since the last snapshot.

Writes go through one writer per dataset, one at a time (or merged, with write coalescing). A write returns after its
delta is stored and the in-memory index is updated.

## Snapshots

`WithSnapshot()` runs a background consumer after every version. It writes a snapshot when either:

- the newest version is `SnapshotCreation:SnapshotInterval` (default 100) or more past the last snapshot, or
- the last snapshot is older than `SnapshotCreation:PeriodInHours` (default 24) and something changed since.

A snapshot is built by streaming the previous snapshot and the newer deltas into a temporary file, then uploading it.
Only the lock holder writes snapshots.

Without `WithSnapshot()`, no snapshot is ever written and every start replays every delta since version 1. That is
fine for a test server and slow for anything that lives long.

## Cleaning

`WithCleanup()` deletes what is no longer needed. It keeps the newest `Cleanup:MinimumSnapshotsToKeep` snapshots,
and every snapshot younger than `Cleanup:MinimumAgeToLiveInMinutes`. It deletes older snapshots and, with
`Cleanup:KeepAllDeltas = false`, also every delta up to and including the oldest snapshot it keeps. It runs when the
dataset becomes ready, after every snapshot and, when deltas are deleted, after every reporter's progress.

**Cleaning never deletes what a reporter still needs, and that has a catch.** It deletes nothing while the lowest
version stored in `ProcessedVersions.pb` is below the oldest snapshot it would keep. When no reporter has ever stored
a version, that lowest version is 0, so:

- **`WithCleanup()` without any reporter deletes nothing.**
- A reporter that you removed from the host, or whose `ReportingIdentifier` you changed, leaves its old entry in
  `ProcessedVersions.pb`. That entry never moves again, and **blocks cleaning for good**. Remove the entry when you
  retire a reporter.

`WithCleanup()` also needs `WithSnapshot()`: only the snapshot option registers the scheduler cleaning runs on, and
without snapshots there is nothing to clean anyway.

What deleting deltas costs: history (`GetHistory`, `StreamHistory`, `TailHistory`, `ReadHistoric`) only reaches back
to the oldest delta that is left. Change feeds keep working (they use the versions in the index), and so do
reporters that are up to date. Keep all deltas when history matters; storage is cheap.

## Reporters

A reporter is told about every change of a dataset, in version order, after the change is stored. It is the way to
feed a change queue, a message bus, a search index or an audit log.

```csharp
public interface IDatasetUpdateReporting<T>
{
    string ReportingIdentifier { get; }

    Task OnNewDto(string name, byte[] newProtoBytes, string? trackingId, DateTimeOffset? slaTimestamp, object? optionalTransaction);
    Task OnChangedDto(string name, byte[] newProtoBytes, string? trackingId, DateTimeOffset? slaTimestamp, object? optionalTransaction);
    Task OnDeletedDto(string name, string? trackingId, DateTimeOffset? slaTimestamp, object? optionalTransaction);

    // Optional: an alias was gained (New) or lost (Deleted) by an entity.
    Task OnAliasChanged(string aliasName, string alias, string entityName, ChangeType change, string? trackingId,
        DateTimeOffset? slaTimestamp, object? optionalTransaction) => Task.CompletedTask;

    // Optional: one transaction per delta.
    Task<object?> OpenDeltaTransaction(ulong deltaVersion) => Task.FromResult<object?>(null);
    Task CommitDeltaTransaction(object state) => Task.CompletedTask;
    Task RollBackDeltaTransaction(object state) => Task.CompletedTask;
}
```

### How it is called

- Deltas are reported **one at a time, in version order**: first the new records, then the changed ones, then the
  deleted ones, then alias changes. When `OpenDeltaTransaction` returns a state, `CommitDeltaTransaction` follows
  after the delta, or `RollBackDeltaTransaction` after a failure.
- After each delta, the reporter's progress is stored in `ProcessedVersions.pb` under its `ReportingIdentifier`. A
  reporter that restarts, or one on a new lock holder, continues after the last stored version.
- **A method that throws makes the whole delta retry**, every 5 seconds, until it succeeds. Delivery is therefore
  **at least once**: a delta can be reported again after a crash or a failure halfway through. Make the receiving side
  idempotent (a change queue merges duplicates, a bus consumer re-reads the record).
- A delta that keeps failing blocks the reporter forever, and everything after it. Log well, and alert on a reporter
  that falls behind.
- Reporting runs only on the lock holder.
- Changing `ReportingIdentifier` starts that reporter again from version 1, and leaves the old entry behind (see
  [Cleaning](#cleaning)).
- Two processes that run a reporter with the same identifier against the same folder report deltas twice or skip
  them. The lock prevents that; without a lock, run one instance.

### Registering a reporter

There is no builder method for your own reporter. Register three things, after the dataset:

```csharp
services.AddSingleton<BusReporter<Order>>();
services.AddSingleton<DatasetUpdateReportingPlugin<Order, BusReporter<Order>>>();
services.AddHostedService<DatasetUpdateConsumingService<Order, DatasetUpdateReportingPlugin<Order, BusReporter<Order>>>>();
```

Registered after storage, its hosted service starts after storage and stops before it.

A builder extension makes it one call, and validates the reporter's configuration at startup:

```csharp
public static class BusReportingExtensions
{
    public static DtoServerBuilder<T> WithBusReporting<T>(this DtoServerBuilder<T> builder)
        where T : class, IMessage<T>
    {
        builder.Services.AddValidatedConfigSection<BusReportingConfig>();   // fails fast on a missing topic
        builder.Services.AddSingleton<BusReporter<T>>();
        builder.Services.AddSingleton<DatasetUpdateReportingPlugin<T, BusReporter<T>>>();
        builder.Services.AddHostedService<DatasetUpdateConsumingService<T, DatasetUpdateReportingPlugin<T, BusReporter<T>>>>();
        return builder;
    }
}
```

### Example: one message per change

```csharp
public class BusReporter<T>(IProducer<string, byte[]> producer, BusReportingConfig config) : IDatasetUpdateReporting<T>
{
    public string ReportingIdentifier => "bus-reporter";

    public Task OnNewDto(string name, byte[] bytes, string? trackingId, DateTimeOffset? sla, object? tx) =>
        Send(name, ChangeReport.Created(name, bytes, trackingId, sla));

    public Task OnChangedDto(string name, byte[] bytes, string? trackingId, DateTimeOffset? sla, object? tx) =>
        Send(name, ChangeReport.Changed(name, bytes, trackingId, sla));

    public Task OnDeletedDto(string name, string? trackingId, DateTimeOffset? sla, object? tx) =>
        Send(name, ChangeReport.Deleted(name, trackingId, sla));

    private async Task Send(string name, ChangeReport report)
    {
        // The record name as key keeps every record's changes in order on a partitioned topic.
        var result = await producer.ProduceAsync(config.Topic,
            new Message<string, byte[]> { Key = name, Value = report.ToByteArray() });
        if (result.Status != PersistenceStatus.Persisted)
            throw new InvalidOperationException($"Report for {name} not persisted: {result.Status}");  // retried
    }
}
```

The two things that matter: **throw when the message isn't safely stored**, so the delta is retried instead of
silently lost; and **use the record name as the message key**, so a partitioned topic keeps each record's changes in
order. For throughput, collect a delta's messages in `OpenDeltaTransaction` and send them in
`CommitDeltaTransaction`.

### Feeding a change queue

A change queue reporter enqueues `(type, name, sla, tracking id)` per change, and implements `OnAliasChanged` so
queues subscribed through an alias are told when the mapping changes. That is all a reporter needs to turn a dataset
into the [change flow](01-concepts.md#the-change-flow). The DataVlow server's in-memory queue plugin is a complete
example (`ChangeQueueReporter<T>`).

### JSON files

`WithJsonReporting()` (package `RxDatasets.Reporting`) is a ready-made reporter that writes every change as a JSON
file into the same storage: `{Prefix}{created|changed|deleted}/{name}.json`, optionally with the version in the path
or file name (`JsonOutput:IncludeVersionInPath`, `JsonOutput:IncludeVersionInFileName`). A delete writes `{}`. Useful
as a simple export for systems that read files.

### Other consumers

Reporters are built on a more general hook: `IDatasetUpdateConsumer<T>` (`HandleNewDeltas`, `ConsumerIdentifier`),
run by `DatasetUpdateConsumingService<T, TConsumer>` once the dataset is ready and after every new version. The
snapshot writer is one. Use it for work that isn't per record, and store your own progress.

Inside the host, `INotificationsHub<T>` publishes `DatasetUpdatedNotification(version)`,
`SnapshotCreatedNotification`, storage readiness and lock changes. It replays the last notification of each kind to a
new subscriber.

## Watching a dataset from storage

`IDatasetWatcher<T>` (package `RxDatasets.DatasetWatcher`) replays a dataset and then follows it, reading straight
from storage, without the reader:

```csharp
services.AddReadOnlyDtoServer<Order>().WithGoogleCloudStorage().Build();   // the storage registration
RxDatasets.DatasetWatcher.DI.AddDatasetWatcher<Order>(services);

await watcher.SubscribeAllData(new MyObserver(), ct);   // an IDatasetObserver<Order>
```

It calls the observer for every record of the latest snapshot (tombstones as deletes), then checks for new versions
every `DatasetWatcher:CheckingIntervalInSeconds` and replays each delta between `OnDatasetVersionStart` and
`OnDatasetVersionEnd`. Two limits: it returns at once, without an error, while the dataset has no snapshot yet, and it
gets stuck when it falls behind deltas that cleaning deleted.

## Aliases

Aliases are kept in the dataset's index, next to the names ([concepts](01-concepts.md#aliases)). Register the
definitions in code, from patterns:

```csharp
services.AddDatasetAliases<Order>(
    Order.Parser,
    ["t/{tenantid}/s/{siteid}/orders/{orderid}"],                       // the id patterns
    ("by_customer", "t/{tenantid}/s/{siteid}/customers/{.customer_id}/orders/{orderid}"));
```

or with a factory of your own and a hash that changes whenever the factory would give other aliases:

```csharp
services.AddDatasetAlias<Order>("by_customer",
    (name, bytes) => [AliasOf(name, Order.Parser.ParseFrom(bytes.Span))],
    definitionHash: "by_customer-v1");
```

Read through an alias with a keyed reader:

```csharp
var byCustomer = serviceProvider.GetDatasetReader<Order>("by_customer");
var (aliases, version) = await byCustomer.List("t/acme/s/site01/customers/c-7", ct);
```

`GrpcReaderServiceImpl<T>` takes an alias name on every read, so a gRPC service passes `alias_name` through.

- An alias value belongs to at most one record. A write that would give two records the same alias fails as a whole
  with `AliasConflictException`. To move an alias, free it first (delete or change the old owner) in an earlier
  request, or earlier in the same `PutOrDeleteMany`.
- **Changing the definitions** is safe: every delta and snapshot carries a hash of the definitions. A writer that
  starts with other definitions recomputes all aliases from the data before it reports ready, and stores the result
  as one delta. If that makes an alias come out on two records, the newest record keeps it; the conflicts are logged
  and counted (metric `rxdatasets.aliases.rebuild_conflicts`, meter `RxDatasets.Aliases`).
- A writer refuses to start when the stored data has aliases and the host registers no definitions at all.
- `serviceProvider.GetAliasesFor(id, record)` computes a record's aliases without writing it, to validate a request.

The full pattern language is in the [aliasing README](../../src/RxDatasets.Aliasing/README.md).

## Data migrations

`IDataMigrator` runs named, one-off migrations of the stored data. Each dataset registers one. A server exposes them
through an admin-only endpoint that runs a migration by name on every dataset and returns a report; the DataVlow
server's is `GET /migrate/{name}`. The one migration today, `historysupport`, adds the data history needs to datasets
written before history existed.

## Observability

- **Logs:** each part logs under its generic type name. Reporters log every processed delta at information level, and
  retries at warning level.
- **Metrics:** the meter `RxDatasets.Aliases`, with `rxdatasets.aliases.rebuild_conflicts` and
  `rxdatasets.aliases.rebuild_unparseable`. There are no other metrics or traces yet.
- **Health:** the checks in [Building a server](04-building-a-server.md#health-checks). A reporter that falls behind
  isn't a health check; compare `ProcessedVersions.pb` with the newest version if you need that.
