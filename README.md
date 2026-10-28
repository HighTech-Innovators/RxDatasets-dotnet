###### Sonar Statistics:
- [![Quality Gate Status](https://sonar.developer.htiprojects.nl/api/project_badges/measure?project=oss_reactive-datasets_8d09a016-e5e6-4c6a-a047-59be74e6adfc&metric=alert_status&token=sqb_9fbd6ed9645a4778a27db7fbb01df9072047af09)](https://sonar.developer.htiprojects.nl/dashboard?id=oss_reactive-datasets_8d09a016-e5e6-4c6a-a047-59be74e6adfc)

## License

  

This project is licensed under the Common Public Attribution License Version 1.0 (CPAL-1.0).

  

You may obtain a copy of the License in the [LICENSE](./LICENSE) file.

  

For more details, you can also refer to the full license text at the [SPDX website](https://spdx.org/licenses/CPAL-1.0.html).

  

## Introduction

  

Reactive Datasets is a project which provides an efficient way to managing status updates of entities in protobuf format. It persistently stores entities in a disk format (On Azure Blob, S3, or in memory). And it allows for the retrieval of entities in a reactive way, allowing clients to get a list of all changed entities since the last time they checked.

  

[A README for the diskformat used to store entities](src/RxDatasets/diskFormat.md)

[The distributed lock and fenced writes, with a timeline per scenario](docs/distributed-lock.md)

## Features

- **Entity Management**: Persistent storage and retrieval of protobuf-formatted entities
- **Reactive Updates**: Delta-based change tracking and notification system
- **Storage Flexibility**: Support for multiple backends (Local Disk, Azure Blob, S3, Google Cloud Storage, Minio)
- **Change Tracking**: Efficient tracking of additions, modifications, and deletions
- **CountChildren**: Query hierarchical entity counts by parent path (useful for testing and validation)
- **gRPC Integration**: Full gRPC service support for remote access to dataset operations

  

## Configurations

### Cleanup Config Options

- MinimumAgeToLiveInMinutes - must be at least 120 minutes (default)

- MinimumSnapshotsToKeep - must be at least 2 snapshots kept (default)

- KeepAllDeltas = true (default); // When disabled, all deltas leading to the outdated snapshot would be deleted,
e.g. lets say we have deltas [1,2,3,4,5,6,7,8,9,10] and snapshots [1,4,7, 10] (assuming a snapshot interval of 3),
when snapshot 1,4 are deleted, deltas 1,2,3,4,5,6 would also be deleted since the earliest to keep snapshot would 7

- IsEnabled = true (default);


## Reporting to CQS

`RxDatasets.Reporting.Cqs` sends a notification to the Change Queue Service (CQS) for every DTO that
changes, so consumers can pull or be pushed the most urgent changes first. The notification carries the
DTO's type (its name and major version, such as `demodto.v1`), its name, the SLA and the tracking id, never the
DTO bytes. The reporter says so with `SkipsDtoBytes = true` (a member of `IDatasetUpdateReporting`, false by default),
so the reporting plugin does not read a delta's data file for it: a delta costs one index read, whatever its size,
and a delta whose data file is missing is still reported from its index. An SLA at or before the Unix epoch (such as
`default(DateTimeOffset)`) is sent as no SLA, which CQS sets to now + 1 hour; sent as-is, it would put the notification
ahead of everything in the queue.

```csharp
services.AddDtoServer<Demodto>()
    .WithS3Storage().WithDistributedLock().WithCache().WithSnapshot().WithCleanup()
    .WithCqsReporting()
    .Build(ReadinessTags);
```

```json
"CqsReporting": {
  "Endpoint": "http://localhost:50051"
}
```

- Each transaction is one durable `PushBatch`. The reporter's processed-version cursor moves only after
  CQS answers `durable = true`, so every change reaches CQS at least once. A `durable = false` answer, a
  timeout or an unreachable CQS makes the transaction fail and retry; CQS drops the duplicates.
- A burst of waiting deltas goes out as one transaction of up to `MaxDeltasPerTransaction` (default 1000),
  split into calls of at most `MaxNotificationsPerPush` (default 10 000) and at most about 1 MiB, well under gRPC's
  default 4 MiB message limit. A push CQS refuses in a way a retry cannot fix (such as `RESOURCE_EXHAUSTED` or
  `INVALID_ARGUMENT`) is logged as an error; it is still retried, so reporting stays stuck at that delta until the
  cause is fixed. The reporting plugin, `DatasetUpdateReportingPlugin`, batches for every reporter that implements
  `IBatchedDatasetUpdateReporting` (in `RxDatasets.Abstractions`), as this one does; a reporter that does not gets
  one delta per transaction.
- Within the notifications not pushed yet, an entity changed several times is sent once, with its earliest SLA and
  that change's tracking id, which is what CQS keeps of duplicates anyway. After each push the reporter logs how many
  notifications CQS routed to a queue, at Information when it routed none (no queue takes that `dto_type`).
- `ReportingIdentifier` (default `cqs-reporter`) names the reporter's processed version in the dataset. Two CQS
  reporters on one dataset, such as an old and a new CQS during a migration, each need their own.
- Enabled on a dataset that already has deltas, reporting starts at the newest delta and reports only the changes
  after it: consumers that need the current state do their own initial sync. Set `ReplayRetainedHistory` to `true` to
  send every delta the dataset still keeps instead, from the oldest one (on a large dataset that can be millions of
  notifications). Either way it never looks for deltas that cleanup has removed; a reporter whose processed version
  is older than those logs a warning naming the deltas it missed.
- `DurabilityTimeout` (default 35 s) is sent with every push: CQS holds the push that long for its next persistence
  cycle, then answers `durable = false`. The client's own deadline is 5 s longer, so CQS answers first. It must
  outlast `persistence.interval` (15 s by default) with room to spare; raise it if you raise that interval. A
  failed transaction is retried after 5 s, doubling on each failure in a row up to 1 minute.
- Each notification's `dto_type` is the DTO name and major version in lowercase, for example `demodto.v1` for
  `Hightechinnovators.Dtos.DemodtoV1.Demodto`; CQS needs a queue that routes that name. The reporter reads it from the
  type's namespace, so a DTO type must be named `<Namespace>.<Name>V<version>.<Name>`, or the reporter cannot be
  created. See `config/cqs-reporter.toml` for a CQS config with two queues, and the CQS repository's
  `docs/operations.md` for running the service. CQS has no authentication: use it on a trusted network.
- Alias changes are reported too: each alias an entity gains or loses (also when it is deleted, and after a
  rebuild of the alias definitions) is a notification with that `alias_name` and the alias value as `dto_id`, so a CQS
  queue subscribed to `type:alias` is told which alias changed and reads it through the alias reader. The entity's own
  change is a separate notification with an empty `alias_name`. The reporter reads both the index tree a writer stores
  today and the legacy flat lists of older deltas.
- The protos in `src/RxDatasets.Reporting.Cqs/Protos` are copies of the CQS repository's `protos/`: refresh them
  when CQS changes its contract.
