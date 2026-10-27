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
DTO's type (its protobuf full name), its name, the SLA and the tracking id, never the DTO bytes.

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
  split into calls of at most `MaxNotificationsPerPush` (default 10 000). `WithCqsReporting` registers
  `BatchingDatasetUpdateReportingPlugin` for this; every other reporter keeps the standard
  `DatasetUpdateReportingPlugin` and its one delta per transaction, unchanged.
- `DurabilityTimeout` (default 35 s) must outlast CQS's wait for its next persistence cycle (twice
  `persistence.interval`, 30 s by default). Raise it if you raise that interval.
- CQS needs a queue that routes each type's full name, for example `hightechinnovators.dtos.demodto.v1.Demodto`.
  See `config/cqs-reporter.toml` for a CQS config with two queues, and the CQS repository's
  `docs/operations.md` for running the service. CQS has no authentication: use it on a trusted network.
- Alias changes are reported too: each alias an entity gains or loses (also when it is deleted, and after a
  rebuild of the alias definitions) is a notification with that `alias_name` and the alias value as `dto_id`, so a CQS
  queue subscribed to `type:alias` is told which alias changed and reads it through the alias reader. The entity's own
  change is a separate notification with an empty `alias_name`. The reporter reads both the index tree a writer stores
  today and the legacy flat lists of older deltas.
