# DataVlow and RxDatasets: user guide

This guide is for programmers who build a server on RxDatasets, or who build services that share data through
DataVlow. It goes from the ideas to the code to running it in production.

**DataVlow** is a way for services to share data. A producer writes typed records to a DataVlow server. Consumers
learn about changes through a change queue and read the current records whenever they need them. No service calls
another service's API to get data.

**RxDatasets** is the .NET library under a DataVlow server. It stores a dataset of protobuf records in object storage
(local disk, Google Cloud Storage, S3, Azure Blob or MinIO) as numbered deltas and snapshots. It keeps an in-memory
index and serves reads, writes, change feeds and history. Its distributed lock lets several instances share one
dataset safely.

You can use RxDatasets in three ways:

| Way | What you write | Chapter |
|---|---|---|
| A hand-written server | A proto per record type, generated gRPC services, and `AddDtoServer<T>()` per type | [Building a server](04-building-a-server.md) |
| A server generated from a template | One template that expands per record type, into one process or one container per type | [Real-world servers](09-real-world-servers.md) |
| The generic DataVlow server | Nothing: drop a `.proto` file in a folder and the server serves it | [The DataVlow server](10-datavlow-server.md) |

## Chapters

1. [Concepts](01-concepts.md): records, ids and scopes, aliases, ownership, the change flow, and versioning
   record types.
2. [Service contracts](02-contracts.md): the reader, writer and history services, paging, write semantics, SLA
   timestamps and tracking ids.
3. [Change queues and consumers](03-change-queues.md): queues, notifications, priorities, error handling, batch
   consumers, `ReEnqueue` and `SelfEnqueue`, and large files.
4. [Building a server](04-building-a-server.md): a dataset from proto to running gRPC service, step by step.
5. [Configuration reference](05-configuration.md): every configuration section and key, with defaults.
6. [Storage providers](06-storage-providers.md): local disk, Google Cloud Storage, S3, Azure Blob, MinIO, in-memory,
   the cache, and writing your own.
7. [Locking, replicas and deployment](07-locking-and-deployment.md): when to use which lock, read-only replicas,
   health probes and deployment patterns.
8. [Inside a dataset](08-dataset-lifecycle.md): versions, deltas, snapshots, cleaning, reporters, watchers,
   aliases and migrations.
9. [Real-world servers](09-real-world-servers.md): how two production server families use RxDatasets, and what to
   copy from them.
10. [The DataVlow server](10-datavlow-server.md): the generic server that serves record types from raw protos at
    runtime.
11. [Testing](11-testing.md): in-memory datasets, storage emulators and end-to-end tests.
12. [Pitfalls and known quirks](12-pitfalls.md): the things that bite, collected in one list.

Related documents:

- [Distributed lock and fenced writes](../distributed-lock.md): how the lock works, with a timeline for every
  scenario.
- [Disk format](../../src/RxDatasets/diskFormat.md): the files a dataset stores.
- [Aliasing](../../src/RxDatasets.Aliasing/README.md): alias registration and the pattern language.

## Where to start

- **You build services that use DataVlow:** read chapters 1, 2 and 3.
- **You build or run a server:** read chapters 4 to 8, then 12.
- **You want a server without writing one:** read chapter 10.

## Terms

| Term | Meaning |
|---|---|
| record | One protobuf message stored under a string id. Older code and protos call it a DTO (data transfer object). |
| record type | A protobuf message definition with an id pattern and a major version, for example `order.v2`. |
| dataset | All records of one record type, stored by RxDatasets in one folder. |
| version | The dataset version: a counter that goes up by one with every write that changes something. |
| delta | The changes of one version, stored as files. |
| snapshot | The full state of a dataset at one version, stored as files. |
| producer, owner | The one service that writes a record type. |
| consumer | A service that reads a record type and reacts to its changes. |
| change queue service, CQS | Keeps one queue per consuming service and delivers change notifications to it. |
