# 12. Pitfalls and known quirks

The things that bite, in one list. Each links to where it is explained.

## Startup and readiness

- **The dataset only loads when the reader or writer is first resolved** (by a request or a readiness probe).
  Resolve both after `Build()`, before `app.Run()`, so loading starts at once.
  ([Building a server](04-building-a-server.md#why-the-reader-and-writer-are-resolved-by-hand))
- **Until it has loaded, every call throws `InvalidOperationException`.** Map it to `UNAVAILABLE`, and make clients
  retry. The same happens on a standby, during a handover and while stopping.
- **Dataset checks in liveness kill instances that are only loading or waiting.** The untagged
  `DatasetHealthCheck` is unhealthy while a dataset loads. Let liveness check only that the process answers. ([Health checks](04-building-a-server.md#health-checks))
- **Don't start a dataset with a timeout token.** RxDatasets keeps the start token for later work.
- **Building datasets in parallel isn't safe.** Configuration validation uses a shared set that isn't thread-safe.
  Build service collections one at a time.
- **A writer that can't initialize retries forever**, every 5 seconds, and can't be stopped by disposing it. One known
  cause: stored data has aliases, and the host registers no alias definitions.

## Configuration

- **Sections are per service collection, not per dataset.** `Storage:DtoName` in a multi-type host puts every dataset
  in the same folder. `WithWriteCoalescing()` turns coalescing on for every dataset in the collection.
  ([Configuration](05-configuration.md#how-sections-work))
- **Quotes in docker compose's list form become part of the value**, and validation rejects
  `Storage__CustomRootFolder="data/"`.
- **`Storage:CustomRootFolder` must end in `/`.**
- **Some keys in older configurations do nothing**: the compression keys, `Storage:LogStorageAccess`,
  `Storage:ReadOnlyMode`, `StartupDelaySeconds`. ([Keys that do nothing](05-configuration.md#keys-that-do-nothing))

## Writing

- **Name validation differs per method.** With `Writer:AllowedNamesPattern`, `Put` rejects a bad name, but `PutMany`,
  `PutAll` and `PutOrDeleteMany` skip it with only a warning (unless write coalescing is on).
- **`PutAll` deletes everything you don't send**, for every tenant. Refuse it on tenant-scoped types.
- **`DeleteChildren` needs whole id pairs** (`t/acme/s/site01`, not `t/acme/s`), and keeps the ancestor itself.
- **Records that serialize differently each time** (protobuf `map`, unsorted repeated fields) are a change on every
  write, and wake every consumer.
- **An alias can't move in one step.** Free it in an earlier request, or earlier in the same `PutOrDeleteMany`.

## Reading and change feeds

- **Page tokens live in one instance's memory** for 60 seconds. Behind a load balancer, a token can land on another
  replica and fail with `FAILED_PRECONDITION`.
- **Read-only replicas lag the writer by 5 to 10 seconds.**
- **`BatchRead` leaves missing records out.** Compare with the request to find deletes.
- **Stored versions belong to one dataset.** A dataset that is recreated (an in-memory server that restarts, a wiped
  bucket) starts at version 1 again, and every consumer's cursor is wrong.
- **History stops where cleaning deleted deltas.** `StreamHistory` stops at the gap, `TailHistory` skips it.

## Snapshots and cleaning

- **`WithCleanup()` needs `WithSnapshot()`**, or the host fails to start.
- **`WithCleanup()` deletes nothing until a reporter has stored its progress.** A retired reporter, or one whose
  identifier changed, blocks cleaning for good with its old entry in `ProcessedVersions.pb`.
  ([Cleaning](08-dataset-lifecycle.md#cleaning))
- **Without `WithSnapshot()`, every start replays every delta.**
- **Don't put `WithSnapshot()` or `WithCleanup()` on a read-only builder.** Nothing stops you, but they need a writer.

## Reporters

- **At least once.** A delta can be reported again after a failure or a crash. Make the receiving side idempotent.
- **Throw when you couldn't deliver**, or the change is lost. A delta that always fails blocks the reporter forever.
- **One process per reporter identifier per folder.** Two report deltas twice or skip them. Use the lock.
- **Register reporters after storage**, so they stop before it.

## Locking and storage

- **Lock whenever two read/write processes can see the same storage**, including the overlap of a rolling deployment.
  ([Locking](07-locking-and-deployment.md))
- **A standby serves no reads.** Scale reads with read-only replicas, not with more read/write instances.
- **A crashed holder blocks takeover until its lock expires**, up to `DistributedLock:RenewalInterval`.
- **Keep clocks synchronized.**
- **With the shared lock and a host that stops services one by one, register the lock first.** Otherwise every
  shutdown waits for the full shutdown timeout.
- **MinIO through `WithMinioStorage()` doesn't fence deltas of 5 MiB or more.** Use `WithS3Storage()` against MinIO.
  ([MinIO](06-storage-providers.md#minio))
- **An S3-compatible store must support conditional writes** for the lock and the fenced commit.
- **Google Cloud Storage creates a missing bucket in a fixed location.** Create buckets yourself.
- **Local disk has no health check**, and its compare-and-swap is only atomic on one machine.

## Known quirks in the library

These are known and harmless, but confusing when you read the code:

- Some public names are misspelled: `DistirbutedLockStatus.Acquried`, `StorageProviderReadinessNotifcation`.
- Health check names differ: dataset checks use `Order-V2` style names (from the namespace, `V1` included), storage
  checks the bare type name.
- `WithGoogleCloudStorage()` and `WithS3Storage()` don't replace an `IDiskStorage<T>` you registered before them
  (`TryAdd`); the other providers do (`Add`).
- `RxDatasets.DatasetWatcher.DI.AddDatasetWatcher<T>(services)` and
  `RxDatasets.InMemoryDataset.DI.ConfigureServices<T>(services)` are static methods, not extension methods.
- There are two different "in memory" options: `WithInMemoryDataset()` on the builder and the `InMemoryDataset<T>`
  package. ([Testing](11-testing.md))
- The only metrics are the two alias-rebuild counters; there is no tracing yet.
