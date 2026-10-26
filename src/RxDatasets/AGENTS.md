# src/RxDatasets

The core engine (NuGet `HighTechInnovators.RxDatasets`): dataset reader and writer over an `IStorageProvider<T>`, the
DI builder, background services, and the immutable in-memory index. Storage layout: [diskFormat.md](diskFormat.md);
the `.proto` in `Models/` is authoritative where the two differ.

## Files
- `DtoServerBuilder.cs` — `AddDtoServer<T>()` / `AddReadOnlyDtoServer<T>()` → `DtoServerBuilder<T>`: `With*()` flags,
  then `Build(readinessTags)` registers everything. Also `AddSingletonAlias`, `AddSingletonAsHostedService`. Read it
  first to know which classes are live in a mode.
- `DatasetWriter.cs` — `DatasetWriter<T>` (`IDatasetWriter<T>`): `Put`, `PutMany`, `PutAll`, `PutAllChildren`,
  `PutOrDeleteMany`, `Delete`, `DeleteMany`, `DeleteChildren`; alias rebuild on start (`RebuildAliases`); write
  coalescing (`EnqueueAndAwait`, `DrainAndFlush`, `MergeIntents`); hook `protected virtual OnDeltaApplied`.
- `DatasetReader.cs` — `DatasetReader<T>` (`IDatasetReader<T>`, `IAliasIndex`, `IHostedService`): `Get`, `BatchGet`,
  `List`, `CountChildren`, `GetChangedNames`, `WaitChangedNames`, `GetHistory`, `StreamHistory`, `TailHistory`,
  `GetHistoric`, `ResolveAlias`, `ListAliases`, `CheckHealthAsync`, `CheckReadinessAsync`.
- `DatasetReaderWriter.cs` — `DatasetReaderWriter<T>`: the writer plus `IDatasetReader<T>` over the writer's own index
  (`WithSharedReaderWriter()`); wakes waiters in `OnDeltaApplied`.
- `AliasStaging.cs` — internal `AliasStaging`: claims/releases aliases op by op, throws `AliasConflictException`,
  `Mark`/`Rollback`, `AttachTo(delta)`; public `AliasMetrics` counters (meter `RxDatasets.Aliases`).
- `DatasetHelper.cs` — builds an `ImmutableDatasetState` from storage (`GetLatestIndex`, `GetIndexOfVersion`,
  `GetLatestSnapshotIndex`, `GetChangedDeltas`); static object naming (`GetObjectNamePrefix`, `GetObjectName`,
  `GetObjectFolder`), `FindParentOfEntity`, implicit-delete sets, `ComputeCrc32`, `WriteProtobufLengthPrefix`, temp
  files (env var `TEMP_FILE_DIRECTORY`).
- `DatasetExtensions.cs` — `ApplyDelta(s)`, `GetLatestChangeType`, `GetLatestEntityVersion`, `ConvertToTree`,
  `FindChange`, `Enumerate{New,Changed,Deleted}Entities`, `EnumerateHistoryEntries`, `SnapshotIndex.EnumerateEntities`.
  Read delta and snapshot indexes through these, so both the tree and the legacy flat format are covered.
- `IndexExtensions.cs` — `ValidateAgainstVersion` for `DeltaIndex`/`SnapshotIndex`: the current or legacy version
  field must equal the requested version.
- `DatasetHealthCheck.cs` — `DatasetHealthCheck`, `DatasetReadinessCheck`, `ReadOnlyDataset{Health,Readiness}Check`,
  `DistributedLockHealthCheck`; they delegate to the reader/writer checks.
- `GrpcReaderServiceImpl.cs` — `GrpcReaderServiceImpl<TEntity>`: backing logic for a gRPC reader service (parse to
  `TEntity`, `RpcException` mapping, paging with page tokens in a `MemoryCache`). `aliasName` picks the keyed
  `IDatasetReader<T>` that `RxDatasets.Aliasing` registers. Reads root config keys `PageSize`, `MaxPageSize`,
  `PaginationCacheDuration`. The builder does not register it.
- `PaginationHelper.cs` — `Paginate`: slices a page and hands the remainder to a store callback under a GUID token.
- `DataMigrator.cs` — `DataMigrator<T>` (`IDataMigrator`): `MigrateDataset("historysupport")` backfills
  `previous_version`, timestamps and `deleted_entities` in stored delta indexes.
- `SemaphoreSlimExtensions.cs`, `ActionDisposable.cs` — `using (await semaphore.LockAsync())`; release-once disposable.
- `SharedMethods.cs` — `ExtractNumber(key)`: the version from `.../{n}/file`, for the directory-listing fallback.
- `diskFormat.md` — storage format specification; partly stale (folder `snapshot/` is `snapshots/` in code, field
  names predate the `.proto`).
- `RxDatasets.csproj` — compiles both `.proto` files; `InternalsVisibleTo` RxDatasets.Tests, RxDatasets.StorageTests,
  RxDatasets.InMemoryDataset.
- `RxDatasets.v3.ncrunchproject` — NCrunch settings that include the `.proto` files.

## Folders
- `BackgroundServices/` — snapshot creation, cleaning, distributed lock, the consumer loop, clocks. [AGENTS.md](BackgroundServices/AGENTS.md)
- `Configs/` — options classes bound from configuration. [AGENTS.md](Configs/AGENTS.md)
- `GenericNotifierLogic/` — the in-process notifications hub. [AGENTS.md](GenericNotifierLogic/AGENTS.md)
- `Models/` — `.proto` messages, the immutable index tree, name ↔ tree mapping. [AGENTS.md](Models/AGENTS.md)
- `Notifications/` — notification records and who sends/receives them. [AGENTS.md](Notifications/AGENTS.md)
- `StorageProviders/` — delta/snapshot storage over `IDiskStorage<T>`, caches, in-memory provider. [AGENTS.md](StorageProviders/AGENTS.md)

## Gotchas
- **Three copies of the read path.** `ReadEntityBytes` lives in `DatasetReader`, `DatasetReaderWriter` and
  `DatasetWriter`; `GenerateChangedNames` and the history methods in the first two. Change all copies together.
- **Startup is notification-driven.** In read/write mode the writer, the reader and `DatasetReaderWriter` are not hosted
  services: they initialise on `StorageProviderReadinessNotifcation(Ready)` and rely on `NotificationsHub` replaying the
  last notification to late subscribers. Only the read-only `DatasetReader` is hosted (`StartAsync`, then `Refresh`
  every 5 s, which rebuilds the whole index from storage). The read/write `DatasetReader` is built with
  `retryInitialization = false`: a failed init stays failed until the next `Ready`.
- **No read-your-writes with separate reader and writer.** The reader catches up on `DatasetUpdatedNotification`,
  which is sent from `StoreDelta` before the writer swaps its index and handled asynchronously.
- **Delta assembly order.** `AliasStaging.AttachTo` matches entities by `Name` in the flat lists, and `ConvertToTree`
  empties those names, so AttachTo runs first. `StoreDelta` runs before `_index` is swapped.
- **CRC32 0 means "unknown (legacy)".** An empty payload also hashes to 0, so `PutMany`/`PutAll`/coalesced writes
  store it again every time, while `Put` compares bytes and skips it.
- **Invalid names differ per path.** `Put` throws; `PutAll`/`PutMany`/`PutOrDeleteMany` skip them with a warning,
  and `PutAll` then implicitly deletes an existing entity of that name; with coalescing, `PutMany` throws.
  `PutAllChildren` skips names that have no parent.
- **Data file shape.** `PutEntitiesAsync` appends one zero byte after the entity data (reason unverified); a
  deletes-only delta has no `data.pbs`.
- **Options are per host, not per dataset.** `IOptions<StorageConfig>` is shared by every `AddDtoServer<T>`, so a
  configured `DtoName` puts all datasets in one host under the same prefix (unverified at runtime).
- **`WithCleanup()` needs `IScheduler`**, which only `WithSnapshot()` registers.
- `DatasetHelper.GetLatestSnapshotIndex(specificSnapshot)` overwrites the argument with the newest snapshot.
- `DataMigrator` rewrites delta `index.pb` files in place through `IDiskStorage<T>`: no lock check, no cache
  invalidation, no `DeltaInfo` update.
