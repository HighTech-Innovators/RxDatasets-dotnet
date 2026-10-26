# Terminology — datasets (project-specific)

Domain vocabulary of RxDatasets: delta/snapshot storage of protobuf
entities and its reactive change feed. This file extends
`.ai/terminology/global.md`; on a clash, this file wins for this project.

---

## Domain terms

### Datasets and storage

- **Dataset** — every entity of one protobuf type `T`, stored under the key
  prefix `{CustomRootFolder}{DtoName or typeof(T).Name}/`
  (`DatasetHelper.GetObjectNamePrefix`). One `AddDtoServer<T>()` per dataset.
  _Avoid_: table, collection (collection is a name segment, see below).
- **DTO server** — a host that serves one or more datasets over gRPC, built
  with `AddDtoServer<T>()` / `AddReadOnlyDtoServer<T>()` and the fluent
  `With…().Build()` calls (`src/RxDatasets/DtoServerBuilder.cs`). _Avoid_:
  "storage server" (the storage is the blob backend).
- **Entity** — one protobuf message stored under a name. The library handles
  its bytes; only aliases read its fields. _Avoid_: record, object (object is
  a blob in storage).
- **Disk storage** — `IDiskStorage<T>`, the raw blob API (upload, download
  ranges, list, delete) that each `RxDatasets.StorageProvider.*` project
  implements (`S3Storage`, `AzStorage`, …). _Avoid_: storage provider (the
  project names say "StorageProvider", but they implement disk storage).
- **Storage provider** — `IStorageProvider<T>`, the dataset layer on top of
  disk storage that stores and lists deltas, snapshots and metadata files
  (`ObjectStorageProvider<T>`, optionally wrapped by `CacheStorageProvider`).
  _Avoid_: backend (that is disk storage).
- **Storage reader** vs **read-only storage provider** — same members:
  `IStorageReader<T>` is the uncached `ObjectStorageReader<T>` (used by the
  dataset watcher); `IReadOnlyStorageProvider<T>` is the hosted, optionally
  cached provider of read-only mode (`IStorageProvider.cs`).
- **Delta** — the change set of one write: `deltas/{version}/index.pb` plus
  `data.pbs` (no `data.pbs` when it only deletes)
  (`ObjectStorageProvider.StoreDelta`). _Avoid_: diff, patch, changeset (that
  is the reader's `ChangeSet` result).
- **Snapshot** — the full index and data at one version, under
  `snapshots/{version}/` (`FolderNames.cs`; plural, though older docs say
  `snapshot/`). Built by the `SnapshotCreationPlugin`.
- **Index file** vs **data file** — `index.pb` (names, positions, lengths,
  CRCs, aliases) vs `data.pbs` (length-delimited entity bytes)
  (`ObjectNameIndexOrDataEnum`). Entities are read by position and length from
  the data file.
- **DeltaInfo / SnapshotInfo** — `DeltaInfo.pb` (earliest and latest delta
  version) and `SnapshotInfo.pb` (snapshot versions and timestamps): the
  metadata readers use instead of listing. Writing `DeltaInfo.pb` is the
  commit point of a delta (`ObjectStorageProvider.PersistDeltaInfo`). Legacy
  fallbacks: `VersionIndex.pb` (V2) and `latestVersion.pb` (V1)
  (`ObjectStorageReader`).
- **Tree format** vs **flat format** — the hierarchical index layout (field
  `root`) that writers emit, vs the older flat entity lists/map that readers
  still accept (`diskFormat.md`, `DatasetExtensions.ConvertToTree`). Every
  process must be tree-aware before the first tree-format write.
- **Upload verification** — re-checking the stored length after an upload:
  `VerifyAll`, `VerifyLargeDataOnly`, `DoNotVerify` (`Configs/StorageConfig.cs`).

### Versions and changes

- **Dataset version** — the monotonically increasing `ulong` of the latest
  delta; `WriteResponse.NewDatasetVersion`, and the `fromVersion` of
  `GetChangedNames` / `ListChanges`. _Avoid_: revision, sequence number.
- **Delta version** vs **snapshot version** — a snapshot carries the delta
  version it was built at, so both count on the same scale
  (`ImmutableDatasetState`). The reader's **main-index snapshot version** is
  the snapshot its index started from; its **last-used snapshot version** is a
  newer snapshot loaded later as an extra read source (`DatasetReader`).
- **Legacy version field** — proto field 1 (`*_version_legacy`), next to the
  current field 32; writers put 0 in the legacy field (`IndexExtensions.cs`).
- **Change type** — `New`, `Changed`, `Deleted` (`ChangeType.cs`). Over a
  range of versions they fold: New then Deleted is no change, Deleted then New
  is Changed (`DatasetReader.GenerateChangedNames`).
- **Live** vs **tombstone** — a deleted entity stays in the index (and in
  later snapshots) with `IsLive == false`; `List`, `CountChildren` count live
  entities only (`Models/ImmutableEntities.cs`). _Avoid_: soft delete.
- **Previous version** — the version of an entity's prior state, which chains
  its history for `GetHistory` / `GetHistoric` (`DatasetEntities.proto`).

### Names and hierarchy

- **Name** — the entity's primary key: a `/`-separated path whose leading
  segments are **collection**/**id** pairs, e.g. `t/T/s/S/items/I/values`
  (`Models/IndexKeys.cs`). Unique per dataset. _Avoid_: key, path, id (id is
  one segment; the protobuf `Id` field some DTOs have is unrelated).
- **Node / leaf / template / variable** — how a name maps onto the index
  tree: one node per collection/id pair; the leaf is stored under a template
  with a `{}` placeholder (`items/{}/values`) and its variable (`I`)
  (`IndexKeys.Split`, `diskFormat.md`).
- **Parent** vs **ancestor** — the parent is a name minus its last segment(s)
  (`DatasetHelper.FindParentOfEntity`); `List(parent)` and
  `CountChildren(parent)` return live **descendants** at any depth;
  `DeleteChildren(ancestor)` takes a prefix with an even number of segments.
  _Avoid_: "children" when you mean direct children only.
- **Implicit delete** — `PutAll` deletes every live entity it was not sent;
  `PutAllChildren` does so only under the parents of the names it was sent;
  `PutMany` and `PutOrDeleteMany` never do (`IDatasetWriter.cs`,
  `DatasetWriter.PutEntitiesAsync`).

### Aliases

- **Alias** — a unique secondary key of an entity, computed from its data:
  an **alias value** within an **alias name** (e.g. value `B123` under name
  `by_storeesl`). One entity may own many; one alias value has at most one
  live owner (`AliasConflictException`, `IAliasIndex.cs`). _Avoid_: alias for
  the alias name itself; say "alias name".
- **Alias definition** — (alias name, factory, hash) registered with the
  writer; the **definitions hash** stored on every delta and snapshot detects a
  changed definition set (`Configs/AliasOptions.cs`).
- **Alias rebuild** — on start with a different definitions hash, the writer
  reads all live entities once and writes one delta of **alias-only changes**
  (`DatasetWriter.RebuildAliases`, `AliasOnlyChange` in the proto).
- **Alias conflict** — a write that would give one alias value two live
  owners; the writer rejects it with `AliasConflictException(OwnerName)`.
- **First-class alias** vs **reader-side alias** — aliases kept in the index
  and enforced by the writer (`AliasStaging`, `ResolveAlias`) vs
  `AliasDatasetReader` in `RxDatasets.Aliasing`, which evaluates alias
  patterns on read. Prefer the first-class path for new code (unverified as a
  project decision).
- **Id pattern** vs **alias pattern** — the name shape of a DTO
  (`t/{tenantid}/…`) vs the expression that derives an alias from fields
  (`{.field.path}`, `[]`, `/[..N]` slicing) (`RxDatasets.Aliasing/DI.cs`).

### Background processing and hosting

- **Consumer** — an `IDatasetUpdateConsumer<T>` run by
  `DatasetUpdateConsumingService` on the writer host whenever new deltas
  arrive (e.g. `SnapshotCreationPlugin`). Identified by `ConsumerIdentifier`.
- **Reporter** — an `IDatasetUpdateReporting<T>` receiving one callback per
  new, changed or deleted entity, driven by `DatasetUpdateReportingPlugin`
  (itself a consumer). Its progress is a processed version keyed by
  `ReportingIdentifier`. _Avoid_: confusing it with consumer — only reporters
  record processed versions.
- **Watcher** / **observer** — `DatasetWatcher` polls storage from any process
  (no lock, no stored progress) and calls an `IDatasetObserver`.
- **Processed version** — how far one reporter has got, in
  `ProcessedVersions.pb` (`StoreProcessedVersion(who, version)`); the lowest
  one gates cleaning (`StorageCleaningService`).
- **Cleaning** — `StorageCleaningService` deleting snapshots and deltas older
  than the oldest kept snapshot (`CleanupConfig`). _Avoid_: compaction (that is
  snapshot creation).
- **Distributed lock** — single-writer coordination across replicas through
  the `lock-file` and `request-lock` objects; **yield** / **handover** let a
  requesting instance take over (`DistributedLockService`,
  `LockServiceState` Idle/Acquiring/Active/Yielded). Writes without the lock throw.
- **Readiness** vs **health** — readiness (tagged with the tags passed to
  `Build`, served on `/readiness` by the test hosts) is unhealthy until the
  dataset is initialised; health (`/healthz`) reports failures after start.
  `StorageProviderReadinessNotifcation` is an internal signal, not a check.
- **Read-only mode** / **shared reader-writer** — `AddReadOnlyDtoServer<T>()`
  registers only a reader; `WithSharedReaderWriter()` uses one
  `DatasetReaderWriter<T>` for both, so reads see a write as soon as it returns.
- **Write coalescing** — `WithWriteCoalescing()`: concurrent writes merged
  into one delta, each caller getting its own `WriteResponse`
  (`DatasetWriter.DrainAndFlush`).
- **Tracking id / SLA timestamp** — optional per-write metadata stored with
  each changed entity and passed to reporters; when changes merge, the
  earliest SLA wins (`DatasetWriter.UpdateSlaTimestamp`). Business meaning
  unconfirmed (see the setup open tasks).

### Test fixtures

- **Demodto** / **Testdto** — the demo protobuf entities served by the
  `testapps/` hosts and used by tests and the e2e suite
  (`testapps/DTOs/proto/hightechinnovators/dtos/`). Not part of the library.
- **Expected vs actual server** — in the e2e suite, one host serves a
  pre-seeded reference bucket and one writes fresh; their responses are
  compared (`end-to-end-tests/compose.yml`).

---

## How to extend this file

- Add a term only when an agent has gotten it wrong at least
  once, or when a spec needs to disambiguate two candidate words.
- Keep entries to one short paragraph. If a term needs more,
  write a rule or an ADR and link it from here.
- Contradictions with product documentation are resolved in
  favour of product documentation; update this file in the same
  change.
