# src

The shipped `HighTechInnovators.*` packages. `RxDatasets/` is the core; every other project plugs into it, either as a
`DtoServerBuilder<T>` extension method or as an `IDiskStorage<T>` backend that core `ObjectStorageProvider<T>` /
`ObjectStorageReader<T>` turn into deltas and snapshots.

## Folders

- `RxDatasets/` — core library: builder, reader/writer, object storage provider, background services. See
  [`RxDatasets/AGENTS.md`](RxDatasets/AGENTS.md).
- `RxDatasets.Abstractions/` — public contracts (reader, writer, disk storage, storage provider, reporting, alias index)
  and `AddValidatedConfigSection`. See [`RxDatasets.Abstractions/AGENTS.md`](RxDatasets.Abstractions/AGENTS.md).
- `RxDatasets.Aliasing/` — look entities up by alias; alias registration and the alias pattern language. See
  [`RxDatasets.Aliasing/AGENTS.md`](RxDatasets.Aliasing/AGENTS.md).
- `RxDatasets.DatasetWatcher/` — polling observer that replays the newest snapshot, then each new delta. See
  [`RxDatasets.DatasetWatcher/AGENTS.md`](RxDatasets.DatasetWatcher/AGENTS.md).
- `RxDatasets.InMemoryDataset/` — a whole reader/writer kept in memory, for tests. See
  [`RxDatasets.InMemoryDataset/AGENTS.md`](RxDatasets.InMemoryDataset/AGENTS.md).
- `RxDatasets.Reporting/` — `WithJsonReporting()`: mirrors each entity change as a JSON file. See
  [`RxDatasets.Reporting/AGENTS.md`](RxDatasets.Reporting/AGENTS.md).
- `RxDatasets.StorageProvider.AzBlob/` — `IDiskStorage<T>` on Azure Blob. See
  [`RxDatasets.StorageProvider.AzBlob/AGENTS.md`](RxDatasets.StorageProvider.AzBlob/AGENTS.md).
- `RxDatasets.StorageProvider.GoogleCloud/` — `IDiskStorage<T>` on Google Cloud Storage, with a write queue. See
  [`RxDatasets.StorageProvider.GoogleCloud/AGENTS.md`](RxDatasets.StorageProvider.GoogleCloud/AGENTS.md).
- `RxDatasets.StorageProvider.LocalDisk/` — `IDiskStorage<T>` on a local folder. See
  [`RxDatasets.StorageProvider.LocalDisk/AGENTS.md`](RxDatasets.StorageProvider.LocalDisk/AGENTS.md).
- `RxDatasets.StorageProvider.Minio/` — `IDiskStorage<T>` on S3-compatible storage via the Minio SDK. See
  [`RxDatasets.StorageProvider.Minio/AGENTS.md`](RxDatasets.StorageProvider.Minio/AGENTS.md).
- `RxDatasets.StorageProvider.S3/` — `IDiskStorage<T>` on AWS S3 or S3-compatible storage via AWSSDK.S3. See
  [`RxDatasets.StorageProvider.S3/AGENTS.md`](RxDatasets.StorageProvider.S3/AGENTS.md).

## Gotchas

- `RxDatasets.Reporting.Cqs/` is stale build output (`bin/`, `obj/` only): untracked, not in `RxDatasets.sln`. Same for
  `tst/RxDatasets.Reporting.Cqs.Tests/`.
- Three different "in memory" pieces: `RxDatasets.InMemoryDataset` replaces reader and writer;
  `DtoServerBuilder.WithInMemoryDataset()` keeps the real reader/writer over core `InMemoryStorageProvider`;
  `tst/RxDatasets.StorageProvider.InMemoryProvider` (`LowLevelInMemoryStorage<T>`) is a test `IDiskStorage<T>`.
- One storage provider per entity type. AzBlob, Minio and LocalDisk register with `AddSingleton` (last `With*Storage()`
  wins); S3 and GoogleCloud with `TryAddSingleton` (first wins).
- Provider config sections are unnamed options: every dataset in a host shares one bucket/container, kept apart only by
  the `{CustomRootFolder}{DtoName or type name}/` key prefix (core `DatasetHelper.GetObjectNamePrefix`).
- Where the `IDiskStorage<T>` contract is silent, providers differ:

  | Provider | Calls before init finishes | Non-404 read errors | Ranged read gets fewer bytes than asked | Delete of missing object | Health check (probe when cached result > 5 min) |
  |---|---|---|---|---|---|
  | AzBlob | wait ~1 s, then throw | `InvalidOperationException` | zero-padded buffer | no-op | `storage.azblob.{T}`, ranged read of `something/notimportant` |
  | GoogleCloud | wait up to ~500 s | raw SDK exception | shorter buffer | no-op | `storage.googlecloud.{T}`, lists the whole bucket |
  | LocalDisk | n/a (sync ctor) | raw I/O exception | `EndOfStreamException` | logs a warning | none |
  | Minio | wait ~1 s, then throw | `InvalidOperationException` only if transient | zero-padded buffer | no-op (unverified) | `storage.minio.{T}`, ranged read of `something/notimportant` |
  | S3 | wait up to ~500 s | `InvalidOperationException` | zero-padded buffer | no-op | `storage.s3.{T}`, ranged read of `something/notimportant` |
