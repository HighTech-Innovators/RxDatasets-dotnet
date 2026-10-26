# src/RxDatasets.Abstractions

Public contracts every other project codes against, plus the `AddValidatedConfigSection` options helper. No
implementations beyond default interface methods. Package `HighTechInnovators.RxDatasets.Abstractions`.

## Files

- `IDatasetReader.cs` — `IDatasetReader<T>` (`Get`, `BatchGet`, `List`, `CountChildren`, `GetChangedNames`,
  `WaitChangedNames`, `GetHistory`, `GetHistoric`, `StreamHistory`, `TailHistory`) and the `ChangeSet` record.
  Implemented by core `DatasetReader`/`DatasetReaderWriter`, `InMemoryDataset`, `AliasDatasetReader`.
- `IDatasetWriter.cs` — `IDatasetWriter<T>` and nested `WriteResponse`: `Put`, `PutMany`, `PutAll`, `PutAllChildren`,
  `PutOrDeleteMany`, `Delete`, `DeleteMany`, `DeleteChildren`. The `[Obsolete]` overloads without
  `trackingId`/`slaTimestamp` are default interface methods forwarding to the current ones.
- `IDiskStorage.cs` — `IDiskStorage<T>`, the blob-level backend: whole/ranged/streamed download, upload bytes/stream,
  delete file/folder, `ListObjects(prefix)`, `GetObjectContentLength`, `FlushAsync` (default no-op). Implemented by
  every `RxDatasets.StorageProvider.*`.
- `IStorageProvider.cs` — the delta/snapshot level above it: `IStorageProviderReader` (read delta/snapshot index and
  data, `ListVersions`, `ReadDataFromSources`), `IStorageProviderWriter` (store delta/snapshot, `DeleteOlderThan`,
  `StoreProcessedVersion`), `IStorageProvider` (adds processed-version reads), and the per-entity keys
  `IStorageProvider<T>`, `IReadOnlyStorageProvider<T>`, `IStorageReader<T>`.
- `IAliasIndex.cs` — `ResolveAlias`, `ListAliases`; implemented by the dataset reader so lookups match its version.
- `AliasConflictException.cs` — a write would leave one alias with two live owners; nothing of the write is stored.
- `IDatasetUpdateConsumer.cs` — `HandleNewDeltas`, `ConsumerIdentifier`; run by core `DatasetUpdateConsumingService`.
- `IDatasetUpdateReporting.cs` — per-entity callbacks (`OnNewDto`, `OnChangedDto`, `OnDeletedDto`, `OnAliasChanged`)
  and optional per-delta transaction hooks; driven by core `DatasetUpdateReportingPlugin`.
- `IDatasetObserver.cs` — per-version and per-entity callbacks used by `RxDatasets.DatasetWatcher`.
- `IDataMigrator.cs` — `MigrateDataset(migrationName)`; implemented by core `DataMigrator<T>`.
- `ConfigExtensions.cs` — `AddValidatedConfigSection<TConfig>()` and `(key)`: binds the section, registers
  `IOptions<TConfig>`, a `TConfig` singleton and `IOptionsMonitor<TConfig>`, validates DataAnnotations on start.
- `HistoryEntry.cs` — record returned by the history APIs.
- `ChangeType.cs`, `SnapshotType.cs`, `ServiceStatus.cs`, `DistirbutedLockStatus.cs` — small enums/records.
- `ObjectNameTypeEnum.cs`, `ObjectNameIndexOrDataEnum.cs` — select the `deltas/`/`snapshots/` folder and the
  `index.pb`/`data.pbs` file in core `DatasetHelper.GetObjectName`.
- `RxDatasets.Abstractions.csproj` — packable; only Microsoft.Extensions configuration/options/DI/logging packages.
- `RxDatasets.Abstractions.v3.ncrunchproject` — NCrunch settings.

## Folders

- `BackgroundServices/` — the distributed lock's state enum. See [`BackgroundServices/AGENTS.md`](BackgroundServices/AGENTS.md).

## Gotchas

- The section key is the config type name with every `Config` removed (`AzStorageConfig` → `AzStorage`). Renaming a
  config class renames its appsettings section.
- `AddValidatedConfigSection` prints every key and value of the section to the console on first bind, secrets
  included (`S3:SecretKey`, `AzStorage:ConnectionString`, `GoogleCloud:JsonCredentials`). It also builds a throwaway
  service provider inside the `Configure` callback.
- The `T` on `IDiskStorage<T>`, `IStorageProvider<T>`, `IReadOnlyStorageProvider<T>` and `IStorageReader<T>` is only a
  per-entity DI key. `IStorageReader<T>` and `IReadOnlyStorageProvider<T>` share members but not backing:
  `IStorageReader<T>` is core `ObjectStorageReader` (uncached, straight off `IDiskStorage<T>`, registered only with
  object storage); `IReadOnlyStorageProvider<T>` is the hosted, optionally cached provider.
- Core detects a missing object by catching `FileNotFoundException` (`ObjectStorageReader.ObjectExists`); providers
  honour the rest of the exception contract unevenly, see [`../AGENTS.md`](../AGENTS.md).
- `HistoryEntry.Id` is filled by `StreamHistory`/`TailHistory` (`DeltaIndex.EnumerateHistoryEntries`) and left empty
  by `DatasetReader.GetHistory`.
- `IDataMigrator` is non-generic but `DtoServerBuilder.Build()` registers one per entity type; with several datasets a
  plain `IDataMigrator` resolve gets the last one.
- `OpenDeltaTransaction`'s return value reaches the per-entity callbacks as `optionalTransaction`.
- `DistirbutedLockStatus.Acquried` is misspelt public API; a rename breaks consumers.
