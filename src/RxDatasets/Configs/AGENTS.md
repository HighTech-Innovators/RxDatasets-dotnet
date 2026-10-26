# src/RxDatasets/Configs

Options classes for the dataset engine. All but `AliasOptions<T>` are bound and validated (DataAnnotations, on start)
by `AddValidatedConfigSection` in `src/RxDatasets.Abstractions/ConfigExtensions.cs`, called from `DtoServerBuilder`.

## Files
- `StorageConfig.cs` — `StorageConfig`: `CustomRootFolder` (empty or ends in `/`), `DtoName` (overrides `typeof(T).Name`
  in the object prefix), `UploadVerificationMode`; enum `UploadVerificationMode` (`VerifyAll`, `VerifyLargeDataOnly`
  = stream uploads only, `DoNotVerify`). Section `Storage`.
- `WriterConfig.cs` — `WriterConfig`: `AllowedNamesPattern` (compiled into `AllowedNamesRegex` on set),
  `EnableWriteCoalescing` (set by `WithWriteCoalescing()` through `PostConfigure`). Section `Writer`.
- `SnapshotCreationConfig.cs` — `SnapshotCreationConfig`: `PeriodInHours` (24), `SnapshotInterval` in deltas (100).
  Section `SnapshotCreation`.
- `CleanupConfig.cs` — `CleanupConfig`: `MinimumAgeToLiveInMinutes` (≥ 120), `MinimumSnapshotsToKeep` (≥ 2),
  `KeepAllDeltas` (true), `IsEnabled` (true). Section `Cleanup`.
- `DTOStorageCacheConfig.cs` — `DtoStorageCacheConfig`: `CacheSizeMB` (0–2048, default 100). Section `DtoStorageCache`.
- `AliasOptions.cs` — `AliasDefinition(Name, Factory, DefinitionHash)` and `AliasOptions<T>.Definitions`; `HashOf`
  gives the order-independent definitions hash stored in every delta. Filled in code by `AddDatasetAlias` in
  `RxDatasets.Aliasing`, not from configuration.

## Gotchas
- **The section key is the class name minus `Config`** (`DistributedLockOptions` passes `DistributedLock` explicitly):
  renaming a class renames its configuration section.
- **One instance per host.** These options are not keyed by entity type, so every dataset in a host shares them.
- Some members are `virtual` (`GetMinimumAgeToLiveInMinutes`, `PeriodIntervalAsTimespan`, `SnapshotInterval`) so tests
  can mock them; keep them virtual.
- `DistributedLockOptions` lives in `BackgroundServices/`, not here.
