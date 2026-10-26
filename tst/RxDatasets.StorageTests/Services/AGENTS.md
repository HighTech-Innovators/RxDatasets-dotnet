# tst/RxDatasets.StorageTests/Services

Background services and dataset consumers, run on real `ObjectStorageProvider`s over `LowLevelInMemoryStorage` or on `InMemoryStorageProvider`.

## Files

- `SnapshotCreationTests.cs` — class `SnapshotCreationPluginTests`: `SnapshotCreationPlugin` making a time-based snapshot as a `TestScheduler` advances, plus the static `IsDeltaGapConditionMet` and `CalculateTimeUntilNextSnapshot`.
- `StorageCleaningTaskTests.cs` — class `StorageCleanTaskTests`: `StorageCleaningService` deleting old snapshots and deltas by count, minimum age and lowest processed version, on `InMemoryStorageProvider` and on a real `LocalDiskStorage` folder.
- `DatasetUpdateConsumingServiceStartTests.cs` — `DatasetUpdateConsumingService` start ordering under .NET 10 `BackgroundService` semantics; the class summary says why it lives in this project.
- `DataWatcherTests.cs` — `DatasetWatcher.SubscribeAllData` (`src/RxDatasets.DatasetWatcher`): starts from the latest snapshot and reports the deltas after it to a mocked `IDatasetObserver`.
- `TreeDeltaConsumerTests.cs` — consumers that read delta indexes directly (`DatasetWatcher`, `DatasetUpdateReportingPlugin`, `DataMigrator`) on tree-format deltas from the real writer.
- `FirstClassAliasTests.cs` — aliases kept in the index by `DatasetReaderWriter` with `AliasOptions`: conflicts, release on delete, `PutAll`/`PutAllChildren` implicit deletes, coalesced batches, restart without rebuild, snapshots, rebuild on changed definitions (including a crash mid-rebuild), replicas, and `OnAliasChanged` reporting. Defines `CountingStorage`, a decorator that counts entity-data reads and can hold an upload.

## Gotchas

- `DataWatcherTests` and `TreeDeltaConsumerTests` build their snapshot service with `UsingLowLevelInMemoryStorage.CreateSnapshotCreationService` from `../SpecTests/UsingInMemoryTests/`: interval 5, so snapshots land at version 1 and then every 5 deltas. Their expected versions follow from that.
- `SnapshotCreationPluginTests` starts the `TestScheduler` at `DateTime.UtcNow.Ticks`; its `Describe` message exists because the test has failed in CI with one snapshot where two were expected.
- `StorageCleanTaskTests` drives cleanup and snapshots with a `TestScheduler`, but its local-disk setup also waits a real 2 seconds and writes under `DatasetHelper.GetTempFileDirectory()`. Its logger factory writes Trace-level output to the console.
