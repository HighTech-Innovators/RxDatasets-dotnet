# src/RxDatasets/BackgroundServices

Hosted services and dataset-update consumers that run beside a read/write dataset: snapshot creation, storage
cleaning, the distributed write lock, and the loop that drives consumers.

## Files
- `DatasetUpdateConsumingService.cs` — `DatasetUpdateConsumingService<TDto, TConsumer>` (`BackgroundService`): once the
  provider is ready, calls `TConsumer.HandleNewDeltas` and then again after every `DatasetUpdatedNotification`. A
  notification handler returns only after a run that started after it has completed.
- `SnapshotCreationPlugin.cs` — `SnapshotCreationPlugin<T>` (`IDatasetUpdateConsumer<T>`): writes a snapshot when
  `SnapshotInterval` deltas have passed or `PeriodInHours` has elapsed since the last one. Public statics
  `IsDeltaGapConditionMet`, `CalculateTimeUntilNextSnapshot`, `EfficientCopy`; internal `SeekForwardBy` (also used by
  `DatasetWriter`).
- `DatasetUpdateReportingPlugin.cs` — `DatasetUpdateReportingPlugin<TDto, TReporter>` (`IDatasetUpdateConsumer<TDto>`):
  replays every unreported delta into an `IDatasetUpdateReporting<TDto>` (open/commit/rollback transaction, per-entity
  and per-alias callbacks), then stores its processed version. Wired up by `RxDatasets.Reporting`.
- `StorageCleaningService.cs` — `StorageCleaningService<T>` (`IHostedService`): deletes snapshots older than the newest
  `MinimumSnapshotsToKeep` and older than `MinimumAgeToLiveInMinutes`, and their deltas unless `KeepAllDeltas`; runs on
  readiness, snapshot-created and (without `KeepAllDeltas`) version-processed notifications, and on `IScheduler` timers.
- `DistributedLockService.cs` — `DistributedLockService<TEntity>` (`IHostedService`): one writer per dataset through a
  `lock-file` and a `request-lock` object under the dataset prefix; `HasLock`, `State` (`LockServiceState`); notifies
  `DistributedLockNotification`.
- `DistributedLockOptions.cs` — `RenewalInterval`, `PollingInterval`, `RenewalThreshold`, `HandoverTimeout`; bound from
  section `DistributedLock`.
- `IClock.cs`, `SystemClock.cs`, `RxSchedulerClock.cs` — `IClock` (`UtcNow`, `Delay`) for the lock loop; the Rx version
  drives it from an `IScheduler` (tests use a `TestScheduler`).

## Gotchas
- **Processed versions gate cleaning.** Only `DatasetUpdateReportingPlugin` stores processed versions, under
  `ReportingIdentifier` (not `ConsumerIdentifier`). `StorageCleaningService` deletes nothing while the lowest processed
  version is below the oldest kept snapshot, and with no reporter that lowest version is 0, so nothing is ever deleted.
- **"Synchronous hub" comments hold only in tests.** `DatasetUpdateConsumingService` documents that a write returns
  after the consumer saw it; that holds with `NotificationsHubSync`. `DtoServerBuilder` registers the asynchronous
  `NotificationsHub`, where `Notify` returns after enqueueing.
- **Snapshot timers pile up.** Each run that finds a snapshot not yet due schedules another `IScheduler` callback, which
  blocks on `HandleNewDeltas(...).Wait()` outside the consuming service; the semaphore makes the extra runs no-ops.
- **Snapshots are rebuilt from storage** (`DatasetHelper.GetIndexOfVersion`), not from the writer's memory. Deleted
  entities stay in the new snapshot index as tombstones (position and length 0), contrary to `diskFormat.md`. An empty
  snapshot's data file is one zero length prefix.
- **Lock acquisition is read-then-write** on a plain `UploadBytes`, with no conditional write, so two instances racing
  can both acquire (unverified at runtime). Expiry is compared with each instance's own clock.
- **`Acquried` is notified before `_lockToken` is set**, so a handler that runs before `AcquireLockAsync` returns sees
  `HasLock == false`; `ObjectStorageProvider` writes throw `SynchronizationLockException` then.
- **Handover:** a starting instance writes its id to `request-lock`; the holder yields when it sees another id, and a
  yielded instance reclaims after `HandoverTimeout`, which must exceed `PollingInterval` (the constructor throws).
  `StopAsync` flushes `IDiskStorage.FlushAsync` before releasing.
- Lock defaults exist twice: in `DistributedLockOptions` and as constructor fallbacks.
- `IClock`/`SystemClock` are also used by `RxDatasets.StorageProvider.GoogleCloud`; keep their namespace stable.
- Reporting: a delta whose index or data is missing is skipped and still marked processed; a delta that throws is
  retried every 5 s and blocks the ones after it. Legacy `DeletedNames` and `DeletedEntities` are both reported.
- `StorageCleaningService` keeps scheduled dates in a plain `List` while several notification handlers run
  `SearchForOutdatedData` concurrently.
