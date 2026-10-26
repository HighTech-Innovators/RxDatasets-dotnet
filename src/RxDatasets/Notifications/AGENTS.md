# src/RxDatasets/Notifications

The notification records sent through a dataset's `INotificationsHub<T>` (see `../GenericNotifierLogic/`). Each file
holds one `readonly record struct` implementing `INotification`.

## Files
- `DatasetUpdatedOurNotification.cs` — `DatasetUpdatedNotification(ulong Version)`: a delta is stored.
  Sent by `ObjectStorageProvider.StoreDelta` (after `DeltaInfo.pb`), `ReadOnlyObjectStorageProvider` polling and
  `InMemoryStorageProvider`. Received by `DatasetReader` and `DatasetUpdateConsumingService`.
- `SnapshotCreatedOurNotification.cs` — `SnapshotCreatedNotification(ulong Version)`, the newest snapshot version.
  Sent by the storage providers. Received by `DatasetReader`, `DatasetWriter`, `StorageCleaningService`,
  `ReadOnlyCacheStorageProvider`.
- `VersionProcessedOurNotification.cs` — `VersionProcessedNotification(string Who, ulong NewVersion)`: a consumer stored
  its processed version. Sent by `StoreProcessedVersion`. Received by `StorageCleaningService` (only without
  `KeepAllDeltas`).
- `LockAquiredNotification.cs` — `DistributedLockNotification(DistirbutedLockStatus LockStatus)`, `Acquried` or `Lost`.
  Sent by `DistributedLockService`. Received by `ObjectStorageProvider`, which (re)initialises or reports `NotReady`.
- `StorageProviderReadinessNotifcation.cs` — `StorageProviderReadinessNotifcation(ServiceStatus ServiceStatus)`. Sent by
  `ObjectStorageProvider` and `InMemoryStorageProvider`, never by the read-only provider. Received by `DatasetReader`,
  `DatasetWriter`, `DatasetUpdateConsumingService`, `StorageCleaningService`, `ReadOnlyCacheStorageProvider`; `Ready`
  starts their initialisation, `NotReady` drops the reader's and writer's index.

## Gotchas
- **File names differ from type names**: the `*OurNotification.cs` files hold types without `Our`, and
  `LockAquiredNotification.cs` holds `DistributedLockNotification`. Search by type name.
- **The misspellings are public API**: `StorageProviderReadinessNotifcation`, and `DistirbutedLockStatus.Acquried` in
  `RxDatasets.Abstractions`. Renaming them breaks consumers.
- The hub is keyed by the notification's CLR type; a new notification needs a new type, not a new field.
