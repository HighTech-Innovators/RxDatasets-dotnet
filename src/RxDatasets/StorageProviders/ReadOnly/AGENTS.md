# src/RxDatasets/StorageProviders/ReadOnly

Storage providers for a host that only reads a dataset another host writes (`AddReadOnlyDtoServer<T>()`). Both classes
are also the base classes of the read/write providers one folder up.

## Files
- `ReadOnlyObjectStorageProvider.cs` — `ReadOnlyObjectStorageProvider<T>` (`IReadOnlyStorageProvider<T>`,
  `IHostedService`): holds `DeltaInfo` and `Snapshots` (protected, used by `ObjectStorageProvider`), refreshes them
  from `IStorageReader<T>.ListVersions` every 5 s, and notifies `DatasetUpdated`/`SnapshotCreated` when they change.
  `ReadDataFromSources` drops sources it believes are gone; `protected virtual Initialize`.
- `ReadOnlyCacheStorageProvider.cs` — `ReadOnlyCacheStorageProvider<T, TStorageProvider>`: a size-limited
  `MemoryCache` (`CacheSizeMB`, entry size in bytes) over another provider. Caches indexes, whole snapshot data and
  ranges (keys `{Delta|Snapshot}_{Index|Data}_{version}` and `…_pos{p}_len{l}`); preloads the newest snapshot that fits
  and evicts the previous one. Protected `MemoryCache` and `GetCacheKey` are used by `CacheStorageProvider`.

## Gotchas
- **Change notifications may lag or be lost** (unverified at runtime): `Initialize` compares the old `DeltaInfo` with
  the current one before `DoInitialize` has finished, so with an asynchronous `ListVersions` the comparison sees no
  change. The read-only `DatasetReader` re-reads the index every 5 s, which hides this.
- **The cache's preload waits for readiness `Ready`**, which a read-only host never sends; there it preloads only on
  `SnapshotCreatedNotification` and otherwise fills lazily. Without a hub, `StartAsync` blocks host start until the
  preload succeeds.
- **`StartAsync` here does not run for `ObjectStorageProvider`**, which hides it with `new`; the 5 s poll is read-only
  only.
- A range read stores the bytes under the positioned key of every source passed in, whichever source served it.
- `ReadSnapshotDataAsync` throws `NotImplementedException`.
