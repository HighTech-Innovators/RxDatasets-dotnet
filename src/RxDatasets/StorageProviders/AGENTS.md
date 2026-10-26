# src/RxDatasets/StorageProviders

The layer between the reader/writer and a blob backend: it names, stores and lists deltas, snapshots and the metadata
files over an `IDiskStorage<T>` (implemented in the `RxDatasets.StorageProvider.*` projects), and caches their bytes.
Layering, bottom up: `IDiskStorage<T>` → `ObjectStorageReader<T>` → `ReadOnly/ReadOnlyObjectStorageProvider<T>` →
`ObjectStorageProvider<T>` → optionally `CacheStorageProvider<T, …>`, which is then the `IStorageProvider<T>`.

## Files
- `ObjectStorageReader.cs` — `ObjectStorageReader<T>` (`IStorageReader<T>`): reads indexes and data ranges, and
  `ListVersions` from `DeltaInfo.pb`/`SnapshotInfo.pb`, with fallbacks: directory listing when they look corrupt or
  the latest delta index is missing; legacy `VersionIndex.pb`, then `latestVersion.pb`, then listing when they are
  absent; a forward probe for deltas newer than `DeltaInfo`.
- `ObjectStorageProvider.cs` — `ObjectStorageProvider<T>` (`IStorageProvider<T>`, `IHostedService`): `StoreDelta`,
  `StoreSnapshot` (byte[] and Stream overloads), `DeleteOlderThan`, `StoreProcessedVersion`, `GetProcessedVersion`,
  `GetLowestProcessedVersion`; optional upload verification; lock checks; sends readiness, update and snapshot
  notifications.
- `CacheStorageProvider.cs` — `CacheStorageProvider<T, TStorageProvider>`: the read cache of `ReadOnly/` plus
  write-through caching of what it stores; forwards processed versions and `DeleteOlderThan`.
- `InMemoryStorageProvider.cs` — `InMemoryStorageProvider<T>`: dictionaries instead of storage, registered by
  `WithInMemoryDataset()`.
- `FolderNames.cs` — `deltas` and `snapshots`, the folder names in object keys.

## Folders
- `ReadOnly/` — the read-only provider and read cache, base classes of the two above. [AGENTS.md](ReadOnly/AGENTS.md)

## Gotchas
- **`DeltaInfo.pb` is the commit point.** `StoreDelta` uploads index and data in parallel, then saves `DeltaInfo`
  (rolled back in memory if that fails), then notifies. Readers also find deltas past a stale `DeltaInfo` by probing
  for `deltas/{n+1}/index.pb`.
- **The read/write provider trusts its memory.** It keeps `DeltaInfo` and the snapshot list in memory and never polls
  (the base `StartAsync` loop does not run); only a re-init reloads them. That assumes one writer, which the
  distributed lock provides.
- **`new StartAsync`/`StopAsync`** in `ObjectStorageProvider` take effect only because the class re-lists
  `IHostedService`; a call through a `ReadOnlyObjectStorageProvider<T>` reference runs the base versions. Base and
  derived class each have their own private `_initTask`.
- **With a lock service, initialisation waits for `DistributedLockNotification(Acquried)`**; `Lost` sends readiness
  `NotReady`, which makes the reader and writer drop their index. Every write throws `SynchronizationLockException`
  without the lock.
- **Requests wait at most ~1 s for initialisation**, then throw `InvalidOperationException`.
- `ObjectStorageReader.ReadDataFromSources` reads only the first source; trying the others is up to the caches.
  `SharedMethods.ExtractNumber` parses versions as `int`.
- `CacheStorageProvider` calls `StartAsync` in its constructor and is also registered as a hosted service, so it starts
  twice.
- `CacheStorageProvider.StoreDelta(byte[], byte[])` caches the index under the data key and the data under the index
  key. `CacheStorageProviderTest` builds both arrays from the same seed and cannot see it. The writer only calls the
  `Stream` overload, which delegates to the inner provider's byte[] overload.
- `InMemoryStorageProvider.ReadSnapshotData(ulong)` reads from the delta map; `ListVersions` reports no snapshots while
  there are no deltas; its constructor blocks on `Notify(...).Wait()`; `StoreDelta(Stream)` disposes the caller's stream.
- `ReadSnapshotDataAsync` throws `NotImplementedException` in every provider.
