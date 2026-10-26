# src/RxDatasets.DatasetWatcher

Polling subscriber that replays a dataset from storage to an `IDatasetObserver<T>`: the newest snapshot first, then
every delta after it, until cancelled. Reads storage directly through `IStorageReader<T>`; keeps no processed version
and takes no lock, so it works from any host that can read the bucket.

## Files

- `DatasetWatcher.cs` — `IDatasetWatcher<T>.SubscribeAllData(observer, ct)` and its implementation `DatasetWatcher<T>`.
- `DatasetWatcherConfig.cs` — section `DatasetWatcher`: `CheckingIntervalInSeconds` (default 1).
- `DI.cs` — `DI.AddDatasetWatcher<TEntity>(services)`: a static method on a non-static class (not an extension
  method); registers the config and a singleton `IDatasetWatcher<TEntity>`.
- `RxDatasets.DatasetWatcher.csproj` — references core.

## Gotchas

- Needs `IStorageReader<T>`, which `DtoServerBuilder.Build()` registers only for object storage (not with
  `WithInMemoryDataset()`), and an `IDiskStorage<T>` from a `With*Storage()` call.
- Returns at once when the dataset has no snapshot: a dataset written without `WithSnapshot()` is never watched.
- Loads the whole snapshot data file into memory. Snapshot entities arrive by their latest change type (`OnCreate`,
  `OnChange` or `OnDelete`; none for `ChangeType.None`).
- Once cleanup has deleted deltas past the watcher's position (`version < OldestDelta`) it logs a warning every poll
  and never advances. Every poll error is logged as a warning and retried next interval.
- Not a hosted service: the caller owns the loop and its cancellation token.
