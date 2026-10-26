# src/RxDatasets.StorageProvider.LocalDisk

`IDiskStorage<T>` on a local folder; an object key is a path relative to the root folder. Cross-provider
differences: [`../AGENTS.md`](../AGENTS.md).

## Files

- `DI.cs` — `WithLocalDiskStorage<TEntity>()`: binds section `LocalDiskStorage`,
  `AddSingleton<IDiskStorage<T>, LocalDiskStorage<T>>`. Registers no health check.
- `LocalDiskStorage.cs` — `LocalDiskStorage<T>`, plus `LocalDiskStorageConfig` (`RootFolder`, default
  `dataset-localstorage`, relative to the working directory). Writes go to `.tmp_{file name}`, then
  `File.Replace`/`File.Move`.
- `RxDatasets.StorageProvider.LocalDisk.csproj` — references Abstractions and core.
- `RxDatasets.StorageProvider.LocalDisk.v3.ncrunchproject` — NCrunch settings.

## Gotchas

- `ListObjects(prefix)` splits the prefix into a directory and a `{last segment}*` file-name pattern searched
  recursively, so it behaves like a blob prefix only when the prefix ends in `/` (core always passes one). It returns
  an empty list on any error, and lists in-flight `.tmp_*` files.
- Concurrent writes to one key share one temp file name.
- `UploadBytes` falls back to delete-then-move on `UnauthorizedAccessException`; `UploadFileStream` has no fallback.
- Reads create missing parent directories as a side effect.
- `LastResult` is maintained but nothing reads it.
