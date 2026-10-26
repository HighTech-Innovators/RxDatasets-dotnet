# tst/RxDatasets.StorageProvider.InMemoryProvider

Test helper library, not a test project: an in-memory `IDiskStorage<T>` that stands in for a blob backend. Assembly and namespace are `RxDatasets.TestImplementations.InMemoryProvider`. Used by `RxDatasets.StorageTests` and by `testapps/Testserver` (the host behind `GrpcWebApplicationFactory`).

## Files

- `LowLevelInMemoryStorage.cs` — `LowLevelInMemoryStorage<T>`: a `ConcurrentDictionary` of object key to bytes. Missing keys throw `FileNotFoundException`; a range read past the end throws `EndOfStreamException`; `DeleteFolder` appends a `/` to the prefix.
- `RxDatasets.StorageProvider.InMemoryProvider.csproj` — project file.

## Gotchas

- `ListObjects` reports each object's upload time from the wall clock (`DateTimeOffset.UtcNow`), never from an `IScheduler`. Age-based logic run on a `TestScheduler` sees real upload times; `AliasHostRestartTests` runs its hosts on a scheduler shifted 3 hours ahead to get past cleanup's 2-hour minimum age.
- `LastResult` is always healthy; this storage cannot simulate an outage. Tests that need failures, latency or a held upload wrap it in a decorator (`FaultyInMemoryStorage`, `SlowInMemoryStorage`, `GatedStorage`, `CountingStorage` in StorageTests).
- The csproj lists each project reference twice.
