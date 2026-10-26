# src/RxDatasets.InMemoryDataset

An `IDatasetReader<T>`, `IDatasetWriter<T>` and `IAliasIndex` held wholly in memory, for tests. It replaces the core
reader, writer and storage; no `DtoServerBuilder` is involved.

## Files

- `InMemoryDataset.cs` — `InMemoryDataset<T>(aliasDefinitions)`: versions start at 1, one delta per changing write,
  aliases checked with core's `AliasStaging`; `ObserveDatasetChanges(fromVersion)` (Rx, concrete class only) and the
  `DatasetVersionUpdate` record.
- `DI.cs` — `DI.ConfigureServices<TEntity>(services)`: one singleton registered as reader and writer, with the
  `AliasOptions<T>` definitions when aliases are registered.
- `RxDatasets.InMemoryDataset.csproj` — references Abstractions and core.
- `RxDatasets.InMemoryDataset.v3.ncrunchproject` — NCrunch settings.

## Gotchas

- Distinct from `DtoServerBuilder.WithInMemoryDataset()`, which keeps the real reader/writer over core
  `InMemoryStorageProvider`. Combining both registers two `IDatasetReader<T>`; the last wins.
- Uses core internals through `InternalsVisibleTo RxDatasets.InMemoryDataset` in `RxDatasets.csproj`; renaming this
  assembly breaks the build.
- Behaves unlike the core reader in places: `List` always returns version 0; `List("")` and `List("/")` return only
  names with exactly one `/`, while `List(parent)` returns all descendants and `CountChildren("")` counts everything;
  `GetHistory`, `StreamHistory`, `TailHistory` throw `NotImplementedException`; `GetHistoric` throws unless the entity
  changed in exactly that version; `trackingId`/`slaTimestamp` are ignored.
- `Put` compares bytes; `PutMany`, `PutAll`, `PutAllChildren` and `PutOrDeleteMany` count an equal CRC32 as unchanged.
- `DeleteChildren` throws `ArgumentException` for an ancestor with an odd number of segments.
- `StartAsync`/`StopAsync` exist but the class is not registered as a hosted service.
