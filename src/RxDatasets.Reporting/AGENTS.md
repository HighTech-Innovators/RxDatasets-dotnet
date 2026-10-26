# src/RxDatasets.Reporting

`WithJsonReporting()`: a writer-side `IDatasetUpdateReporting<T>` that mirrors every entity change as an indented
JSON file in the dataset's own `IDiskStorage<T>`.

## Files

- `DI.cs` — `DtoServerBuilderExtensions.WithJsonReporting<TEntity>()`: binds section `JsonOutput`, registers
  `ProtobufJsonToObjectStorageReporter<T>`, wraps it in core `DatasetUpdateReportingPlugin<T, …>` and hosts that with
  `DatasetUpdateConsumingService<T, …>`.
- `ProtobufJsonToObjectStorageReporter.cs` — writes `{Prefix}[{version}/]{created|changed|deleted}/{name}[.{version}].json`
  (a delete writes `{}`); `ReportingIdentifier` is `json-entity-writer`.
- `JsonOutputConfig.cs` — `Prefix` (empty, or ending in `/`), `IncludeVersionInPath`, `IncludeVersionInFileName`.
- `RxDatasets.Reporting.csproj` — references core.
- `RxDatasets.Reporting.v3.ncrunchproject` — NCrunch settings.

## Gotchas

- Needs `IStorageProvider<TEntity>`, which `AddReadOnlyDtoServer` does not register: use it on the writer host.
- JSON goes under `Prefix` at the storage root, outside the dataset's `{CustomRootFolder}{DtoName}/` folder:
  `WithCleanup()` never removes it, and datasets sharing a bucket and `Prefix` share the `created/`, `changed/`,
  `deleted/` folders.
- Progress is the processed version `json-entity-writer` in the dataset's processed versions. `StorageCleaningService`
  deletes nothing past the lowest processed version and nothing removes an entry, so a host that once ran
  `WithJsonReporting()` and then drops it stops cleaning up.
- A delta that fails is retried every 5 s (core plugin); later deltas wait behind it.
- `OnAliasChanged` keeps its default no-op: alias changes are not written.
