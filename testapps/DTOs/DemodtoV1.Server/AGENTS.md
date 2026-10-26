# testapps/DTOs/DemodtoV1.Server

Class library that exposes a `Demodto` dataset over gRPC by adapting the generated `ReaderService`/`WriterService` bases to RxDatasets.

## Files

- `DI.cs` — `DI.ConfigureServices` registers the `GrpcReaderServiceImpl<Demodto>` singleton; `DI.ConfigureApp` maps `WriterServiceImpl` and `ReaderServiceImpl`. Hosts call both.
- `ReaderService.cs` — `ReaderServiceImpl`: `GetDemodto`, `BatchGetDemodtos`, `ListDemodtos`, `ListChanges` (forwards `wait_for_changes`), `CountChildren`; delegates to `GrpcReaderServiceImpl<Demodto>` (`src/RxDatasets/GrpcReaderServiceImpl.cs`).
- `WriterService.cs` — `WriterServiceImpl`: `Put`, `Delete`, `PutAll`, `PutAllChildren`, `PutMany`, `PutOrDeleteMany` onto `IDatasetWriter<Demodto>`; tracking id and SLA timestamp are always `null`.
- `DemodtoV1.Server.csproj` — library with the ASP.NET Core framework reference; references `src/RxDatasets` and `../Grpc`.
- `DemodtoV1.Server.v3.ncrunchproject` — NCrunch settings.

## Gotchas

- `DI.ConfigureServices` registers no dataset. The host supplies `IDatasetReader<Demodto>`/`IDatasetWriter<Demodto>` via `AddDtoServer<Demodto>()` or `AddReadOnlyDtoServer<Demodto>()` … `.Build()`.
- Generated types live in `Hightechinnovators.Dtos.DemodtoV1` / `Hightechinnovators.Dtos.Type` (proto `csharp_namespace`), separate from this project's `RxDatasets.DemodtoV1.Server` namespace.
- `tst/RxDatasets.StorageTests` references this project, so changes here run through those tests.
