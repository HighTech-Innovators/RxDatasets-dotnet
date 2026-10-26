# testapps/DTOs/TestdtoV1.Server

Class library that exposes a `Testdto` dataset over gRPC; a smaller sibling of `DemodtoV1.Server`.

## Files

- `DI.cs` — `DI.ConfigureServices` registers the `GrpcReaderServiceImpl<Testdto>` singleton; `DI.ConfigureApp` maps `WriterServiceImpl` and `ReaderServiceImpl`.
- `ReaderService.cs` — `ReaderServiceImpl`: `GetTestdto`, `BatchGetTestdtos`, `ListTestdtos`, `ListChanges`, `CountChildren` via `GrpcReaderServiceImpl<Testdto>`.
- `WriterService.cs` — `WriterServiceImpl`: `Put`, `Delete`, `PutAll` onto `IDatasetWriter<Testdto>` (all the Testdto `WriterService` proto declares).
- `TestdtoV1.Server.csproj` — library; references `src/RxDatasets.Abstractions`, `src/RxDatasets`, `../Grpc/DemodtoV1.Grpc.csproj`.
- `TestdtoV1.Server.v3.ncrunchproject` — NCrunch settings.

## Gotchas

- No host registers a Testdto dataset. `Host` maps these services but never calls this `DI.ConfigureServices` or `AddDtoServer<Testdto>()`, so their constructor dependencies are missing and Testdto RPCs fail when called (failure mode unverified). `Testserver` references the project without mapping it.
- `ListChanges` drops `wait_for_changes` when mapping the request, so long-polling is ignored here (Demodto forwards it).
