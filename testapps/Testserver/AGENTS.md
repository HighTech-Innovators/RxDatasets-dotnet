# testapps/Testserver

Project `Host.Testserver`: read-write Demodto DTO server on in-memory storage (`LowLevelInMemoryStorage` from `tst/RxDatasets.StorageProvider.InMemoryProvider`). It is the `Program` that `tst/RxDatasets.StorageTests` hosts through `WebApplicationFactory<Program>` (`GrpcWebApplicationFactory`), and the root `Dockerfile` publishes it as `final-testcontainer` (`/bin/dtoserver/Host.Testserver`).

## Files

- `Program.cs` — `Program.Main` in the global namespace: Kestrel on 18080 (HTTP/1.1) and 10080 (HTTP/2 cleartext) unless the environment is `Testing`; the `/healthz` warm-up is commented out.
- `DI.cs` — namespace `Host.Testserver`. `ConfigureServices`: Demodto glue, `AddDtoServer<Demodto>().WithDistributedLock().WithSnapshot().WithCleanup().Build()`, `IDiskStorage<Demodto>` = `LowLevelInMemoryStorage<Demodto>`, gRPC + reflection. `ConfigureApp`: Demodto services, reflection, grpc-web, `/healthz`, `/`, then resolves `IDatasetReader`/`IDatasetWriter<Demodto>` eagerly.
- `appsettings.json` — logging, Kestrel defaults and an unread `Serilog` section; no `DistributedLock`/`SnapshotCreation`/`Cleanup`, so those run on class defaults.
- `Host.Testserver.csproj` — Web SDK; references `RxDatasets`, `RxDatasets.InMemoryDataset`, both DTO servers and the `tst` in-memory provider.
- `Host.Testserver.v3.ncrunchproject` — NCrunch settings.

## Folders

- `Properties/` — launch profiles. See [Properties/AGENTS.md](Properties/AGENTS.md).

## Gotchas

- Keep `Program` in the global namespace and the `Testing` check: `GrpcWebApplicationFactory` binds to the unqualified `Program` and sets environment `Testing` so the TestServer replaces Kestrel.
- Storage is per process and empty at start; nothing persists across restarts.
- The `final-testcontainer` target is not among the CI `DOCKERFILE_TARGET`s and no test in the repo references it.
