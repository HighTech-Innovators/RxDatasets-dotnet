# testapps/ReadOnlyHost

Read-only Demodto DTO server on S3 with cache: serves a dataset another writer (e.g. `Host`) maintains. No Dockerfile stage or compose service builds it.

## Files

- `Program.cs` — `ReadOnlyHost.Program.Main`: Kestrel on 18081 (HTTP/1.1: grpc-web, `/healthz`, `/`) and 10081 (HTTP/2 cleartext gRPC) unless the environment is `Testing`; warms `/healthz` twice after start.
- `DI.cs` — `AddReadOnlyDtoServer<Demodto>().WithS3Storage().WithCache().Build()`, gRPC + reflection; maps the Demodto Reader and Writer services, grpc-web, `/healthz` (all checks; no `/readiness`), `/`.
- `appsettings.json` — `Storage`, `S3` (same MinIO `localhost:9000`, bucket `dto-server`, test credentials as `Host`), plus inert `OpenTelemetry`/`Serilog`.
- `ReadOnlyHost.csproj` — plain `Microsoft.NET.Sdk` exe; ASP.NET Core arrives through `DemodtoV1.Server`'s framework reference, hence the explicit `Microsoft.AspNetCore.*` usings. References Abstractions, the LocalDisk and S3 providers, `DemodtoV1.Server`, `DemodtoV1.Grpc`.

Configuration read: `Storage`, `S3`, `DtoStorageCache`, top-level `PageSize`/`MaxPageSize`/`PaginationCacheDuration` (see `../Host/AGENTS.md` for keys).

## Folders

- `Properties/` — launch profiles. See [Properties/AGENTS.md](Properties/AGENTS.md).

## Gotchas

- The Demodto `WriterService` is mapped, but read-only mode registers no `IDatasetWriter<Demodto>`, so write RPCs fail when called (failure mode unverified).
- `Storage:ReadOnlyMode` in `appsettings.json` matches no `StorageConfig` property; read-only comes from `AddReadOnlyDtoServer`.
- The plain SDK has no rule copying `appsettings.json` to the output folder (the Web SDK does). `dotnet run` from this folder finds it through the content root; a binary started elsewhere runs without it (unverified).
