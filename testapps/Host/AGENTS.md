# testapps/Host

Read-write Demodto DTO server on S3 storage (MinIO in compose) with distributed lock, cache, snapshots, cleanup and a console update reporter. Published by the root `Dockerfile` as `final-host` (`dotnet /bin/dtoserver/Host.dll`); run by root `compose.yml` and by `end-to-end-tests/`.

## Files

- `Program.cs` — `Host.Program.Main`: slim builder; Kestrel listens on 18080 (HTTP/1.1: grpc-web, `/healthz`, `/readiness`, `/`) and 10080 (HTTP/2 cleartext gRPC) unless the environment is `Testing`. After start it calls `/healthz` twice, 5 s apart, to warm the dataset.
- `DI.cs` — `ConfigureServices`: Demodto glue, `AddDtoServer<Demodto>().WithS3Storage().WithDistributedLock().WithCache().WithSnapshot().WithCleanup().Build("readiness")`, gRPC + reflection, `ConsoleReporter` through `DatasetUpdateReportingPlugin`/`DatasetUpdateConsumingService`. `ConfigureApp`: maps Demodto and Testdto services, reflection, grpc-web (`DefaultEnabled`), `/healthz` (checks without the `readiness` tag), `/readiness` (tagged checks), `/` (plain text).
- `ConsoleReporter.cs` — `IDatasetUpdateReporting<T>` that prints each added/changed/deleted name to stdout.
- `appsettings.json` — defaults for the sections below, pointing at MinIO on `localhost:9000`, bucket `dto-server`.
- `Host.csproj` — Web SDK; references `RxDatasets`, the S3, Minio, AzBlob and LocalDisk providers, and both DTO server projects.
- `Host.v3.ncrunchproject` — NCrunch settings.

Configuration read (section = config class name minus `Config`; override with env `Section__Key`): `Storage` (`CustomRootFolder`, must end in `/`; `DtoName`), `S3` (`AccessKey`, `SecretKey`, `ServiceUrl`, `BucketName`, `Region`), `DistributedLock`, `DtoStorageCache`, `SnapshotCreation`, `Cleanup`, `Writer`, and top-level `PageSize`, `MaxPageSize`, `PaginationCacheDuration` (`GrpcReaderServiceImpl`).

## Folders

- `Properties/` — launch profiles. See [Properties/AGENTS.md](Properties/AGENTS.md).

## Gotchas

- Inert config: `AzStorage`, `OpenTelemetry` and `Serilog` have no reader (no Serilog/OTel package); the `MINIOS3__*` env vars in the compose files only take effect after switching `DI.cs` to `WithMinioStorage()`; the explicit listeners override `Kestrel:EndpointDefaults`. Switching backend is a `DI.cs` edit — the provider references are already in the csproj.
- `S3` credentials `ROOTUSER`/`MINIOPASSWORD` are the local MinIO test values used by every compose file.
- Testdto services are mapped but no Testdto dataset is registered — see `../DTOs/TestdtoV1.Server/AGENTS.md`.
- Keep port 18080 and the `/` endpoint: the image `HEALTHCHECK` curls `http://127.0.0.1:18080/`, and `e2e_tests.sh` expects `Healthy` from `:18080/healthz`.
