# testapps/Host/Properties

## Files

- `launchSettings.json` — profiles `Host` (project) and `WSL` (WSL2, empty `distributionName`), both with `ASPNETCORE_ENVIRONMENT=Development` and an empty `applicationUrl`; ports come from `../Program.cs`.

## Gotchas

- There is no `appsettings.Development.json`, so a local run uses `../appsettings.json`: S3 at `localhost:9000`. Start MinIO first (the `minio` service in root `compose.yml`).
