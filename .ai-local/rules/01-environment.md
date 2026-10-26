# Environment (RxDatasets)

## Setup

- **Language/runtime.** C# on .NET 10: every project targets `net10.0`; CI
  builds in `mcr.microsoft.com/dotnet/sdk:10.0`. There is no `global.json`, so
  the newest installed 10.0 SDK is used.
- **Dependencies.** NuGet with central package management:
  versions live only in `Directory.Packages.props`
  (`ManagePackageVersionsCentrally`, transitive pinning on); csproj files list
  `PackageReference` without `Version`. No lock files locally
  (`Directory.Build.props`). No repo `nuget.config`: packages come from
  nuget.org, or from `PRIVATE_NUGET` / `NUGET_PROXY` in Docker builds.
- **System tools.** Docker (Testcontainers and the e2e suite); bash for
  `end-to-end-tests/*.sh`. `protoc` comes from `Grpc.Tools`, except in the
  Linux Docker build, which installs `protobuf-compiler` (arm64 workaround).
- **Cloud access.** None needed for development: every backend has a local
  stand-in (MinIO, Azurite, LocalStack, a GCS emulator) started by
  Testcontainers or `compose.yml`. Real S3/Azure/GCS credentials go only in
  a host's own configuration.

| Variable | Read by | Purpose |
|---|---|---|
| `DOCKER_HOST` | `tst/RxDatasets.StorageTests/*Fixture.cs`, `Dockerfile` `test` stage | Docker endpoint for Testcontainers; also the host the GCS emulator is reached on |
| `RXDATASETS_BENCHMARK` | `tst/RxDatasets.Tests/Benchmarks/IndexMemoryBenchmark.cs` | Set to run the index memory benchmark test, which is skipped otherwise |
| `STORAGE_EMULATOR_HOST` | `GoogleCloudStorage.cs` | GCS emulator endpoint when `EmulatorHost` is not configured |
| `GOOGLE_PROJECT_ID` | `GoogleCloudStorage.cs` | GCS project id fallback |
| `TEMP_FILE_DIRECTORY` | `DatasetHelper.cs` | Directory for temporary files (set to `/temp_file_directory` in the runtime images) |
| `Section__Key` (e.g. `S3__BUCKETNAME`, `Storage__CustomRootFolder`) | ASP.NET Core configuration in the test hosts | Override `appsettings.json` sections, as `compose.yml` does |
| `REGISTRY_PREFIX`, `PRIVATE_NUGET`, `NUGET_PROXY`, `VERSION`, `TARGET_NUGET`, `TARGET_NUGET_APIKEY` | `Dockerfile`, `.gitlab-ci.yml` | CI build arguments: registry mirror, package feeds, package version, NuGet push target |

## Usage

```
dotnet restore
dotnet build
dotnet test                                                           # all tests; Docker must be running
dotnet test tst/RxDatasets.Tests                                      # unit tests only, no Docker
dotnet test --filter "FullyQualifiedName!~WithCustomTestContainersImplementation"   # skip the external-provider suites (GoogleCloudStorage/WriteQueueIntegrationTests still needs Docker)
dotnet test --filter "FullyQualifiedName~ClassName.MethodName"         # one test
docker compose up --build        # testapps/Host + MinIO: gRPC :10080, grpc-web/health :18080 (/healthz, /readiness)
cd end-to-end-tests && ./run.sh  # e2e: builds two hosts + seeded MinIO, compares them with grpcurl
```

- **Build and release.** CI only (`.gitlab-ci.yml`): restore, build, test and
  e2e on every branch and MR; `dotnet pack` and `dotnet nuget push` on git tags,
  with the tag as package version.
- **Not available.** SonarQube (the host is gone; the stage is allowed to
  fail). The `.vscode/launch.json` configuration points at `src/Host`
  (net8.0), which no longer exists: run `testapps/Host` instead.

## Local assets

_None needed._
