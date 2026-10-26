# Project Overview (RxDatasets)

## Project description

RxDatasets is a set of .NET libraries, shipped as NuGet packages, for
building *dataset servers*: services that store one protobuf message type
(`TEntity : IMessage<TEntity>`) per dataset as named entities on blob storage,
and let clients read the current state, list children of a path-like name, and
ask "what changed since version N" (polling or waiting reactively). Writes are
appended as versioned **deltas**; background work aggregates them into
**snapshots** and cleans up old ones. The library is inspired by Netflix Hollow
(`Directory.Build.props` description). The gRPC contract a host exposes is the
host's own; the `testapps/` folder holds a demo contract (`Demodto`, `Testdto`)
and hosts used for tests and benchmarks.

What a consumer of the library does:

- Registers a dataset with `services.AddDtoServer<TEntity>()` (or
  `AddReadOnlyDtoServer<TEntity>()`), picks one backend
  (`WithS3Storage()`, `WithMinioStorage()`, `WithAzureStorage()`,
  `WithGoogleCloudStorage()`, `WithLocalDiskStorage()`), adds options
  (`WithCache`, `WithDistributedLock`, `WithSnapshot`, `WithCleanup`,
  `WithSharedReaderWriter`, `WithWriteCoalescing`), then calls `Build(readinessTags)`.
- Injects `IDatasetWriter<TEntity>` / `IDatasetReader<TEntity>` (or wraps the
  reader in `GrpcReaderServiceImpl<TEntity>` behind its own gRPC service).
- Optionally hosts consumers of new deltas (`IDatasetUpdateConsumer<T>` run by
  `DatasetUpdateConsumingService`), e.g. JSON reporting or a `DatasetWatcher`.

The stored layout is a contract with every process that opens the dataset
(other replicas, reporting jobs, older library versions): see
`src/RxDatasets/diskFormat.md` before changing anything in
`src/RxDatasets/Models/*.proto` or the object naming in `DatasetHelper`.

Stack: C# on .NET 10 (`net10.0` in every csproj), Google.Protobuf + Grpc.Tools,
System.Reactive, Microsoft.Extensions.* (Hosting, Options, Caching, HealthChecks);
backends via AWSSDK.S3, Minio, Azure.Storage.Blobs (+ Azure.Identity),
Google.Cloud.Storage.V1. Tests: xunit, FluentAssertions, NSubstitute, Moq,
Microsoft.Reactive.Testing, Testcontainers (fixtures for MinIO, Azurite,
LocalStack S3, Google Cloud Storage and the Host image in
`tst/RxDatasets.StorageTests/*Fixture.cs`). Built and published from GitLab CI through Docker stages; packages
pushed to a NuGet feed on tags.

## Nested AGENTS.md files

Read the one closest to the file you're editing before making changes
there.

Each has a sibling `CLAUDE.md` symlink.

- `src/AGENTS.md` — the shipped packages and the provider behaviour table;
  links down to `RxDatasets/` (core: `BackgroundServices/`, `Configs/`,
  `GenericNotifierLogic/`, `Models/`, `Notifications/`, `StorageProviders/`
  → `ReadOnly/`), `RxDatasets.Abstractions/` (→ `BackgroundServices/`),
  `RxDatasets.Aliasing/`, `RxDatasets.DatasetWatcher/`,
  `RxDatasets.InMemoryDataset/`, `RxDatasets.Reporting/` and the five
  `RxDatasets.StorageProvider.*/` backends.
- `tst/AGENTS.md` — test projects, Docker needs and filters; links down to
  `RxDatasets.Tests/` (→ `Benchmarks/`, `DatasetWriterTests/`, `Models/`),
  `RxDatasets.StorageTests/` (→ `AzBlobStorage/`, `Common/`,
  `GoogleCloudStorage/`, `Services/`, `SpecTests/` → `UsingInMemoryTests/`,
  `WithCustomTestContainersImplementation/`), `RxDatasets.AliasingTests/`,
  `RxDatasets.StorageProvider.InMemoryProvider/`.
- `testapps/AGENTS.md` — demo DTOs, hosts, benchmarks, client; links down to
  `DTOs/` (→ `DemodtoV1.Server/`, `TestdtoV1.Server/`, `Grpc/`, `proto/…`),
  `Host/`, `ReadOnlyHost/`, `Testserver/` (each → `Properties/`),
  `RxDatasetsBenchmark/`, `RxDatasetsGcsBenchmark/`,
  `RxDatasetsReaderBenchmark/`, `TestClient/`.
- `end-to-end-tests/AGENTS.md` — the docker-compose e2e suite; links down to
  `expected-data/` and `generate-expected-data/`.

## Architecture

1. **Write.** `DatasetWriter<T>` (or the combined `DatasetReaderWriter<T>`)
   receives `Put*` / `Delete*` / `PutOrDeleteMany` calls, computes a CRC32 per
   entity to skip unchanged bytes, stages alias changes, and builds one delta
   (index + concatenated delimited protobuf data).
2. **Store.** `IStorageProvider<T>` — `ObjectStorageProvider<T>`, optionally
   wrapped by `CacheStorageProvider<T, …>` — uploads
   `deltas/{version}/index.pb` and `data.pbs` through an `IDiskStorage<T>`
   backend, updates `DeltaInfo.pb`, then publishes a
   `DatasetUpdatedNotification` on the `INotificationsHub<T>`.
3. **Read.** `DatasetReader<T>` loads the latest snapshot index plus the
   following delta indexes into an `ImmutableDatasetState` (hierarchical
   `ImmutableIndexNode` tree), and serves `Get`, `BatchGet`, `List`,
   `CountChildren`, `GetChangedNames` / `WaitChangedNames`, history and alias
   lookups; entity bytes are fetched by position/length from `data.pbs`.
4. **Background.** `SnapshotCreationPlugin` (run by
   `DatasetUpdateConsumingService`) writes `snapshots/{version}/…` and
   `SnapshotInfo.pb`; `StorageCleaningService` deletes old snapshots/deltas per
   `CleanupConfig`, respecting `ProcessedVersions.pb`;
   `DistributedLockService` keeps a single writer across replicas via
   `lock-file` / `request-lock` objects.

### Layout

```
src/                         ← packable libraries (NuGet)
├── RxDatasets.Abstractions/   ← public interfaces: IDatasetReader/Writer, IStorageProvider, IDiskStorage, consumers
├── RxDatasets/                ← core: DtoServerBuilder, reader/writer, ObjectStorageProvider, cache, background services, protos
│   └── diskFormat.md          ← storage format specification (contract)
├── RxDatasets.StorageProvider.{S3,Minio,AzBlob,GoogleCloud,LocalDisk}/  ← IDiskStorage<T> backends + With*Storage()
├── RxDatasets.InMemoryDataset/  ← in-memory IDatasetReader/Writer
├── RxDatasets.Aliasing/       ← alias-based reader wrapper (see its README.md)
├── RxDatasets.DatasetWatcher/ ← watches a dataset's changes from another process
└── RxDatasets.Reporting/      ← writes changed entities as JSON to object storage
tst/                         ← xunit test projects (+ an in-memory IDiskStorage used by tests and Testserver)
testapps/                    ← demo DTO protos, gRPC servers, hosts, benchmarks, test client
end-to-end-tests/            ← docker-compose e2e: two hosts + MinIO, compared with grpcurl; expected MinIO data
specs/                       ← spec-kit feature specs/plans (historical design docs)
.specify/  .github/prompts/  ← spec-kit tooling and Copilot prompts
.vscode/                     ← launch/tasks for the Host test app
Dockerfile                   ← multi-stage build: restore, build, test, pack, nuget push, test/benchmark images
sonar.Dockerfile             ← SonarQube scan + SAST report stages
compose.yml                  ← local Host + MinIO
.gitlab-ci.yml               ← CI pipeline
Directory.Build.props / Directory.Packages.props ← shared package metadata / central package versions
```

### Data flow

1. Client → host gRPC service → `IDatasetWriter<T>.Put*/Delete*`
   (`DatasetWriter.EnqueueAndAwait` → `DrainAndFlush`; batched when write
   coalescing is on, serial otherwise).
2. Writer → `IStorageProvider<T>.StoreDelta(version, index, data)` →
   `IDiskStorage<T>.UploadBytes` / `UploadFileStream` → `DeltaInfo.pb`.
3. `NotificationsHub<T>` → `DatasetUpdatedNotification(version)` → readers
   apply the delta; `DatasetUpdateConsumingService` wakes its consumer.
4. Consumer (`SnapshotCreationPlugin`, `DatasetUpdateReportingPlugin`, …)
   processes the new deltas; `DatasetUpdateReportingPlugin` records its progress
   with `StoreProcessedVersion(who, version)` in `ProcessedVersions.pb`.
5. `StorageCleaningService` reads `GetLowestProcessedVersion()` and calls
   `IStorageProvider.DeleteOlderThan(version, keepAllDeltas)` up to the oldest
   snapshot it keeps; it skips the run while any recorded consumer is behind
   that snapshot.
6. Client → host gRPC reader → `GrpcReaderServiceImpl<T>` → `IDatasetReader<T>`
   → `IStorageReader` reads index/data ranges (served from cache when enabled).

### Key concepts

- **Two storage layers.** `IDiskStorage<T>` is the raw blob API each backend
  implements; `IStorageProvider<T>` is the dataset-level delta/snapshot store
  built on it. Backends never know about deltas.
- **Dataset version.** One monotonically increasing `ulong` per delta; a
  snapshot carries the version of the delta it was built at. `WriteResponse`
  returns it, and `GetChangedNames(fromVersion)` is keyed on it.
- **Entity names are paths.** `collection/id/…` names form the
  parent/children hierarchy used by `List`, `CountChildren`, `DeleteChildren`
  and the tree index (`Models/IndexKeys.cs`). Validate names via the writer.
- **Single writer.** Writes require the distributed lock when it is enabled
  (`ObjectStorageProvider.ThrowIfNoLockIsAcquired`); readers can run anywhere.
- **Format compatibility.** Readers accept flat and tree indexes; writers emit
  only the tree format. Every process must be upgraded before the first
  tree-format write — `diskFormat.md` has the rollout rule.
- **Readiness vs health.** Each dataset registers a readiness check (tagged
  with the tags passed to `Build`) and a health check, named with
  `VersionedEntityName` (e.g. `DemodtoV1`).
- Domain vocabulary: `.ai-local/terminology/`.

### Output contract

The on-storage format is defined by `src/RxDatasets/Models/DatasetEntities.proto`
(and `Legacy-Entities.proto`) and specified in `src/RxDatasets/diskFormat.md`.
A change to either changes what other processes and older library versions
read: update `diskFormat.md`, the readers' backward-compatible parsing, and the
e2e expected data (`end-to-end-tests/generate-expected-data/`) in the same change.

### Deployment

- `.gitlab-ci.yml`: restore → build → test (`dotnet test` in the `test` Docker
  stage, plus `e2e test` via `end-to-end-tests/run.sh`) → on tags: `dotnet pack`
  and `dotnet nuget push` (`TARGET_NUGET`, `TARGET_NUGET_APIKEY`) →
  `sonarqube-check` (currently `allow_failure: true`: the Sonar host is gone).
- Version comes from the git tag (`-p:VERSION=…`); untagged builds use `0.0.0-cibuild`.
- Docker images for the test server and benchmarks are built from the root
  `Dockerfile` (`final-*` stages); nothing in this repo deploys a service.

### Running locally

```
dotnet restore
dotnet build
dotnet test                                                  # needs Docker for Testcontainers tests
dotnet test --filter "FullyQualifiedName!~WithCustomTestContainersImplementation"   # skip external-provider tests
docker compose up --build                                    # Host on :10080 (gRPC) / :18080 (grpc-web, /healthz, /readiness) + MinIO
cd end-to-end-tests && ./run.sh                              # e2e suite (bash, Docker)
```

### Known gaps and traps

- The root `CLAUDE.md` says storage providers implement `IStorageProvider`;
  they implement `IDiskStorage<T>` (`src/RxDatasets.StorageProvider.*/*Storage.cs`).
- `Dockerfile` stages `publish-benchmark` / `publish-reader-benchmark` publish
  `testapps/DtoServerBenchmark` and `testapps/DtoServerReaderBenchmark`, which do
  not exist (the folders are `RxDatasetsBenchmark`, `RxDatasetsReaderBenchmark`).
  CI does not build those stages.
- Microsoft.Extensions.* packages are pinned to 9.0.8 while the target is
  .NET 10 (`Directory.Packages.props`); Renovate is disabled (`renovate.json`).
- `src/RxDatasets.Reporting.Cqs/`, `tst/RxDatasets.Reporting.Cqs.Tests/` and
  `testapps/DTOs/Demodto2V1.Server/` hold only stale `bin/obj` output from removed projects.
- Two design-doc systems coexist: spec-kit (`specs/`, `.specify/`,
  `.github/prompts/`) and dot-ai (`docs/`).
- `src/RxDatasets/diskFormat.md` lags the code (e.g. it says `snapshot/`, the
  code writes `snapshots/`; old proto field names). The `.proto` files and
  `DatasetHelper` are the truth.
- `AddValidatedConfigSection` (`src/RxDatasets.Abstractions/ConfigExtensions.cs`)
  prints every bound config value, secrets included, to stdout.
- `dotnet test` needs Docker; the `WithCustomTestContainersImplementation` filter
  does not exclude every Docker test (`tst/AGENTS.md`).
- Full list of findings from the setup: `docs/2026-10-22-setup-open-tasks.md`.
