# tst/RxDatasets.StorageTests

Integration tests of the storage stack: `ObjectStorageProvider`, `DatasetReader`/`DatasetWriter`/`DatasetReaderWriter`, background services, `DtoServerBuilder` hosts, the gRPC test server, and every storage provider. Entities are raw `object` bytes or `Demodto` (from `testapps/DTOs`). Mocking library: Moq (`Mock.Of<ILogger<T>>()` for loggers).

## Files

- `AliasTestHost.cs` (new, uncommitted) — `AliasTestHost` and `AliasHostOptions`: a `DtoServerBuilder<Demodto>` host with `AddDatasetAliases` on a caller-owned `IDiskStorage`, so a test can restart on the same data; plus `Eventually`, `Puts`, `Changes`, `Entity` helpers.
- `AliasHostTests.cs` — `AddDatasetAliases` through `DtoServerBuilder`, with and without `WithSharedReaderWriter`: a conflicting write throws `AliasConflictException`.
- `AliasHostRestartTests.cs` (new, uncommitted) — aliases across a restart on `AliasTestHost`: same definitions, changed definitions (one rebuild delta), none, after a snapshot, after cleanup.
- `AliasSeparateReaderHostTests.cs` (new, uncommitted) — aliases with a separate `DatasetReader`, which catches up in the background.
- `AliasSharedReaderWriterHostTests.cs` (new, uncommitted) — aliases through the shared reader/writer, readable as soon as the write returns. `AliasSharedReaderWriterCoalescingHostTests` reruns every test with write coalescing. Defines `GatedStorage`, an `IDiskStorage` decorator that can hold an upload.
- `SharedReaderWriterHostTests.cs` — read-after-write through `WithSharedReaderWriter` (`Get`, `List`, `CountChildren`, `WaitChangedNames`), 200 iterations each.
- `CacheStorageProviderTest.cs` — `CacheStorageProvider` cache hits over a mocked `IStorageProvider<object>`.
- `E2eTestsUsingTestHost.cs` — `E2ETestsUsingTestHost`: gRPC writer/reader services, health, distributed lock between two hosts, `PutOrDeleteMany`, `CountChildren`, via `GrpcWebApplicationFactory` on `LowLevelInMemoryStorage`.
- `GrpcWebApplicationFactory.cs` — `WebApplicationFactory<Program>` for `testapps/Testserver` (global namespace), environment `Testing`.
- `AzuriteContainerInstanceFixture.cs` — Azurite container for `AzStorageDatasetTests`.
- `GoogleCloudContainerInstanceFixture.cs` — `fsouza/fake-gcs-server` on a random free port; host taken from `DOCKER_HOST`.
- `LocalStackS3TestContainerFixture.cs` — LocalStack 4.4.0 with S3, on its own network.
- `MinioTestContainerFixture.cs` — MinIO (`pgsty/minio`) on its own network.
- `DemodtoHostContainerFixture.cs` — builds the root `Dockerfile` target `final-host` with the docker CLI and starts it against MinIO. No test uses it.
- `RxDatasets.StorageTests.csproj` — project file; references all providers, `testapps` DTO server and Testserver, and the InMemoryProvider helper.
- `RxDatasets.StorageTests.v3.ncrunchproject` — NCrunch settings; ignores the five container fixtures in `SpecTests/WithCustomTestContainersImplementation`.

## Folders

- `AzBlobStorage/` — Azure Blob config, credential and managed-identity tests. See [`AzBlobStorage/AGENTS.md`](AzBlobStorage/AGENTS.md).
- `Common/` — the abstract spec suite and `DatasetHelper` tests. See [`Common/AGENTS.md`](Common/AGENTS.md).
- `GoogleCloudStorage/` — GCS write queue and disposal. See [`GoogleCloudStorage/AGENTS.md`](GoogleCloudStorage/AGENTS.md).
- `Services/` — background services, watcher, first-class aliases. See [`Services/AGENTS.md`](Services/AGENTS.md).
- `SpecTests/` — concrete spec suites per provider. See [`SpecTests/AGENTS.md`](SpecTests/AGENTS.md).

## Gotchas

- Docker is needed by `SpecTests/WithCustomTestContainersImplementation` (see its `AGENTS.md` for the filter that excludes it) and by `GoogleCloudStorage/WriteQueueIntegrationTests`, which that filter does not exclude. Every image except LocalStack is `:latest` with `PullPolicy.Always`, so each run pulls.
- Container fixtures only build the container. Each test's `InitializeAsync` starts it when it is not running, so a test that stops the shared container leaves the restart to the next test. The fixtures' own `IClassFixture<T>` declarations do nothing; only `LocalStackS3TestContainerFixture` implements `IAsyncLifetime`.
- `NotificationsHubSync` makes a write return only after its consumers (snapshot creation, readers) handled it, so tests built on it assert at once. Hosts (`AliasTestHost`, `SharedReaderWriterHostTests`, the E2E tests) use the async hub of a real server and poll (`Eventually`, `/healthz`).
- In a host test, resolve `IDatasetReader`, `IDatasetWriter` and alias readers before `StartAsync`; they pick up the storage readiness notification sent during the start.
- This project runs on `Microsoft.AspNetCore.App`, where `BackgroundService.ExecuteAsync` runs after `StartAsync` returns (.NET 10), as on a real server. Tests of start ordering belong here, not in `RxDatasets.Tests` (see `Services/DatasetUpdateConsumingServiceStartTests.cs`).
- Process-wide environment variables leak between parallel classes: `E2ETestsUsingTestHost` sets `SnapshotCreation__SnapshotInterval=5`, `E2ETestsUsingTestHostWithMinio` sets `MINIOS3__*`, `S3__*` and `Storage__CustomRootFolder`, `Common/DatasetHelperTests` sets `TEMP_FILE_DIRECTORY`; none clear them.
- For a new alias host test, start from `AliasTestHost`.
