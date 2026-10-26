# tst/RxDatasets.StorageTests/SpecTests/WithCustomTestContainersImplementation

Spec suites on real storage providers, mostly in Testcontainers. The namespace segment `WithCustomTestContainersImplementation` is the switch that excludes them.

## Files

- `AzStorageDatasetTests.cs` — `StorageTestSpecWithTwoStepInit` on `AzStorage<object>` in Azurite (`AzuriteContainerInstanceFixture`), plus put errors while the container is stopped.
- `GoogleCloudStorageDatasetTests.cs` — the same on `GoogleCloudStorage<object>` (write queue off) in fake-gcs-server (`GoogleCloudContainerInstanceFixture`); creates the bucket per test through the emulator.
- `MinIOS3StorageDatasetTests.cs` — the same on `MinioS3Storage<object>` in MinIO (`MinioTestContainerFixture`), plus `WriterConfig.AllowedNamesPattern` validation.
- `S3StorageDatasetTests.cs` — the same on `S3Storage<object>` in LocalStack (`LocalStackS3TestContainerFixture`), plus `DeleteOlderThan` on stored deltas and snapshots, and a skipped 3000-item listing test.
- `LocalDiskTests.cs` — class `LocalDiskDatasetTests`: the same on `LocalDiskStorage<object>` under `DatasetHelper.GetTempFileDirectory()/UnitTestsData_localdisk_Tests/`. Needs no container.
- `E2eTestsUsingTestHostWithMinio.cs` — class `E2ETestsUsingTestHostWithMinio`: the gRPC test server (`GrpcWebApplicationFactory`) on `MinioS3Storage<Demodto>` in MinIO: health with two hosts, raw snapshot data read from the bucket, write then read across a host restart.

## Gotchas

- The root `Dockerfile` (`test` stage) and `sonar.Dockerfile` run `dotnet test --filter "FullyQualifiedName!~WithCustomTestContainersImplementation"` when the build argument `RunExternalProvidersTests` is `false`. The `Dockerfile` defaults it to `true`, `sonar.Dockerfile` to `false`, and `.gitlab-ci.yml` passes `true` to both, so CI runs these tests with Docker reached through `DOCKER_HOST`. Locally, without Docker, apply the same filter.
- The filter matches the namespace, so a container test belongs in this folder and namespace; `LocalDiskDatasetTests` sits here and is excluded with them although it needs no Docker.
