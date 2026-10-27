# 11. Testing

There are several ways to test code that uses RxDatasets, from a plain object in a unit test to a full server against
a storage emulator.

## Unit tests: InMemoryDataset

Package `RxDatasets.InMemoryDataset`. `InMemoryDataset<T>` is a reader and a writer in one object, in memory, ready at
once:

```csharp
var dataset = new InMemoryDataset<Order>();
await dataset.Put("t/acme/s/site01/orders/o-1", order.ToByteArray(), "test", null);
var hit = await dataset.Get("t/acme/s/site01/orders/o-1");
```

- It implements `IDatasetReader<T>`, `IDatasetWriter<T>` and `IAliasIndex`, and checks aliases like the real writer.
- `ObserveDatasetChanges(fromVersion)` gives an `IObservable` of new versions.
- History isn't implemented: `GetHistory`, `StreamHistory` and `TailHistory` throw `NotImplementedException`.
- Register it for code that takes the interfaces with `RxDatasets.InMemoryDataset.DI.ConfigureServices<T>(services)`,
  which also picks up alias definitions registered with `AddDatasetAliases<T>`.

## Host tests: WithInMemoryDataset

`WithInMemoryDataset()` on the builder keeps the real reader, writer and gRPC services, on an in-memory store:

```csharp
services.AddDtoServer<Order>().WithInMemoryDataset().WithSharedReaderWriter().Build("readiness");
```

Use it to test your gRPC services with `WebApplicationFactory` or an in-process test server. It has no lock,
snapshots, cleaning or migrations, and starts at version 1 every time.

## The full stack in memory

`LowLevelInMemoryStorage<T>` (in `tst/RxDatasets.StorageProvider.InMemoryProvider`; not a package, copy it) is an
in-memory blob store with compare-and-swap. Register it as the storage, and the whole stack runs on it: the object
storage provider, deltas, snapshots, cleaning and the lock.

```csharp
var storage = new LowLevelInMemoryStorage<Order>();
services.AddSingleton<IDiskStorage<Order>>(storage);
services.AddDtoServer<Order>().WithDistributedLock().WithSnapshot().Build("readiness");
```

Give two hosts the same storage object to test handovers and the fenced commit without any infrastructure.

## A temporary folder

The closest to production without containers: local disk in a fresh folder per test, configured as in production,
and a wait for readiness before the first call.

```csharp
var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
{
    ["LocalDiskStorage:RootFolder"] = Path.Combine(Path.GetTempPath(), "dataset-" + Guid.NewGuid().ToString("N")),
    ["SnapshotCreation:SnapshotInterval"] = "3",           // exercise snapshots quickly
}).Build();

// ... build and start the host ...

var health = host.Services.GetRequiredService<HealthCheckService>();
for (var i = 0; i < 300; i++)
{
    var report = await health.CheckHealthAsync(c => c.Tags.Contains("readiness"));
    if (report.Status == HealthStatus.Healthy) break;
    await Task.Delay(100);
}
```

Restart the host on the same folder to test that data survives, and that a reporter continues where it stopped.

## Storage emulators

`tst/RxDatasets.StorageTests` has Testcontainers fixtures for every provider:

| Provider | Emulator | Fixture |
|---|---|---|
| Google Cloud Storage | fake-gcs-server | `GoogleCloudContainerInstanceFixture` |
| S3 | LocalStack | `LocalStackS3TestContainerFixture` |
| MinIO | MinIO | `MinioTestContainerFixture` |
| Azure Blob | Azurite (key and OAuth) | `AzuriteContainerInstanceFixture`, `AzuriteOAuthContainerInstanceFixture` |

Point the provider at the emulator with configuration: `GoogleCloud:EmulatorHost`, `S3:ServiceUrl`,
`MinioS3:ServiceURL`, or an Azurite connection string. Use one bucket per test.

For lock tests, use short timings so a handover takes a second or two:

```csharp
["DistributedLock:PollingInterval"] = "00:00:01",
["DistributedLock:HandoverTimeout"] = "00:00:03",
```

and start two hosts on one bucket. The lock and time-based services take an `IClock` and an `IScheduler`, so
tests can also drive time by hand (`RxSchedulerClock` with a test scheduler); `NotificationsHubSync<T>` makes
notifications synchronous.

`ConditionalStorageContract.cs` is the contract test of the compare-and-swap interface. Run it against any storage
provider you write.

## End-to-end tests

Run the server image and a test runner in docker compose:

```yaml
services:
  server:
    image: ${SERVER_IMAGE:-my-server:local}
    environment:
      - Jwt__Disable=true
      - DATAVLOW__DTOS=order,customer      # only the types the tests need: faster startup
  test-runner:
    image: my-test-runner                  # an SDK image with grpcurl and the test scripts
    depends_on: [server]
    command: ["pwsh", "/tests/test.ps1"]
```

```bash
docker compose up --build --exit-code-from test-runner
```

- Wait until `grpcurl list` shows the services before the first test.
- Retry writes and reads while they answer `UNAVAILABLE`: the dataset is still loading.
- Add a compose override that swaps local disk for a storage emulator, to run the same tests against cloud storage.
- The repository's `end-to-end-tests/` compares a fresh server with one started on stored expected data, to catch
  changes in the stored format.

## Sample apps

`testapps/` in this repository:

| App | What it is |
|---|---|
| `Host` | A read/write host on S3 with lock, cache, snapshots, cleaning and a console reporter |
| `ReadOnlyHost` | A read-only replica |
| `Testserver` | A host on `LowLevelInMemoryStorage` |
| `TestClient` | A gRPC and gRPC-Web client |
| `DTOs` | Sample protos and their reader and writer services |
| `RxDatasetsBenchmark`, `RxDatasetsGcsBenchmark`, `RxDatasetsReaderBenchmark` | Benchmarks |

The root `compose.yml` runs `Host` with MinIO.

## Testing your consumers

Services that consume a DataVlow server test against a real server, not a mock: run the
[DataVlow server](10-datavlow-server.md) (or your own server's image) in their docker compose, with JWT off and the
in-memory change queue on. It enforces id patterns, aliases and the queue's fan-out, so the tests see what production
does. Remember that its versions and queues start empty on every start.
