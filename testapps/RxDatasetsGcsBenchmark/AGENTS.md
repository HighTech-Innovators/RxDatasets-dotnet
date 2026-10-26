# testapps/RxDatasetsGcsBenchmark

Console benchmark of parallel `Put` throughput straight against Google Cloud Storage, in process (no gRPC, no host).

## Files

- `Program.cs` — top-level statements. Args: `--bucket <b> --project <p> [--count 100] [--parallelism 10] [--prefix benchmark] [--disable-queue]`. Wires `GoogleCloudStorage` → `ObjectStorageReader` → `ObjectStorageProvider` → `DatasetWriter` by hand (no DI) with write coalescing on, writes `count` `BenchmarkDto` records and prints total and effective throughput. Also defines `BenchmarkDto` and `NotificationsHubSync<T>`, a minimal `INotificationsHub<T>`.
- `RxDatasetsGcsBenchmark.csproj` — exe; references `src/RxDatasets` and `src/RxDatasets.StorageProvider.GoogleCloud`.

## Gotchas

- Writes to a real bucket under `--prefix` and never cleans up (the delete call is commented out). GCS credentials come from the provider's default lookup (unverified which).
- `BenchmarkDto` is a C# record stored as UTF-8 of `ToString()`, not protobuf; fine for throughput, not a readable dataset.
- The hand wiring calls the concrete constructors of `ObjectStorageProvider`, `ObjectStorageReader`, `DatasetWriter` and `GoogleCloudStorage`; a constructor change in `src/` must be mirrored here.
- The usage text says `--count <1000>`; the code defaults to 100.
