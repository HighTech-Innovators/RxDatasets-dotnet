# tst/RxDatasets.StorageTests/GoogleCloudStorage

Tests for the write queue and disposal of `GoogleCloudStorage<T>` (`src/RxDatasets.StorageProvider.GoogleCloud`). The GCS spec suite runs in `SpecTests/WithCustomTestContainersImplementation/GoogleCloudStorageDatasetTests.cs`.

## Files

- `WriteQueueIntegrationTests.cs` — the one live test: rapid writes to one key with `enableWriteQueue: true`, then `DisposeAsync` must flush the last one; checked by downloading from the emulator and by verifying the Moq logger's flush messages. Uses `GoogleCloudContainerInstanceFixture`.
- `WriteQueueTests.cs` — rapid writes queued, bypass after two seconds, per-file queues, order. All skipped. Also defines `TestLogger<T>`, a no-op `ILogger<T>` the other skipped files use.
- `ShutdownFlushTests.cs` — `DisposeAsync` flushes queued and in-flight writes. All skipped.
- `DispatcherCleanupTests.cs` — client disposal: `ObjectDisposedException` after dispose, idempotent and timely disposal. All skipped.

## Gotchas

- `WriteQueueIntegrationTests` needs Docker and sits outside `WithCustomTestContainersImplementation`, so the filter that drops container tests still runs it.
- The 15 skipped tests say "requires GCS emulator" and construct the storage with only a bucket name; nothing points them at `GoogleCloudContainerInstanceFixture`.
- `WriteQueueIntegrationTests` and `GoogleCloudStorageDatasetTests` both set and clear `STORAGE_EMULATOR_HOST`, `GOOGLE_APPLICATION_CREDENTIALS` and `GCLOUD_PROJECT`, and run in parallel. Pass the emulator host through `GoogleCloudConfig.EmulatorHost`, as both do, rather than through the variable.
