# src/RxDatasets.StorageProvider.GoogleCloud

`IDiskStorage<T>` on one Google Cloud Storage bucket, with a per-object write queue for GCS's limit of about one
mutation per second per object. Cross-provider differences: [`../AGENTS.md`](../AGENTS.md).

## Files

- `DI.cs` — `WithGoogleCloudStorage<TEntity>()`: binds section `GoogleCloud`,
  `TryAddSingleton<IDiskStorage<T>, GoogleCloudStorage<T>>`, health check `storage.googlecloud.{T}` (tags
  `googlecloud` and the entity name).
- `GoogleCloudStorage.cs` — `GoogleCloudStorage<T>`: retries a rate-limited upload (429 or "rate limit for object
  mutation") 5 times, 1.1 s apart; `FlushAsync`, `DisposeAsync` and `Dispose` flush the queue. A constructor overload
  takes an `IClock` for tests.
- `GoogleCloudConfig.cs` — `BucketName` (required), `ProjectId`, `JsonCredentials`, `EmulatorHost`, `EnableWriteQueue`
  (default true).
- `WriteQueueManager.cs` — internal: per-object queues; `TryQueueWriteAsync`, `MarkSuccessfulWriteAsync`, `FlushAsync`,
  `FlushAndDisposeAsync`.
- `FileWriteQueueState.cs` — internal per-object state: pending write, last successful write, scheduled flush.
- `QueuedWrite.cs` — internal queued payload.
- `GoogleCloudHealthCheck.cs` — `GoogleCloudHealthCheck<T>`.
- `RxDatasets.StorageProvider.GoogleCloud.csproj` — Google.Cloud.Storage.V1; references core.

## Gotchas

- Write queue: `UploadBytes` to a key written successfully under 2 s ago returns at once without writing. Only the
  latest pending bytes survive, and they are written when the 2 s window ends; readers see the old object until then.
  `UploadFileStream` bypasses the queue. Core leans on this: `DistributedLockService` calls `FlushAsync()` before it
  releases the lock, and `ObjectStorageReader` probes forward past a stale `DeltaInfo.pb`.
- `DeleteFile` leaves a pending queued write for the same key in place; its flush recreates the object.
- A failed queued flush is only logged; the caller was already told the write succeeded.
- Emulator mode (`EmulatorHost`, else env `STORAGE_EMULATOR_HOST`): `http://{host}/storage/v1/`, unauthenticated,
  ranged downloads skip hash validation. Otherwise `JsonCredentials`, else Application Default Credentials.
- A missing bucket is created only when `ProjectId` or env `GOOGLE_PROJECT_ID` is set, at location `eu-north-1` (an
  AWS-style region name; unverified that GCS accepts it).
- When the non-retrying constructor's initialization faults, later calls rethrow that fault.
