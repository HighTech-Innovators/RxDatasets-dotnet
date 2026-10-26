# src/RxDatasets.StorageProvider.AzBlob

`IDiskStorage<T>` on one Azure Blob container. Cross-provider differences: [`../AGENTS.md`](../AGENTS.md).

## Files

- `DI.cs` — `WithAzureStorage<TEntity>()`: binds section `AzStorage`, `AddSingleton<IDiskStorage<T>, AzStorage<T>>`,
  health check `storage.azblob.{T}`.
- `AzStorage.cs` — `AzStorage<T>`: creates the container if missing on construction, retrying every 5 s; Azure SDK
  retry policy of 2 retries; `LastResult` feeds the health check.
- `AzStorageConfig.cs` — `ConnectionString` or `AccountUrl` (with optional `ManagedIdentityClientId`), and the required
  `ContainerName`.
- `ConnectionStringOrAccountUrlRequiredAttribute.cs` — class-level validation: exactly one of the two.
- `AzHealthCheck.cs` — `AzHealthCheck<T>`.
- `RxDatasets.StorageProvider.AzBlob.csproj` — Azure.Identity, Azure.Storage.Blobs; references core.
- `RxDatasets.StorageProvider.AzBlob.v3.ncrunchproject` — NCrunch settings.

## Gotchas

- Two auth modes: `AccountUrl` uses `DefaultAzureCredential` (whatever identity that chain finds on the host);
  `ConnectionString` uses the credentials inside it. Validation rejects setting both.
- Initialization runs in an `async void` loop started by the constructor; a call made before it succeeds waits about
  1 s, then throws `InvalidOperationException("Azure Blob Storage not yet initialized")`.
- Initialization failures are logged as "Could not init S3" (copied from the S3 provider).
- `ListObjects` is a flat prefix listing, sorted by key, with `LastModified` in UTC.
- `DeleteFile` and `DeleteFolder` delete blob snapshots too.
