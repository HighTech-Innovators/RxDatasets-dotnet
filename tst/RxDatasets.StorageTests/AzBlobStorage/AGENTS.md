# tst/RxDatasets.StorageTests/AzBlobStorage

Tests for `src/RxDatasets.StorageProvider.AzBlob` outside the spec suite (that runs in `SpecTests/WithCustomTestContainersImplementation/AzStorageDatasetTests.cs`).

## Files

- `AzStorageConfigTests.cs` — `AzStorageConfig` validation: exactly one of `ConnectionString` and `AccountUrl`.
- `AzStorageTests.cs` — the private static `AzStorage<T>.CreateCredential`, called by reflection, with and without `ManagedIdentityClientId`.
- `AzStorageManagedIdentityTests.cs` — `AzStorage<T>` with `AccountUrl` and `DefaultAzureCredential` against Azurite in OAuth mode. Skipped; the class summary has the one-time manual setup.
- `AzuriteOAuthContainerInstanceFixture.cs` — Azurite with `--oauth basic` and a self-signed TLS certificate, kept at `%TEMP%/rxdatasets-azurite-oauth-test-cert.pfx` so it is trusted once.

## Gotchas

- Constructing `AzStorage<T>` starts its initialization loop, which probes the real Azure credential chain over the network. Unit tests here reach credential logic by reflection and leave the constructor to container tests.
- `AzStorageManagedIdentityTests` needs Docker, `az login` and the trusted certificate. It lives outside `WithCustomTestContainersImplementation`, so the Docker filter does not exclude it; its `Skip` keeps it out of CI.
