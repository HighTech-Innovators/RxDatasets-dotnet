# src/RxDatasets.StorageProvider.S3

`IDiskStorage<T>` on AWS S3 or an S3-compatible service through AWSSDK.S3 (v4). Overlaps
[`../RxDatasets.StorageProvider.Minio`](../RxDatasets.StorageProvider.Minio/AGENTS.md); cross-provider differences:
[`../AGENTS.md`](../AGENTS.md).

## Files

- `DI.cs` — `WithS3Storage<TEntity>()`: binds section `S3`, `TryAddSingleton<IDiskStorage<T>, S3Storage<T>>`, health
  check `storage.s3.{T}` (tags `s3` and the entity name).
- `S3Storage.cs` — `S3Storage<T>`: uploads through `TransferUtility`; `CreateS3Exception` logs the full S3 error (code,
  request id, truncated response body) and wraps it in `InvalidOperationException`; explicit `IDisposable`.
- `S3Config.cs` — `AccessKey`, `SecretKey`, `BucketName` (required); `ServiceUrl` or `Region`;
  `RequestChecksumCalculation`/`ResponseChecksumValidation` (default `WHEN_REQUIRED`).
- `RegionOrEndpointRequiredAttribute.cs` — class-level validation: exactly one of `Region`/`ServiceUrl`.
- `S3HealthCheck.cs` — `S3HealthCheck<T>`.
- `RxDatasets.StorageProvider.S3.csproj` — AWSSDK.S3; references core.
- `RxDatasets.StorageProvider.S3.v3.ncrunchproject` — NCrunch settings.

## Gotchas

- `ServiceUrl` mode forces path-style addressing and prepends `http://` when the URL has no scheme; `Region` mode uses
  the AWS regional endpoint. Credentials are always the static access/secret key pair.
- Keep both checksum settings at `WHEN_REQUIRED` for S3-compatible backends: the SDK v4 default `WHEN_SUPPORTED` gets
  400s from some of them, and their ranged GETs fail response validation.
- The bucket is probed with `ListObjectsV2` on prefix `no_need_to_exist`; `NoSuchBucket` leads to `PutBucket`.
- `DeleteFolder` lists every key first, then deletes all of them in parallel without a concurrency limit.
