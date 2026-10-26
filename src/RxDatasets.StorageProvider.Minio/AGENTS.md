# src/RxDatasets.StorageProvider.Minio

`IDiskStorage<T>` on an S3-compatible bucket (MinIO, or AWS S3 by region) through the Minio .NET SDK. Overlaps
[`../RxDatasets.StorageProvider.S3`](../RxDatasets.StorageProvider.S3/AGENTS.md); cross-provider differences:
[`../AGENTS.md`](../AGENTS.md).

## Files

- `DI.cs` — `WithMinioStorage<TEntity>()`: binds section `MinioS3`, `AddSingleton<IDiskStorage<T>, MinioS3Storage<T>>`,
  health check `storage.minio.{T}`.
- `MinioS3Storage.cs` — `MinioS3Storage<T>`: creates the bucket if missing; internal `MinioExceptionExt.IsTransient`
  decides which errors become `InvalidOperationException`.
- `MinioS3Config.cs` — `AccessKey`, `SecretKey`, `BucketName` (required); `ServiceURL` or `AwsRegion`.
- `RegionOrEndpointRequiredAttribute.cs` — class-level validation: exactly one of `AwsRegion`/`ServiceURL`.
- `MinioS3HealthCheck.cs` — `MinioS3HealthCheck<T>`.
- `RxDatasets.StorageProvider.Minio.csproj` — Minio; references core.
- `RxDatasets.StorageProvider.Minio.v3.ncrunchproject` — NCrunch settings.

## Gotchas

- `AwsRegion` set: endpoint `{BucketName}.s3.{AwsRegion}.amazonaws.com` with `WithSSL()`. Otherwise `ServiceURL`, without `WithSSL()`.
- `UploadBytes` deletes the object before putting the new one; a reader in between gets `FileNotFoundException` (for
  example on `DeltaInfo.pb`). `UploadFileStream` overwrites in place.
- Only transient errors are wrapped and recorded for the health check; other `MinioException`s from upload, delete and
  list propagate raw and leave `LastResult` unchanged. `DownloadWithCallBack` never updates `LastResult`.
- Initialization runs in an `async void` loop; a call made before it succeeds waits about 1 s, then throws.
- `ListObjects` is recursive and sorted by key.
