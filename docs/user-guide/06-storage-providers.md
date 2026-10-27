# 6. Storage providers

A storage provider is where a dataset's files live. Every built-in provider supports compare-and-swap writes
(`IConditionalDiskStorage<T>`), so all of them work with the distributed lock and the fenced delta commit.

| Provider | Register | Section | Best for |
|---|---|---|---|
| Local disk | `WithLocalDiskStorage()` | `LocalDiskStorage` | One instance with its own disk; development; a shared disk on one machine |
| Google Cloud Storage | `WithGoogleCloudStorage()` | `GoogleCloud` | Production on Google Cloud |
| S3 | `WithS3Storage()` | `S3` | AWS, and S3-compatible stores (MinIO, LocalStack, ...) |
| Azure Blob Storage | `WithAzureStorage()` | `AzStorage` | Production on Azure |
| MinIO | `WithMinioStorage()` | `MinioS3` | Older setups; prefer `WithS3Storage()` against MinIO |
| In memory | `WithInMemoryDataset()` | none | Tests and development |

Choose one per dataset. The cloud providers create their bucket or container when it doesn't exist, retry their
initialization in the background, and register a health check. Local disk does neither.

## Local disk

```csharp
services.AddDtoServer<Order>().WithLocalDiskStorage().Build();
```

```json
{ "LocalDiskStorage": { "RootFolder": "/var/lib/datasets" } }
```

- `RootFolder` defaults to `dataset-localstorage`, relative to the working directory. In a container, point it at a
  volume (`VOLUME /data`, `LocalDiskStorage__RootFolder=/data`).
- Plain writes go to a temporary file that then replaces the target, so a reader never sees half a file.
- Compare-and-swap works across processes **on one machine**: create-only uses an atomic move on Windows and a hard
  link on Linux, and a replace holds an exclusive file handle while it compares. Across machines on a network share
  (NFS, SMB) these guarantees depend on the share; one server family runs active/passive hosts on NFS with the lock
  on, and relies on only one host running at a time.
- The version token is a SHA-256 of the content.
- No health check is registered.
- Data on a container's own disk disappears with the container. That is fine for a test server, and wrong for
  anything else.

## Google Cloud Storage

```csharp
services.AddDtoServer<Order>().WithGoogleCloudStorage().WithCache().Build();
```

```json
{ "GoogleCloud": { "BucketName": "my-datasets", "ProjectId": "my-project" } }
```

| Key | Meaning |
|---|---|
| `BucketName` | Required. |
| `ProjectId` | Needed only to create a missing bucket (or set `GOOGLE_PROJECT_ID`). |
| `JsonCredentials` | A service account key as JSON. Leave it empty to use application default credentials (workload identity, `gcloud auth application-default login`), which is better. |
| `EmulatorHost` | `host:port` of an emulator such as fake-gcs-server. `STORAGE_EMULATOR_HOST` works too. The emulator is used without authentication. |
| `EnableWriteQueue` | Default `true`. Merges writes to the same object that arrive within 2 seconds, to stay under Cloud Storage's limit of about one write per second per object; retries rate-limit errors (HTTP 429). Conditional writes are never queued. |

- Compare-and-swap uses the object generation (`ifGenerationMatch`).
- A call made before the client has initialized waits for it for up to about 8 minutes, then fails.
- Queued writes are flushed when the dataset stops. A host that disposes storage itself should call `FlushAsync()`
  first.
- A missing bucket is created in a fixed location. Create buckets yourself, in the region you want.
- Health check: `storage.googlecloud.{type}`, tagged `googlecloud`.

## S3

```csharp
services.AddDtoServer<Order>().WithS3Storage().WithCache().Build();
```

```json
{ "S3": { "BucketName": "my-datasets", "Region": "eu-west-1", "AccessKey": "...", "SecretKey": "..." } }
```

| Key | Meaning |
|---|---|
| `BucketName` | Required. |
| `AccessKey`, `SecretKey` | Static credentials. |
| `Region` | For AWS. Set this or `ServiceUrl`, not both. |
| `ServiceUrl` | For an S3-compatible store: `minio:9000`, `http://localstack:4566`. Without a scheme, `http://` is assumed. Path-style addressing is used. |
| `RequestChecksumCalculation`, `ResponseChecksumValidation` | Default `WHEN_REQUIRED`, which S3-compatible stores need. `WHEN_SUPPORTED` turns on the AWS SDK's newer checksums. |

- Compare-and-swap uses ETags with `If-Match` and `If-None-Match: *`. **The store must support conditional writes.**
  AWS S3 does; for an S3-compatible store, check before you turn the lock on. A recent MinIO does, also for large
  objects, which makes `WithS3Storage()` the better choice for MinIO.
- A call made before the client has initialized waits for it for up to about 8 minutes, then fails.
- Health check: `storage.s3.{type}`, tagged `s3`.

## Azure Blob Storage

```csharp
services.AddDtoServer<Order>().WithAzureStorage().WithCache().Build();
```

```json
{ "AzStorage": { "ContainerName": "datasets", "AccountUrl": "https://myaccount.blob.core.windows.net" } }
```

| Key | Meaning |
|---|---|
| `ContainerName` | Required. |
| `ConnectionString` | A connection string with a key or SAS. Set this or `AccountUrl`. |
| `AccountUrl` | Uses `DefaultAzureCredential`: managed identity, workload identity, environment, Azure CLI. |
| `ManagedIdentityClientId` | The client id of a user-assigned managed identity. |

- Compare-and-swap uses ETags.
- For local development use Azurite with its connection string.
- Health check: `storage.azblob.{type}`.

## MinIO

```csharp
services.AddDtoServer<Order>().WithMinioStorage().Build();
```

```json
{ "MinioS3": { "BucketName": "datasets", "ServiceURL": "minio:9000", "AccessKey": "...", "SecretKey": "..." } }
```

- `ServiceURL` connects over plain HTTP. With `AwsRegion` instead, it connects to AWS over HTTPS.
- **Caveat:** the MinIO SDK uploads objects of 5 MiB and more in parts, and drops the write condition for them.
  A conditional replace of such an object throws `NotSupportedException`, and a create-only write becomes
  check-then-write, which two writers can race. Lock files are small, so the lock works, but a delta of 5 MiB or
  more isn't fenced. See [the fenced delta commit](../distributed-lock.md#fenced-delta-commit).
- Prefer `WithS3Storage()` with `S3:ServiceUrl` pointing at MinIO. It has none of these limits.
- Health check: `storage.minio.{type}`.

## In memory

```csharp
services.AddDtoServer<Order>().WithInMemoryDataset().Build();
```

Keeps the dataset in memory, with the real reader and writer on top. It is ready at once, and ignores the lock,
snapshots, cleaning and migrations. **Every restart starts again at version 1**, so clients that stored a version
from before the restart are wrong afterwards. See [Testing](11-testing.md) for the other in-memory options.

## The cache

`WithCache()` puts an in-memory cache in front of any provider. It holds the index files of deltas and snapshots and
the byte ranges of records, up to `DtoStorageCache:CacheSizeMB` per dataset, and preloads the latest snapshot when
the dataset becomes ready. On cloud storage it turns most reads into memory reads; use it there. A read-only replica
gets its own cache.

## Bucket layout

Several datasets, and several environments, can share a bucket:

```
my-datasets/
  prod/                   <- Storage:CustomRootFolder
    Order/                <- the type name, or Storage:DtoName
      DeltaInfo.pb
      SnapshotInfo.pb
      ProcessedVersions.pb
      lock-file           <- with a distributed lock
      request-lock
      deltas/1/index.pb
      deltas/1/data.pbs
      snapshots/100/index.pb
      snapshots/100/data.pbs
    Customer/
  test/
    Order/
```

Don't give two datasets the same folder, and don't let a shared lock's folder (`AddSharedDistributedLock(lockFolder)`)
collide with a dataset's folder.

## Writing your own provider

A provider implements `IConditionalDiskStorage<T>`:

```csharp
public interface IDiskStorage<T>
{
    Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey);                     // FileNotFoundException if absent
    Task<ReadOnlyMemory<byte>> DownloadBytes(string objectKey, long position, int length);
    Task DownloadWithCallBack(string objectKey, Action<Stream> callback);
    Task UploadBytes(string objectKey, byte[] bytes);
    Task UploadFileStream(string objectKey, Stream stream);
    Task DeleteFile(string objectKey);
    Task DeleteFolder(string prefix);
    Task<List<(string ObjectKey, DateTimeOffset DateTime)>> ListObjects(string prefix);
    Task<long> GetObjectContentLength(string objectKey);
    Task FlushAsync() => Task.CompletedTask;
}

public interface IConditionalDiskStorage<T> : IDiskStorage<T>
{
    // The bytes and an opaque version token (generation, ETag, hash).
    Task<(ReadOnlyMemory<byte> Bytes, string Version)> DownloadBytesWithVersion(string objectKey);
    // Writes only if the stored version is expectedVersion (null: only if the object doesn't exist).
    // Returns the new version, or null when the condition failed.
    Task<string?> TryUploadBytes(string objectKey, byte[] bytes, string? expectedVersion);
    // Creates the object only if it doesn't exist. False when it already existed.
    Task<bool> TryCreateFromStream(string objectKey, Stream stream);
}
```

Register it instead of a `With...Storage()` call:

```csharp
services.AddSingleton<IDiskStorage<Order>, MyStorage<Order>>();
services.AddDtoServer<Order>().WithCache().Build();
```

(If `MyStorage<Order>` implements the conditional interface, the engine finds it through the registered
`IDiskStorage<Order>`.)

Throw `InvalidOperationException` when the storage is unavailable; the gRPC layer turns it into `UNAVAILABLE`. A
condition that fails is not an error: return null or false. When the outcome of a call is unknown (a timeout after
sending), don't guess: read the object back, or throw. The engine recovers a write whose response was lost.

**Run the contract tests.** `tst/RxDatasets.StorageTests/ConditionalStorageContract.cs` is a shared contract test
for the compare-and-swap interface: create-only, replace with the right and the wrong version, races, and large
objects. Run it against your provider before you use it with a lock. A storage that only implements `IDiskStorage<T>`
still works without a lock, but its deltas aren't fenced, and `WithDistributedLock()` fails at startup.
