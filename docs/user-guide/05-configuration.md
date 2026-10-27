# 5. Configuration reference

RxDatasets reads standard .NET configuration: `appsettings.json`, `appsettings.{Environment}.json`, environment
variables and command-line arguments.

## How sections work

- Each options class binds to a section named after the class without `Config`: `StorageConfig` is `Storage`,
  `DtoStorageCacheConfig` is `DtoStorageCache`. A few use other names (`DistributedLockOptions` is `DistributedLock`).
- A section is only read when the builder registers it. `Cleanup` does nothing without `WithCleanup()`, and
  `SnapshotCreation` nothing without `WithSnapshot()`.
- Sections are validated when the host starts (data annotations). An invalid value stops the host with a message
  that names the key.
- At startup each section prints the keys it bound (never the values) to standard output, so you can see what was
  picked up.
- In environment variables, `__` separates the section and the key, and case doesn't matter:
  `Storage__CustomRootFolder=prod/`, `SNAPSHOTCREATION__SNAPSHOTINTERVAL=500`.
- **A section is shared by every dataset in the service collection.** Two `AddDtoServer<T>()` calls in one host read
  the same `Storage`, `SnapshotCreation`, `Cleanup`, `DtoStorageCache`, `Writer`, `DistributedLock` and provider
  section. For different settings per dataset, build a service collection per dataset.

Watch the quotes in docker compose. In the list form (`- Storage__CustomRootFolder="data/"`) the quotes become part
of the value, and validation rejects it. Write `- Storage__CustomRootFolder=data/`.

## Storage

| Key | Default | Meaning |
|---|---|---|
| `Storage:CustomRootFolder` | `""` | A prefix in front of every object of every dataset. Letters, digits, `-`, `_`, `/`, and it must end in `/` (`prod/`). Use it to keep datasets apart from other objects in a bucket, or to run several environments in one bucket. |
| `Storage:DtoName` | `""` | The dataset's folder. Empty means the type name (plus `V{n}` for a major version other than 1). Letters, digits, `-`, `_`, `.`. Shared by every dataset in the collection, so only set it in a collection with one dataset. |
| `Storage:UploadVerificationMode` | `DoNotVerify` | `VerifyAll` or `VerifyLargeDataOnly` reads the length of an uploaded object back and fails the write when it differs. Only for storage you don't trust. |

Objects are stored at `{CustomRootFolder}{folder}/...`. The [disk format](../../src/RxDatasets/diskFormat.md) lists
the files.

## Writer

| Key | Default | Meaning |
|---|---|---|
| `Writer:AllowedNamesPattern` | `""` (off) | A regular expression every record name must match. `Put` rejects a name that doesn't match with `ArgumentOutOfRangeException`. The streaming writes skip it with a warning, unless write coalescing is on (then they throw too). |
| `Writer:EnableWriteCoalescing` | `false` | The same as `WithWriteCoalescing()`. |

## Snapshots

Read when `WithSnapshot()` is used.

| Key | Default | Meaning |
|---|---|---|
| `SnapshotCreation:SnapshotInterval` | `100` | Write a snapshot when the newest version is this many versions past the last snapshot. |
| `SnapshotCreation:PeriodInHours` | `24` | Also write a snapshot when the last one is older than this and something changed since. `0` writes one after every version. |

Larger intervals mean fewer snapshot writes and a longer replay at startup. Servers in production use 100 to 1000
versions and 24 to 168 hours.

## Cleaning

Read when `WithCleanup()` is used. See [Cleaning](08-dataset-lifecycle.md#cleaning) before you turn it on.

| Key | Default | Meaning |
|---|---|---|
| `Cleanup:IsEnabled` | `true` | `false` turns cleaning off without changing code. |
| `Cleanup:MinimumSnapshotsToKeep` | `2` | Keep at least this many of the newest snapshots. Minimum 2. |
| `Cleanup:MinimumAgeToLiveInMinutes` | `120` | Never delete a snapshot younger than this. Minimum 120. |
| `Cleanup:KeepAllDeltas` | `true` | `false` also deletes deltas, up to and including the oldest snapshot that is kept. That limits history and `ReadHistoric`. |

## Cache

Read when `WithCache()` is used.

| Key | Default | Meaning |
|---|---|---|
| `DtoStorageCache:CacheSizeMB` | `100` | The size of the in-memory cache of index files and record bytes, per dataset. 0 to 2048. |

Production servers commonly use 1024. In a container, budget the cache of every dataset into the memory limit, and
consider `DOTNET_GCHeapHardLimitPercent` to keep the runtime inside the container's limit.

## Distributed lock

Read when `WithDistributedLock()` or `AddSharedDistributedLock<T>()` is used. Values are `TimeSpan`s (`"00:00:05"`).
[Distributed lock and fenced writes](../distributed-lock.md#timings) explains what follows from them.

| Key | Default | Meaning |
|---|---|---|
| `DistributedLock:PollingInterval` | 5 s | How often every instance acquires, checks for a handover request, and renews. |
| `DistributedLock:RenewalInterval` | 5 min | A lock is written to expire this far ahead. Also how long a crashed holder blocks a standby at most. |
| `DistributedLock:RenewalThreshold` | 30 s | The holder renews once `RenewalInterval - RenewalThreshold` has passed since its last renewal. |
| `DistributedLock:HandoverTimeout` | 15 s | How long a holder that yielded waits for the requester before it competes again. Must be more than `PollingInterval`. |

A shorter `RenewalInterval` (one minute, for example) makes a crashed holder's lock free sooner, at the cost of more
writes to storage.

## Reader paging

These are root keys, not a section. `GrpcReaderServiceImpl<T>` reads them; they aren't validated.

| Key | Default | Meaning |
|---|---|---|
| `PageSize` | `1000` | The page size when a request doesn't set one. Servers often lower it to 500. |
| `MaxPageSize` | `1000` | The largest page a request may ask for. |
| `PaginationCacheDuration` | `60` | Seconds a page token stays valid. Tokens live in the memory of one instance. |

## Storage providers

Each provider has its own section; [Storage providers](06-storage-providers.md) describes them.

| Section | Keys |
|---|---|
| `LocalDiskStorage` | `RootFolder` (default `dataset-localstorage`, relative to the working directory) |
| `GoogleCloud` | `BucketName` (required), `ProjectId`, `JsonCredentials`, `EmulatorHost`, `EnableWriteQueue` (default `true`) |
| `S3` | `BucketName` (required), `AccessKey`, `SecretKey`, `ServiceUrl` or `Region` (one of them), `RequestChecksumCalculation`, `ResponseChecksumValidation` (both default `WHEN_REQUIRED`) |
| `AzStorage` | `ContainerName` (required), `ConnectionString` or `AccountUrl` (one of them), `ManagedIdentityClientId` |
| `MinioS3` | `BucketName` (required), `AccessKey`, `SecretKey`, `ServiceURL` or `AwsRegion` (one of them) |

Keep access keys and connection strings out of shared configuration files. Inject them as environment variables from
a secret store, or use identity-based access (Google application default credentials, Azure managed identity).

## Optional packages

| Section | Package | Keys |
|---|---|---|
| `JsonOutput` | Reporting (`WithJsonReporting()`) | `Prefix` (must end in `/`), `IncludeVersionInFileName`, `IncludeVersionInPath` |
| `DatasetWatcher` | DatasetWatcher | `CheckingIntervalInSeconds` (default `1`) |

## Fixed timings

These aren't configurable, but explain what you see in logs:

| What | Interval |
|---|---|
| Reader and writer retry a failed initialization | 5 s |
| A read-only replica checks storage for new versions | 5 s |
| A reporter or snapshot consumer retries after an error | 5 s |
| Cleaning retries after an error | 30 s |
| The Google Cloud write queue merges writes to one object | 2 s |
| Delta data without an index counts as an orphan (from a crashed writer) after | 1 min |

## Keys that do nothing

Older configurations sometimes carry keys that no current version reads. They are harmless, but misleading:
`Storage:DeltaIndexCompression` and the other `...Compression` keys, `Storage:LogStorageAccess`,
`Storage:ReadOnlyMode`, `SnapshotCreation:StartupDelaySeconds` and `Cleanup:StartupDelaySeconds`. A read-only host is
chosen with `AddReadOnlyDtoServer<T>()`, not with configuration.

## A complete example

```json
{
  "Storage": { "CustomRootFolder": "data/" },
  "GoogleCloud": { "BucketName": "my-datasets", "EnableWriteQueue": true },
  "DtoStorageCache": { "CacheSizeMB": 1024 },
  "SnapshotCreation": { "SnapshotInterval": 500, "PeriodInHours": 24 },
  "Cleanup": { "MinimumSnapshotsToKeep": 3, "MinimumAgeToLiveInMinutes": 1440, "KeepAllDeltas": true },
  "Writer": { "EnableWriteCoalescing": true },
  "DistributedLock": {
    "PollingInterval": "00:00:05",
    "RenewalInterval": "00:01:00",
    "RenewalThreshold": "00:00:20",
    "HandoverTimeout": "00:00:15"
  },
  "PageSize": 500,
  "MaxPageSize": 1000
}
```
