# Bigtable storage provider

Stores a dataset's deltas and snapshots as rows of a Cloud Bigtable table. Payloads too large for a row go to a
Cloud Storage bucket, which also holds the writer lock and `ProcessedVersions.pb`. Compared with the plain bucket
provider (`WithGoogleCloudStorage`), small writes commit with one low-latency row write and there is no `DeltaInfo.pb`
or `SnapshotInfo.pb` to keep in step.

## Activating it

Package: `HighTechInnovators.RxDatasets.StorageProvider.GoogleCloud` (brings `Google.Cloud.Bigtable.V2` and
`Google.Cloud.Storage.V1`).

```csharp
services.AddDtoServer<MyDto>()          // or AddReadOnlyDtoServer<MyDto>() for read-only hosts
    .WithGoogleBigtableStorage()
    .WithCache()                        // optional, as with any provider
    .WithDistributedLock()              // recommended for writers, see "The lock"
    .WithSnapshot()
    .WithCleanup()
    .Build();
```

`WithGoogleBigtableStorage()` also does what `WithGoogleCloudStorage()` does (bucket storage and its health check);
don't call both.

### Configuration

Section `GoogleCloud` (`GoogleCloudConfig`):

| Key | Default | Meaning |
|---|---|---|
| `BucketName` | (required) | Bucket for offloaded payloads, the lock and `ProcessedVersions.pb`. |
| `ProjectId` | none | GCP project of the Bigtable instance (required here) and of the bucket. |
| `BigtableInstanceId` | none | Required. |
| `BigtableTableId` | none | Required. One table can hold many datasets. |
| `JsonCredentials` | none | Service account key JSON. When unset, Application Default Credentials are used. |
| `MaxInlinePayloadBytes` | `1048576` (1 MB) | Payloads up to this size go in the row, larger ones to the bucket. The index is measured after compression, the data before. At most 50 MB (a Bigtable cell holds 100 MB). |
| `InlineDataCacheMB` | `64` | Per dataset reader: memory for data payloads read from rows (see "Sizing"). |
| `CompressPayloads` | `false` | Brotli-compress row payloads and indexes in the bucket, when at least 10 KB and at least 25% smaller. Data in the bucket is never compressed. |
| `BigtableEmulatorHost` | none | `host:port` of a Bigtable emulator; no credentials are used then. |
| `EmulatorHost` | none | `host:port` of a fake GCS server (also read from `STORAGE_EMULATOR_HOST`). |
| `EnableWriteQueue` | `true` | Bucket write queue; payload uploads bypass it. |

Section `Storage` (`StorageConfig`) is as for every provider: `CustomRootFolder` and `DtoName` set the dataset's
folder, which is also its row key prefix.

## The table

The library needs data permissions only, so it does not create the table. Create it once, with the column families
`meta` and `payload`, each keeping one version:

```sh
gcloud bigtable instances tables create rxdatasets --instance=my-instance --project=my-project \
    --column-families="meta:maxversions=1,payload:maxversions=1"
```

or with `cbt`:

```sh
cbt -project my-project -instance my-instance createtable rxdatasets
cbt -project my-project -instance my-instance createfamily rxdatasets meta
cbt -project my-project -instance my-instance createfamily rxdatasets payload
cbt -project my-project -instance my-instance setgcpolicy rxdatasets meta maxversions=1
cbt -project my-project -instance my-instance setgcpolicy rxdatasets payload maxversions=1
```

## IAM

| Host | Bigtable (table or instance) | Bucket |
|---|---|---|
| Writer | `roles/bigtable.user` | `roles/storage.objectAdmin`, plus `storage.buckets.get` (for example `roles/storage.legacyBucketReader`) |
| Read-only | `roles/bigtable.reader` | `roles/storage.objectViewer`, plus `storage.buckets.get` |

On start-up the bucket client checks that the bucket exists and tries to create it when it doesn't; give it
`storage.buckets.create` only if you want that.

## Local development

```sh
gcloud beta emulators bigtable start --host-port=localhost:8086
BIGTABLE_EMULATOR_HOST=localhost:8086 cbt -project test-project -instance test-instance createtable rxdatasets
BIGTABLE_EMULATOR_HOST=localhost:8086 cbt -project test-project -instance test-instance createfamily rxdatasets meta
BIGTABLE_EMULATOR_HOST=localhost:8086 cbt -project test-project -instance test-instance createfamily rxdatasets payload
docker run -p 4443:4443 fsouza/fake-gcs-server -scheme http
```

Then set `GoogleCloud:BigtableEmulatorHost=localhost:8086`, `GoogleCloud:EmulatorHost=localhost:4443`,
`GoogleCloud:ProjectId=test-project`, `GoogleCloud:BigtableInstanceId=test-instance` and
`GoogleCloud:BigtableTableId=rxdatasets`. The emulator keeps nothing after a restart. The tests start both emulators
with Testcontainers (`BigtableContainerInstanceFixture`).

## How it works

**Rows.** One row per delta or snapshot, keyed `{dataset folder}/deltas/{version}` or
`{dataset folder}/snapshots/{version}`, with the version as 20 zero-padded digits so rows sort by version. The
`index.pb` and `data.pbs` contents are the same as with the bucket provider (see `src/RxDatasets/diskFormat.md`).

| Cell | Holds |
|---|---|
| `payload:index`, `payload:data` | The index and the data, when they fit in the row. |
| `meta:index-object`, `meta:data-object` | Otherwise the bucket object holding them: `{dataset folder}/{deltas or snapshots}/{version}/{random id}-index.pb` or `-data.pbs`. |
| `meta:index-encoding`, `meta:data-encoding` | `br` when that payload is Brotli-compressed. |
| `meta:created-at`, `meta:data-length` | Unix ms; uncompressed data length (0 for a delta of only deletes). |

**No DeltaInfo or SnapshotInfo.** The rows are the record of which versions exist. The oldest delta is the first delta
row, the newest the last one; the snapshots are the snapshot rows. Bigtable has no reversed scans in this client, so the
newest delta is found by a keys-only scan that starts at the newest delta seen before, or the latest snapshot if
higher, which reads only the keys written since.

**The commit point.** Payloads that don't fit go to the bucket first, under a name unique to the attempt. Then the
delta's row is written with `CheckAndMutateRow`, only if the row does not exist, and only after checking that no
snapshot is at or above that version. That row write is the commit: readers see the delta from then on. A writer whose
index is behind (an earlier lock holder that missed losing the lock) can therefore neither overwrite a delta nor
re-create one that cleaning removed. Snapshot rows are written unconditionally.

**Inline vs bucket.** A row is read whole: reading one entity from a delta whose data is in the row reads all of that
data. The reader keeps it (`InlineDataCacheMB`), so the other entities of that version (one `BatchGet`, for example)
need no read. Data in the bucket is read by range, one entity at a time.

## Operations

**Cleaning** (`WithCleanup`, section `Cleanup`) works as with the bucket provider: snapshots older than the one it
keeps are deleted, and with `KeepAllDeltas: false` (the default is `true`) the deltas up to it as well, except the
newest delta row, which is always kept. Rows are deleted in batches of 10,000 before any bucket objects. Then every
bucket object in the cleaned versions' folders is deleted, which also removes orphans (see below). A cleaning run that
stops halfway is finished by the next one.

**Health.** `WithGoogleBigtableStorage` registers `storage.bigtable.{Entity}`, healthy when one row key of the table
can be read, next to the bucket check `storage.googlecloud.{Entity}`.

**The lock.** `WithDistributedLock` keeps its lease file in the bucket, as with the bucket provider. It decides which
replica writes and runs the snapshot, cleaning and reporting plugins. Correctness no longer depends on it: a second
writer fails on its first conflicting delta instead of overwriting one.

**Sizing.** Keep `MaxInlinePayloadBytes` small (the 1 MB default) when deltas are large and entities are read one at a
time, such as history, reporting catch-up or `BatchGet` across many versions: each such read from an inline delta
reads the whole row. Raise it when most writes are small and latency matters more. Give `InlineDataCacheMB` room for
the inline payloads of the versions read together, about `MaxInlinePayloadBytes` × the number of versions in a typical
batch. With `WithCache`, that cache may hold the same bytes a second time.

**Orphans.** A crash between a payload upload and its row write, or a writer that loses the race for a version, leaves
bucket objects no row points at. Cleaning removes them with the version's folder; orphans in versions it never cleans
(the newest delta, or every delta with `KeepAllDeltas: true`) stay.

**Version conflicts.** `InvalidOperationException: Delta N of {dataset} was stored by another writer; the dataset is
being reloaded` means this host's writer was behind: another writer stored version N, or a snapshot is already past
it. Its upload is removed; unless the lock was lost meanwhile, the provider lists the versions again and notifies
Ready, and the writer and reader reload their index. The write that failed is not stored; retry it. It shows up when
two hosts write without the lock, or when a lock holder was paused past its lease. If it repeats on one
host, check that only one writer holds the lock.

## Limits and caveats

- The table must exist with both column families before the first start.
- Read-only hosts poll the versions every 5 seconds; writers don't poll.
- Cleaning lists every delta key of the dataset and every bucket object under its `deltas/` (or `snapshots/`) folder
  on each run.
- Data over `MaxInlinePayloadBytes` is stored uncompressed in the bucket, so `CompressPayloads` doesn't shrink it.
- The gap between the snapshot check and the conditional row write is not atomic. Only a snapshot and a cleaning that
  both finish within it, while a stale writer writes, could get past it.
- `ProcessedVersions.pb` and the lock stay in the bucket, so the bucket is needed even when every payload fits in rows.
- A dataset written by the bucket provider is not readable by this provider, and the other way round.
