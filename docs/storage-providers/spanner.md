# Cloud Spanner storage provider

Stores a dataset's deltas and snapshots as rows in a Cloud Spanner database, instead of as blob files. The on-disk
format (delta/snapshot indexes, `data.pbs` contents) is the same as for the blob providers (see
`src/RxDatasets/diskFormat.md`); only where the bytes live, and how versions are found and committed, differ.

## Activating it

Package: `HighTechInnovators.RxDatasets.StorageProvider.Spanner`.

```csharp
// Read/write host
services.AddDtoServer<MyDto>()
    .WithSpannerStorage()
    .WithCache()        // optional, as with any provider
    .WithSnapshot()     // optional; runs on the leader only
    .WithCleanup()      // optional; runs on the leader only
    .Build();

// Read-only host
services.AddReadOnlyDtoServer<MyDto>()
    .WithSpannerStorage()
    .WithCache()
    .Build();
```

Do not add `WithDistributedLock()`: it needs blob storage, and writers don't need it here (see
[How it works](#how-it-works)).

### Configuration

Section `Spanner` (`SpannerConfig`):

| Field | Default | Meaning |
|---|---|---|
| `ConnectionString` | (required) | `Data Source=projects/{project}/instances/{instance}/databases/{database}`. Add `;EmulatorDetection=EmulatorOnly` to use the emulator at `SPANNER_EMULATOR_HOST`. |
| `TablePrefix` | `RxDatasets` | Prefix of the table names. Letters, digits and `_`, starting with a letter. |
| `TablePerDataset` | `false` | `false`: all datasets share one set of tables. `true`: a set of tables per dataset. See [Table layouts](#table-layouts). |
| `CreateTables` | `false` | Lets the host create missing tables at start-up. Needs `spanner.databases.updateDdl`. See [Creating the tables](#creating-the-tables). |

The dataset is identified, as for the blob providers, by its storage folder from section `Storage` (`StorageConfig`):
`CustomRootFolder` + (`DtoName`, or the type name, plus `V{n}` when its namespace ends in a version other than 1) +
`/`, for example `LinkV2/`. That string is the `Dataset` key column in every table.

## Table layouts

Both layouts use the same four tables with the same columns, including `Dataset`; only the table names differ.

| | Shared (default) | `TablePerDataset: true` |
|---|---|---|
| Table names | `{TablePrefix}_Versions`, `_Chunks`, `_ProcessedVersions`, `_Leases` | `{TablePrefix}_{folder}_Versions`, ... |
| Schema changes | One DDL run, ever | One per new dataset (DTO) |
| Table count | 4 | 4 per dataset (Spanner allows 5000 tables per database) |
| Removing a dataset | Keyed `DELETE ... WHERE Dataset = ...` | `DROP TABLE` |
| Access control | Per database | Per table possible (fine-grained access control) |

Pick **shared** unless you need per-dataset access control or instant dataset removal. Spanner DDL is slow
(seconds to minutes), rate limited and needs admin rights, which the shared layout keeps out of deployments. The
leading `Dataset` key spreads datasets over splits; within a dataset, versions are appended at the end of its key
range, which is bounded by that dataset's single writer rate and is not a hotspot problem in practice.

In per-dataset names the folder is sanitized to `[A-Za-z0-9_]`; when that changed it (for example `a-b/` and `a_b/`),
an 8-hex-digit hash of the folder is appended so names stay unique. A name over 128 characters fails at start-up:
shorten `TablePrefix` or the folder.

## Creating the tables

Either create them up front (recommended for production), or set `CreateTables: true` and give the host
`spanner.databases.updateDdl`. With `CreateTables: false` and a table missing, the host never becomes ready and logs
the exact DDL on every retry (5 s); started with `startIfInitFails: false` it fails to start instead.

The DDL for the shared layout with the default prefix (for per-dataset tables, use the per-dataset names):

```sql
CREATE TABLE RxDatasets_Versions (
  Dataset STRING(MAX) NOT NULL,
  Kind INT64 NOT NULL,          -- 0 delta, 1 snapshot
  Version INT64 NOT NULL,
  BlobId STRING(32) NOT NULL,
  DataLength INT64 NOT NULL,
  CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (Dataset, Kind, Version);

CREATE TABLE RxDatasets_Chunks (
  Dataset STRING(MAX) NOT NULL,
  BlobId STRING(32) NOT NULL,
  Part INT64 NOT NULL,          -- 0 index, 1 data
  ChunkNo INT64 NOT NULL,
  Data BYTES(MAX) NOT NULL,
  CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (Dataset, BlobId, Part, ChunkNo);

CREATE TABLE RxDatasets_ProcessedVersions (
  Dataset STRING(MAX) NOT NULL,
  Who STRING(MAX) NOT NULL,
  Version INT64 NOT NULL,
) PRIMARY KEY (Dataset, Who);

CREATE TABLE RxDatasets_Leases (
  Dataset STRING(MAX) NOT NULL,
  Owner STRING(MAX) NOT NULL,
  ExpiresAt TIMESTAMP NOT NULL,
) PRIMARY KEY (Dataset);
```

There are no schema migrations: a different schema means a different `TablePrefix`.

## IAM

| Host | Role |
|---|---|
| Read/write | `roles/spanner.databaseUser` on the database (read, write, DML; it also includes `updateDdl`, which only matters with `CreateTables`). For writers without DDL rights, use a custom role without `spanner.databases.updateDdl`. |
| Read-only | `roles/spanner.databaseReader` on the database. |

## Local development with the emulator

```bash
docker run -d -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator:1.5.51
export SPANNER_EMULATOR_HOST=localhost:9010

gcloud config configurations create emulator
gcloud config set auth/disable_credentials true
gcloud config set project test-project
gcloud config set api_endpoint_overrides/spanner http://localhost:9020/
gcloud spanner instances create test-instance --config=emulator-config --description=test --nodes=1
gcloud spanner databases create rxdatasets --instance=test-instance
```

```json
"Spanner": {
  "ConnectionString": "Data Source=projects/test-project/instances/test-instance/databases/rxdatasets;EmulatorDetection=EmulatorOnly",
  "CreateTables": true
}
```

The tests do the same with Testcontainers (`tst/RxDatasets.StorageTests/SpannerEmulatorFixture.cs`).

## How it works

- **A version is a row.** Storing a delta or snapshot writes its bytes as 1 MiB chunk rows under a fresh blob id,
  then inserts its `Versions` row (`Dataset, Kind, Version` → blob id). That insert is the commit point: until it is
  committed, nobody sees the version. A blob of up to 32 chunks goes in one transaction with its row; a larger one in
  several, with the row in the last.
- **No `DeltaInfo.pb` / `SnapshotInfo.pb`.** The available versions are one query (first and last delta, the
  snapshots). Every host polls it every 5 s, so readers and writers also see what other hosts stored.
- **No lock for writes.** A writer stores version N = its latest + 1. The commit transaction checks that the dataset's
  latest version (last delta, or last snapshot when cleaning removed the deltas) is N − 1 and inserts the row, never
  overwriting. So every instance can write; an instance that is behind fails instead of overwriting (see
  [Version conflicts](#version-conflicts)).
- **One leader for the background work.** Snapshot creation, cleaning and the reporting plugin run on one host per
  dataset: the holder of the `Leases` row. Writes and reads run on every host.

## Operations

### Leader lease

- Each host tries to take or renew the lease on every 5 s poll, by Spanner's clock. A lease lasts 30 s; the holder
  treats it as valid for 20 s after its last renewal, so it stops its background work about 10 s before another host
  can take over.
- A stopping host releases the lease, so a standby takes over on its next poll (≤ 5 s). A crashed leader is replaced
  after ≤ 30 s plus a poll.
- Log lines: `This host runs the {Dataset} background work` / `does not run`, and `Could not renew the {Dataset}
  lease` (warning) when Spanner can't be reached.

### Reporting is at least once

A leader paused for longer than the margin (a long GC pause, a frozen VM) can still run background work while the
next leader already does. A reporting consumer can therefore see a delta twice around a hand-over; make what it does
idempotent.

### Cleaning and the orphan sweep

- `WithCleanup()` behaves as for the blob providers (`Cleanup` config section): it deletes snapshots older than the
  ones kept and, unless `KeepAllDeltas`, the deltas up to the oldest kept snapshot. It only proceeds once the lowest
  recorded processed version (the reporting consumers') has reached that snapshot; with no processed versions recorded,
  nothing is removed. It runs as one transaction per cleanup, which stays well under the 80,000-mutation limit unless
  tens of thousands of versions are removed at once.
- Each cleanup also removes chunk rows older than a day that no version row points at. These come from a writer that
  crashed between the commits of a large blob, or whose commit outcome was unknown. The sweep scans every chunk row of
  the dataset, so it gets slower as the dataset grows.

### Temp disk

Snapshot creation streams the current snapshot through a temporary file and writes the new snapshot to another one.
Give the leader free ephemeral storage of about **2× the snapshot data size**, in `TEMP_FILE_DIRECTORY` (or the
system temp folder if that isn't set).

### Version conflicts

`System.Data.DBConcurrencyException: Delta N of {Type} was stored by another writer first` means another instance
stored version N first. Nothing was overwritten. This host then re-reads the versions and reloads its writer and
reader (they briefly report not ready, then ready again); the write itself failed and the caller should retry it.
Occasional conflicts are normal when several instances write; a steady stream means many instances write the same
dataset, and each conflict costs a full index reload. In that case route a dataset's writes to one instance.

### Monitoring and readiness

- Health checks as for any dataset: `DatasetReadinessCheck-{Type}` and `DatasetHealthCheck-{Type}`. A host is ready
  after its first successful version listing; a leadership change doesn't change readiness.
- Non-leaders are ready and serve reads and writes; only the background work is leader-only.
- Watch for the lease warning above, for `DBConcurrencyException` rates, and for start-up errors listing missing
  tables.

## Limits and caveats

- **Tested against the emulator only** (`gcr.io/cloud-spanner-emulator/emulator:1.5.51`); not yet against a real
  Spanner instance.
- **Size limits:** a cell holds at most 10 MiB (chunks are 1 MiB) and a commit at most 100 MB (a commit holds at most
  32 chunks); large blobs are split over commits automatically. Cleanup is one transaction, so mind the 80,000
  mutations per commit. Identifiers are at most 128 characters.
- **Cost:** Spanner storage costs roughly an order of magnitude more per GB-month than GCS standard storage, plus the
  instance's compute capacity. Large snapshots dominate storage; compare current pricing for your region before moving a
  large dataset.
- **No migration tooling:** there is no tool to move a dataset between blob storage and Spanner, and no schema
  migrations.
