# Setup open tasks — 2026-10-22

Discoveries made while running `go-dot-ai`. The setup ran without
interactive gates at the user's request; every default it picked is
listed here for review.

## Tooling

- [ ] **Root `CLAUDE.md` does not import the scaffold.** `CLAUDE.md` is
  the project's own file, so the installer kept it. Without
  `@.ai/CLAUDE.md` as its first line, Claude Code does not load the
  dot-ai instructions. Decision: add the import line (and possibly trim
  content now covered by `.ai-local/rules/00-project-overview.md` and
  `01-environment.md`).
- [ ] **`.gitignore` changed to let skill links be tracked.** Line 491
  ignored all of `.claude/` (added in b2c38a3), which made the
  installer's `git add` fail. It now reads `.claude/*`,
  `!.claude/skills/`, `!.claude/settings.json`; everything else under
  `.claude/` (e.g. `settings.local.json`) stays ignored. Decision: keep,
  or revert and keep the scaffold links local-only (other clones then
  have no skills).
- [ ] **GitHub Copilot not wired up.** `.github/copilot-instructions.md`
  is a spec-kit generated file (stale: says .NET 8, project targets
  .NET 10). The installer would only replace it with a link to
  `.ai/AGENTS.md` under `--force`. Decision: keep spec-kit's file,
  or re-run `go-dot-ai` with Copilot selected and `--force`.
- [ ] **spec-kit and dot-ai overlap.** `.github/prompts/*.prompt.md`
  (speckit.specify/plan/tasks/implement) and the `specs/` folder cover
  the same ground as dot-ai's `create-spec` / `create-plan` /
  `implement` with `docs/specs`, `docs/plans`. Decision: which workflow
  is canonical, and whether `specs/` should move or stay.
- [ ] **Skill name collision.** dot-ai's `security-review` shadows Claude
  Code's built-in `/security-review`. Decision: accept, or drop the
  skill from the install list.
- [ ] **Windows install needs PowerShell 7.** Windows PowerShell 5.1
  refuses to create symlinks without elevation even with Developer
  Mode on; the install was run with `pwsh` 7.6. The registered
  WorktreeCreate hook calls `powershell` (5.1). Decision: check that
  worktree creation works, or point the hook at `pwsh`.
- [ ] **Issue tracker left at `type: local`.** The remote is GitLab
  (`gitlab.developer.htiprojects.nl`, project
  `oss/rxdatasets/RxDatasets-dotnet`, git over SSH — the SSH key does
  not cover the issue API). `glab` is not installed and no
  `GITLAB_TOKEN`/`GLAB_TOKEN` is set. Fix: install `glab` and run
  `glab auth login --hostname gitlab.developer.htiprojects.nl`, or
  export a token as `GITLAB_TOKEN`; then re-run `go-dot-ai` to switch
  `tracker:` to `gitlab`.

## Configuration and code

- [ ] **`CLAUDE.md` swaps the two storage interfaces.** It says each
  `RxDatasets.StorageProvider.*` project implements `IStorageProvider`;
  they implement `IDiskStorage<T>` (e.g. `S3Storage.cs:11`), and
  `IStorageProvider<T>` is the delta/snapshot layer
  (`ObjectStorageProvider`). Decision: fix `CLAUDE.md`.
- [ ] **Broken Dockerfile stages.** `publish-benchmark` and
  `publish-reader-benchmark` publish `testapps/DtoServerBenchmark` and
  `testapps/DtoServerReaderBenchmark`, which do not exist (now
  `RxDatasetsBenchmark`, `RxDatasetsReaderBenchmark`). CI never builds
  them. Decision: fix the paths or drop the stages.
- [ ] **Microsoft.Extensions.* pinned to 9.0.8 on .NET 10.**
  `Directory.Packages.props`; `Microsoft.AspNetCore.Mvc.Testing` too.
  Decision: move to 10.x.
- [ ] **Renovate disabled.** `renovate.json` has `"enabled": "false"`.
  Decision: re-enable, or delete the file.
- [ ] **Stale build output of removed projects.**
  `src/RxDatasets.Reporting.Cqs/`, `tst/RxDatasets.Reporting.Cqs.Tests/`,
  `testapps/DTOs/Demodto2V1.Server/` contain only `bin/obj` (ignored).
  Decision: delete locally.

## Infrastructure

- [ ] **SonarQube host gone.** `.gitlab-ci.yml` `sonarqube-check` is
  `allow_failure: true` because `sonar.developer.htiprojects.nl` is
  unavailable; the README still shows its badge. Decision: restore,
  replace, or remove the stage and badge.
- [ ] **Stale VS Code config and `.env.example`.** `.vscode/launch.json`
  launches `src/Host/bin/Debug/net8.0/Host.dll` (now `testapps/Host`, net10.0)
  with `envFile: .env`; `.env.example` lists `S3_ENDPOINT`, `S3_ACCESS_KEY`,
  `S3_SECRET_KEY`, which no code reads (hosts read `S3__*` config keys).
  Decision: fix or remove both.

## Terminology (step 9)

- [ ] **Confirm uncertain terms** in `.ai-local/terminology/datasets.md`
  (written without asking, at the user's request):
  - **Tracking id / SLA timestamp** — the code stores and forwards them;
    their business meaning (what system sets them, what the SLA measures)
    is not in the repo.
  - **First-class vs reader-side alias** — is `AliasDatasetReader`
    (`RxDatasets.Aliasing`) being superseded by writer-side aliases on
    `feature/77-first-class-aliases`?
  - **DTOflow, ESL, store, tenant** — names from the consuming product seen in
    tests (`IndexKeysTests`, `IndexMemoryBenchmark`); left out as not library
    terms. Add them if agents need them.
  - Any terms you expect that are missing.

## Environment (step 10)

- [ ] **Review `.ai-local/rules/01-environment.md`** (drafted from the code,
  not confirmed with the user). Probes that were not asked:
  - .NET SDK: no `global.json`; this machine has 10.0.113 and 10.0.301.
    Pin one?
  - How do you run things locally: `docker compose up`, `dotnet run` of
    `testapps/Host`, or an IDE (Rider: `RxDatasets.sln.DotSettings`, NCrunch:
    `*.ncrunchsolution`)?
  - Access needed for real backends (AWS/Azure/GCP accounts), the private
    NuGet feed behind `TARGET_NUGET`, and the GitLab CI variables.
  - Any environment variable or gitignored local file missing from the table
    and the `## Local assets` list (currently none).

## Style alignment (step 11)

`.ai-local/rules/11-dotnet-style.md` was inferred from the codebase (option C,
chosen without asking). Mismatches between it and the code:

- [ ] **`var` rule contradicts practice.** Root `CLAUDE.md` says no `var`
  without an apparent type; `.editorconfig` does not enforce it
  (`csharp_style_var_* = false:none`) and even this branch's new `src/` code
  has 134 `var` declarations vs 35 explicit ones. Decision: drop the rule, or
  enforce it (`csharp_style_var_elsewhere = false:suggestion` or higher).
- [ ] **Block-scoped namespaces** remain in ~25 files (`DtoServerBuilder.cs`,
  `BackgroundServices/`, `GenericNotifierLogic/`, provider `DI.cs`, all
  GoogleCloud files, 10 test files); `GrpcWebApplicationFactory` has none.
  Decision: convert, and add `csharp_style_namespace_declarations =
  file_scoped:warning`.
- [ ] **Interpolated log messages and `Console` output** in `DatasetHelper`,
  `DataMigrator`, `ObjectStorageReader`, `SnapshotCreationPlugin`,
  `LocalDiskStorage`, `MinioS3Storage`, `AzStorage`, `ConfigExtensions`;
  placeholder casing mixed (`{objectKey}` vs `{ObjectKey}`).
- [ ] **`async void` and sync-over-async** — `InitializationLoop` in
  `AzStorage`/`MinioS3Storage`; `.Wait()` in `InMemoryStorageProvider` ctor and
  `SnapshotCreationPlugin`.
- [ ] **Naming** — `MB_BYTES` const; `Async` suffix used inconsistently on
  privates (`EnsureBucketExistsAsync` vs Minio `EnsureBucketExists`); test
  naming mixes five conventions; class/file name mismatches in `tst/`.
- [ ] **Mixed UTF-8 BOM** across files. Decision: pick one and set `charset`
  in `.editorconfig`.

## Configuration and code — found while writing nested AGENTS.md (step 8)

Verified by reading the code (not by running it) unless marked otherwise.
The nested `AGENTS.md` Gotchas sections carry the details.

### Likely bugs

- [ ] **Secrets written to stdout.** `src/RxDatasets.Abstractions/ConfigExtensions.cs:50`
  prints every key/value of each validated config section, including
  `S3:SecretKey`, `MinioS3:SecretKey`, `AzStorage:ConnectionString`,
  `GoogleCloud:JsonCredentials`; it also calls `BuildServiceProvider()`
  inside `Configure` (line 38). Decision: mask or drop the dump.
- [ ] **Cache keys swapped.** `CacheStorageProvider.StoreDelta(ulong, byte[], byte[])`
  (`src/RxDatasets/StorageProviders/CacheStorageProvider.cs:57-58`) caches the
  index under the data key and vice versa; `CacheStorageProviderTest` uses the
  same seed for both arrays, so it cannot see it. Decision: fix and use distinct seeds.
- [ ] **`InMemoryStorageProvider.ReadSnapshotData` reads `_deltas`**
  (`InMemoryStorageProvider.cs:32`); `ListVersions` hides snapshots while no
  deltas exist; `StoreDelta(Stream)` disposes the caller's stream.
- [ ] **`WithCleanup()` without `WithSnapshot()`** leaves `IScheduler`
  unregistered (`DtoServerBuilder.cs`), so DI fails. Decision: `TryAdd` it.
- [ ] **Cleaning and processed versions.** With no `ProcessedVersions` entry,
  `GetLowestProcessedVersion()` is 0 and `StorageCleaningService` returns early;
  a removed reporter's entry blocks cleaning forever. Decision: intended?
  Add expiry or deregistration.
- [ ] **Distributed lock ordering and atomicity.** `DistributedLockService`
  notifies `Acquried` before `_lockToken` is set; acquisition is read-then-write
  with no conditional write; expiry relies on each instance's clock.
- [ ] **Read-only provider may miss change notifications.**
  `ReadOnlyObjectStorageProvider.Initialize` compares `DeltaInfo` before
  `DoInitialize` completes (unverified at runtime).
- [ ] **Smaller correctness items** (details in `src/RxDatasets/**/AGENTS.md`):
  `CacheStorageProvider` starts twice (ctor + hosted service);
  `NotificationsHub` never drops disposed subscriptions;
  `SnapshotCreationPlugin` stacks timers; `ReadDataFromSources` reads only the
  first source; `SharedMethods.ExtractNumber` parses versions as `int`;
  `EnumerateHistoryEntries` prefix match is not segment-aware (`t/1` matches
  `t/10/...`); invalid names throw in `Put` but are skipped (then implicitly
  deleted) in `PutAll`; empty payloads rewritten every `PutMany`; read/write
  `DatasetReader` never retries a failed init; `DataMigrator` rewrites delta
  indexes in place with no lock check; `IOptions<StorageConfig>` is shared by
  all datasets in a host.
- [ ] **Provider inconsistencies.** Minio `UploadBytes` deletes before putting;
  GCS `DeleteFile` ignores a queued write of the same key (can resurrect
  `request-lock`); GCS bucket location hard-coded `eu-north-1` (unverified);
  GCS health check lists the whole bucket; `async void InitializationLoop` in
  `AzStorage`/`MinioS3Storage`; the `IDiskStorage` error contract differs per
  provider (table in `src/AGENTS.md`); `AddSingleton` vs `TryAddSingleton`.
- [ ] **Test hosts.** `testapps/Host` maps Testdto gRPC services with no Testdto
  dataset registered; `ReadOnlyHost` maps the Demodto writer service without a
  writer; `TestdtoV1.Server/ReaderService.cs` drops `WaitForChanges`.

### Docs that disagree with the code

- [ ] **`src/RxDatasets/diskFormat.md` is stale.** Says `snapshot/` (code:
  `snapshots/`); old proto field names (`position_in_datafile`,
  `number_of_bytes`, `*_in_versions`); `crc32` as int32 (proto: uint32); says
  deleted entities leave the next snapshot (they stay as tombstones); omits
  `lock-file`, `request-lock`, legacy `VersionIndex.pb`/`latestVersion.pb` and
  deletes-only deltas without `data.pbs`; broken proto link. Decision: refresh it.
- [ ] **Root `CLAUDE.md`, further mismatches.** Says `snapshot/{version}`; calls
  the cache "LRU" (a size-limited `MemoryCache`); its "no `var` without apparent
  type" rule is neither enforced (`.editorconfig` sets `csharp_style_var_*` to
  `false:none`) nor followed; omits `testapps/`, `end-to-end-tests/`,
  `RxDatasets.AliasingTests`, and that `dotnet test` needs Docker.
  Decision: rewrite, or import `@.ai/CLAUDE.md` and point at the overview.
- [ ] **Misleading test names.** `tst/RxDatasets.Tests/DatasetWriterTests/`
  tests `InMemoryDataset`; `SpecTests/UsingInMemoryTests/InMemoryDatasetTests`
  tests `InMemoryStorageProvider`; several class/file name mismatches.

### Dead code and duplication

- [ ] **Dead code.** Aliasing `DI.cs`: `GetGrpcReaderService<T>`,
  `EvaluateIdPatternExpression`, `TestExtractValueSlicing`, and the ~700-line
  `PatternsAndExpressions` living in a DI file; GoogleCloud `IsFlushEligible`,
  `QueuedAt`, `TryQueueWriteAsync(string, Stream)`; `ImmutableDeltaEntity`;
  `ReadSnapshotDataAsync` (throws everywhere); `DemodtoHostContainerFixture`;
  csproj `Models\Config\**` removes. Decision: delete.
- [ ] **Duplication.** `ReadEntityBytes` x3 and `GenerateChangedNames` x2 in
  core; S3 vs Minio providers (two S3-compatible backends with copied
  init/health code and different timeouts); test helpers (`IDiskStorage`
  decorators, `GenerateRandomData` x3, provider setup x6). Decision: extract,
  or deprecate Minio.
- [ ] **Misspelled public API.** `DistirbutedLockStatus.Acquried`,
  `StorageProviderReadinessNotifcation`; file names `LockAquiredNotification.cs`,
  `*OurNotification.cs`, `DTOStorageCacheConfig.cs`. Decision: rename with
  `[Obsolete]` aliases, or accept.

### Tests

- [ ] **Docker filter leaks.** `GoogleCloudStorage/WriteQueueIntegrationTests`
  needs fake-gcs-server but is outside `WithCustomTestContainersImplementation`;
  `LocalDiskDatasetTests` needs no Docker but is inside it. Decision: move, or
  filter by trait.
- [ ] **Skipped tests.** 15 GCS tests always skipped ("requires GCS emulator")
  though a GCS fixture exists; plus perf/manual skips (put-all-children perf,
  S3 >1000 items, `AzStorageManagedIdentityTests`).
- [ ] **Shared process state.** Tests set `TEMP_FILE_DIRECTORY`, `S3__*`,
  `MINIOS3__*`, `Storage__CustomRootFolder`, `STORAGE_EMULATOR_HOST` without
  resetting them, under xunit parallelism. Decision: reset or serialise.
- [ ] **Timing-sensitive tests** (`SnapshotCreationPluginTests`, 100 ms
  CountChildren perf, zero-allocation assert, 11 s spin). Decision: slow category.
- [ ] **Two mocking libraries.** StorageTests uses Moq, RxDatasets.Tests uses
  NSubstitute. Decision: standardise, or state the per-project rule.
- [ ] **Unpinned images.** Test containers use `:latest` with
  `PullPolicy.Always`; e2e uses `debian:latest`, `bash`, `grpcurl`, `jq`,
  `bitnamilegacy/minio`. Decision: pin.
- [ ] **External copy.** `tst/RxDatasets.Tests/Models/IndexKeysTests.RealPatterns`
  hand-copies patterns from the DTOflow protos (outside this repo).

### Scripts, Docker, config

- [ ] **`#!/bin/sh` scripts use bash syntax.** `end-to-end-tests/run.sh` (the CI
  entry point) and `generate-expected-data/create_expected_data_for_new_tests.sh`
  use `[[`, `local`, `echo -e`. `e2e_tests.sh` refers to a missing `run_me.sh`
  and installs arm64 grpcurl with `sudo`.
- [ ] **E2E fixture is in the legacy format.** `end-to-end-tests/expected-data`
  has `VersionIndex.pb` and no `DeltaInfo.pb`/`SnapshotInfo.pb`; regenerating it
  would drop the legacy-reader coverage. Decision: keep as a legacy fixture and
  add a current-format one.
- [ ] **Unread configuration.** Host `appsettings.json` `AzStorage`,
  `OpenTelemetry`, `Serilog`; ReadOnlyHost `Storage:ReadOnlyMode`; `MINIOS3__*`
  in compose files (hosts use `WithS3Storage`); compose build args `DOCKERHOST`,
  `VERSION`; root `compose.yml` quotes `Storage__CustomRootFolder` (unverified:
  may fail validation). Undocumented root keys `PageSize`, `MaxPageSize`,
  `PaginationCacheDuration` (`GrpcReaderServiceImpl`).
- [ ] **Test credentials committed.** `ROOTUSER`/`MINIOPASSWORD` in the hosts'
  `appsettings.json`, compose files, `expected-data/.root_user`/`.root_password`.
  Decision: accept as test-only and say so.
- [ ] **Project hygiene.** `Host.Testserver.csproj` references unused projects
  and a `tst/` project; `RxDatasets.StorageProvider.InMemoryProvider.csproj`
  lists each reference twice; `Nullable` is set per csproj, not in
  `Directory.Build.props`; benchmarks print microseconds labelled "ns".
