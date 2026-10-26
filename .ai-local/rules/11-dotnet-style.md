# .NET Style (RxDatasets)

Inferred from the codebase (option C of the setup): `.editorconfig`, the
root `CLAUDE.md`, and a read of every C# file under `src/`, `tst/` and
`testapps/` on 2026-10-22. Where the code is split, the rule follows the
newer code (e.g. `feature/77-first-class-aliases`) and the older form is
listed under *Known deviations*. Extends `.ai/rules/stacks/dotnet/*`; this
file wins for this project.

## Layout and formatting

- 4-space indent in C#, 2 in project/props/JSON/Markdown files; LF line
  endings (`.editorconfig` `[*]`; shell scripts and Dockerfiles also via
  `end-to-end-tests/.gitattributes`); max 130 characters per line.
- Allman braces: every opening brace on its own line
  (`csharp_new_line_before_open_brace = all`). A single-statement guard
  `if (…) return;` without braces is accepted; multi-line bodies get braces.
- `using` directives outside the namespace (enforced as warning), System
  first. Implicit usings and nullable are on in every project.
- File-scoped namespaces (`namespace RxDatasets.Configs;`), matching the
  folder path.
- One public type per file, file named after the type.

## Types and members

- Accessibility modifiers on every non-interface member (enforced as
  error).
- Primary constructors for services, providers and health checks
  (`ObjectStorageProvider<T>(…)`, `StorageCleaningService<T>(…)`); use the
  parameters directly instead of copying them into fields unless the field
  needs a different name or lifetime.
- `record` / `readonly record struct` for messages and results
  (notifications, `WriteResponse`); immutable state uses `with`.
- Collection expressions (`[]`, `[..items]`) instead of `new List<T>()`.
- Expression-bodied members for one-line methods and properties; block bodies
  for constructors.
- Optional dependencies are nullable constructor parameters
  (`DistributedLockService<T>? lockService = null`), not service-locator
  lookups.
- Generic type parameter for the entity: `T` or `TEntity`, constrained
  `where T : class, IMessage<T>` on anything that touches protobuf.

## Naming

- Private fields `_camelCase`; protected and public members, constants and
  static readonly fields `PascalCase`.
- Async methods on the public contracts carry no `Async` suffix
  (`Put`, `Get`, `StoreDelta`, `ListVersions`); keep that within an interface.
  New hosted-service and framework overrides keep the framework name
  (`StartAsync`, `CheckHealthAsync`).
- Config classes end in `Config` (or `Options`); their section name is the
  class name without the suffix (`StorageConfig` → `Storage`), bound with
  `services.AddValidatedConfigSection<TConfig>()` and validated with
  DataAnnotations and `ValidateOnStart`.
- Spell new identifiers correctly; leave the misspelled public ones
  (`DistirbutedLockStatus.Acquried`, `StorageProviderReadinessNotifcation`)
  as they are unless the change is a deliberate rename.

## `var`

- Root `CLAUDE.md`: no `var` without an apparent type. Write the type when
  the right-hand side does not show it (`ulong version = state.LatestVersion;`);
  `var` is fine with `new`, casts and obvious literals.
- Most existing code uses `var` everywhere; leave it when you only touch
  nearby lines.

## Errors, logging, async

- "Object missing" from disk storage is `FileNotFoundException`; "not
  initialised / backend unavailable" is `InvalidOperationException`; writes
  without the lock throw `SynchronizationLockException`. Keep these
  contracts; callers depend on them.
- Inject `ILogger<T>`; log with message templates and PascalCase
  placeholders (`logger.LogDebug("Processing {NewCount} new entities",
  newCount)`), never interpolated strings or `Console`.
- `async Task` all the way; no `async void`, no `.Wait()` / `.Result`
  outside test setup.
- Pass `CancellationToken ct = default` as the last parameter of public async
  methods.

## Comments and docs

- XML doc comments on public contracts and on non-obvious internals,
  explaining *why* (see `Models/IndexKeys.cs`, `AliasStaging.cs`). No
  commented-out code.
- `ponytail:` marks a deliberate simplification with its upgrade path
  (`IndexKeys.cs`, `ImmutableIndexNode.cs`).
- Storage format changes update `src/RxDatasets/diskFormat.md` in the same
  change.

## Tests

- xunit + FluentAssertions (`.Should()`, with `because` where the reason
  is not obvious); xunit `Assert` only where FluentAssertions cannot express it.
- Mocking follows the project: NSubstitute in `RxDatasets.Tests` (global
  using), Moq in `RxDatasets.StorageTests`; prefer in-memory fakes
  (`InMemoryStorageProvider`, `LowLevelInMemoryStorage`) over mocks of
  `IDiskStorage`.
- Test names describe behaviour as PascalCase segments joined by
  underscores (`TwoEntitiesClaimingOneAlias_IsRejected_AndNothingIsStored`,
  `CountChildren_WithNoChildren_ReturnsZero`).
- Arrange / act / assert separated by blank lines; comments only when a
  section is long.
- Time through `IClock` / `IScheduler` and `TestScheduler`, not real delays.
- Tests that need Docker live under
  `SpecTests/WithCustomTestContainersImplementation/` so the CI filter can
  exclude them.

## Known deviations

These are recorded in `docs/2026-10-22-setup-open-tasks.md` (Style alignment);
leave them as they are unless the change is about them.

- Block-scoped namespaces in older files (`DtoServerBuilder.cs`, most
  `BackgroundServices/`, `GenericNotifierLogic/`, provider `DI.cs` files, all
  GoogleCloud files, several tests).
- Classic constructors in `DatasetReader`, `DatasetWriter`,
  `CacheStorageProvider`, the `*Storage` providers.
- Interpolated log messages in `DatasetHelper`, `DataMigrator`,
  `ObjectStorageReader`, `SnapshotCreationPlugin`, `LocalDiskStorage`,
  `MinioS3Storage`, `AzStorage`; `Console.Write` in `ConfigExtensions`.
- `MB_BYTES` constant (SCREAMING case).
- Mixed test naming (`Should_…`, lower-case sentences, `Test_` prefix) and
  class/file name mismatches.
