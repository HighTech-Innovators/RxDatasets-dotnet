# tst

Test projects for the RxDatasets library, all xunit 2 on net10.0. Each project's `AGENTS.md` names its mocking library and its gotchas.

## Folders

- `RxDatasets.Tests/` — fast in-process unit tests of `src/RxDatasets` and `src/RxDatasets.InMemoryDataset` (index models, gRPC reader service, distributed lock, writer contracts). See [`RxDatasets.Tests/AGENTS.md`](RxDatasets.Tests/AGENTS.md).
- `RxDatasets.StorageTests/` — the storage stack wired end to end: the per-provider spec suite, background services, alias hosts, gRPC test host, and the Testcontainers-backed provider tests. See [`RxDatasets.StorageTests/AGENTS.md`](RxDatasets.StorageTests/AGENTS.md).
- `RxDatasets.AliasingTests/` — `src/RxDatasets.Aliasing`: alias pattern engine and `AliasDatasetReader<T>`. See [`RxDatasets.AliasingTests/AGENTS.md`](RxDatasets.AliasingTests/AGENTS.md).
- `RxDatasets.StorageProvider.InMemoryProvider/` — not a test project: the in-memory `IDiskStorage<T>` that StorageTests and `testapps/Testserver` run on. See [`RxDatasets.StorageProvider.InMemoryProvider/AGENTS.md`](RxDatasets.StorageProvider.InMemoryProvider/AGENTS.md).

## Gotchas

- `RxDatasets.Reporting.Cqs.Tests/` holds only stale, untracked `bin/` and `obj/` build output; it has no project and is not in `RxDatasets.sln`.
- No project sets a `[Collection]`, `CollectionDefinition` or `xunit.runner.json`, so test classes run in parallel. A test that sets a process environment variable shares it with every class running at that moment.
- `src/RxDatasets` exposes its internals to `RxDatasets.Tests` and `RxDatasets.StorageTests` only; `src/RxDatasets.Aliasing` exposes its internals to `RxDatasets.AliasingTests`. A test of an internal goes in one of those.
