# tst/RxDatasets.StorageTests/Common

The abstract spec suite every storage backend runs, and `DatasetHelper` tests.

## Files

- `StorageTestSpec.cs` — abstract base: about 50 `[Fact]`s on a `DatasetReader<object>`/`DatasetWriter<object>` pair (put, update, delete, `PutAll`, `PutAllChildren`, `DeleteChildren`, `BatchGet`, `List`, `GetChangedNames`, `WaitChangedNames`, `PutOrDeleteMany`, `StreamHistory`), one of them a skipped 2-minute performance test. A subclass supplies `SetupStorageAsync`.
- `StorageTestSpecWithTwoStepInit.cs` — adds two restart tests: a second `SetupStorageAsync` in the same test opens a new reader on the same storage, which must rebuild the same index from snapshot plus deltas.
- `DatasetHelperTests.cs` — `DatasetHelper`: temp-file directory, `FindParentOfEntity` (with and without level), `DeleteImplicitly`, `DeleteUsingParents`.

## Gotchas

- Subclasses live in `../SpecTests/`. Derive from `StorageTestSpecWithTwoStepInit` when the storage outlives the reader (any `IDiskStorage`), and from `StorageTestSpec` when it does not (`InMemoryStorageProvider`).
- `SetupStorageAsync` takes `[CallerMemberName]`; provider subclasses name the bucket, container or root folder after the calling test (lowercased, underscores removed, last 63 characters). Two tests whose names share the last 63 characters share storage.
- `SnapshotsOptions` sets `SnapshotInterval = 10`, and subclasses pass a `TestScheduler` that is never advanced: spec snapshots come from the delta count only.
- Changelist test names encode the sequence: I insert, C change, D delete, F the `from` version read, T the `to` version read (`..._On_IFDIC`).
- `GetRandomTempFilePath_Should_Return_CustomTempPathWhenConfigured` sets `TEMP_FILE_DIRECTORY` for the whole process and never clears it; the system-temp test in the same class then depends on running first.
