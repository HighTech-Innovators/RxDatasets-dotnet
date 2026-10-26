# tst/RxDatasets.StorageTests/SpecTests/UsingInMemoryTests

Spec suites and write-path tests on in-memory storage; they run everywhere, Docker or not.

## Files

- `InMemoryDatasetTests.cs` — `StorageTestSpec` on `InMemoryStorageProvider<object>` (`src/RxDatasets/StorageProviders`). Despite the name it does not test `InMemoryDataset<T>`.
- `UsingLowLevelInMemoryStorage.cs` — `StorageTestSpecWithTwoStepInit` on `ObjectStorageProvider` over `LowLevelInMemoryStorage`. Adds: index initialization falling back to an older snapshot when the newest one's index or data is missing or corrupt, snapshots holding only deleted entities, and the `UploadVerificationMode` checks on a Moq `IDiskStorage`. Its static `CreateSnapshotCreationService` (interval 5) is shared with `../../Services/`.
- `DeltaInfoDriftReproduction.cs` — regression tests for #72: `DeltaInfo.LatestDeltaVersion` must not drift when saving `DeltaInfo.pb` fails and is retried, including three concurrent writers on the async hub. Defines the `FaultyInMemoryStorage` (fails the next `DeltaInfo.pb` uploads) and `SlowInMemoryStorage` (delays uploads) decorators.
- `WriteCoalescingTests.cs` — `DatasetWriter` with `EnableWriteCoalescing`: concurrent `Put`/`PutMany`/`DeleteMany`/`PutOrDeleteMany`/`DeleteChildren` merged into fewer versions with correct per-caller responses, and a storage failure reaching every coalesced caller. Reuses `FaultyInMemoryStorage` and `SlowInMemoryStorage`.

## Gotchas

- Each test-class instance owns one storage, so calling `SetupStorageAsync` twice in a test reopens the same data; that is the restart the two-step spec relies on.
