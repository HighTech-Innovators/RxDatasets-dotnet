# tst/RxDatasets.Tests/DatasetWriterTests

Contract tests for `IDatasetWriter<T>` write results (added/updated/deleted counts, version bumps).

## Files

- `CrcTrustTests.cs` — rewriting identical bytes is no change, and bytes that differ at the same or another length are a change.
- `PutOrDeleteManyContractTests.cs` — `PutOrDeleteMany` reports no change when a request deletes and re-puts the same bytes or ends on the original, and a change when the bytes differ.

## Gotchas

- Both classes run against `InMemoryDataset<T>` cast to `IDatasetWriter<T>`, not `DatasetWriter<T>`. The CRC-trust names and comments ("should not read bytes") describe the storage writer's optimization, which these tests cannot observe; the same `PutOrDeleteMany` cases run on the real writer in `RxDatasets.StorageTests/E2eTestsUsingTestHost.cs`.
