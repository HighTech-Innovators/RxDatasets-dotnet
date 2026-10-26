# tst/RxDatasets.Tests

In-process unit tests for `src/RxDatasets` and `src/RxDatasets.InMemoryDataset`; no containers, no network. Mocking library: NSubstitute, imported by `GlobalUsings.cs`; loggers are `NullLogger<T>.Instance` or `Substitute.For<ILogger<T>>()`.

## Files

- `DataReaderTests.cs` — `GrpcReaderServiceImpl<Demodto>.ListChanges` over an `InMemoryDataset`: change lists, paging and page-token expiry, `PutAll` semantics.
- `DataReaderCountChildrenTests.cs` — `GrpcReaderServiceImpl<Demodto>.CountChildren`: descendants, root and empty parent, odd characters, cancellation, 10,000 children.
- `HistoryDeduplicationTests.cs` — `GrpcReaderServiceImpl<Demodto>.ListChanges` after create/update/delete sequences; the summary frames it as `DeletedNames` vs `DeletedEntities` deduplication.
- `HierarchicalIndexReaderTests.cs` — a dataset whose first deltas are legacy flat deltas, continued by `DatasetReaderWriter` in the tree format; `DatasetReader` and `DatasetReaderWriter` read across both, and `List`/`CountChildren(null)` mean everything.
- `DatasetReaderReinitializationTests.cs` — `DatasetReader`'s five-second re-initialization never reports an older version than an update notification brought.
- `DatasetUpdateConsumingServiceTests.cs` — `DatasetUpdateConsumingService` with `NotificationsHubSync`: `Notify` returns only after a run that saw the delta; stop and dispose.
- `DistributedLockServiceTests.cs` — `DistributedLockService<T>`: acquire, renew, lose on I/O errors, request-file handover, stop; plus `RxSchedulerClock.Delay`.
- `DtoServerBuilderHealthCheckNamingTests.cs` — `DtoServerBuilder<T>.VersionedEntityName`, on stub `Contoso.*` types declared at the top of the file.
- `InMemoryDatasetAliasTests.cs` — `InMemoryDataset<T>` checking aliases like `DatasetWriter<T>`: conflicts, order within a stream, implicit deletes.
- `InMemoryDatasetLockTests.cs` — every `InMemoryDataset<T>` operation releases its semaphore.
- `InMemoryDatasetWaitChangedNamesTests.cs` — `InMemoryDataset<T>.WaitChangedNames` sees a write that lands as the wait starts (VLOW-30), over 2000 raced rounds.
- `ObjectStorageReaderTests.cs` — `ObjectStorageReader.ListVersions` rebuilds from the object listing when the latest delta that `DeltaInfo.pb` names is missing.
- `PaginationHelperTests.cs` — `PaginationHelper.Paginate` page boundaries.
- `SemaphoreSlimExtensionsTests.cs` — `SemaphoreSlimExtensions.Lock`/`LockAsync`: hold until disposed, release once, cancellation.
- `SharedMethodsTest.cs` — `SharedMethods.ExtractNumber` on object keys.
- `GlobalUsings.cs` — global usings: FluentAssertions, NSubstitute, Xunit, `RxDatasets.Abstractions`, `Google.Protobuf.WellKnownTypes`.
- `RxDatasets.Tests.csproj` — project file; references `RxDatasets`, `RxDatasets.InMemoryDataset` and the `Demodto` gRPC types from `testapps`.
- `RxDatasets.Tests.v3.ncrunchproject` — NCrunch settings; hides one warning.

## Folders

- `Benchmarks/` — opt-in index memory benchmark. See [`Benchmarks/AGENTS.md`](Benchmarks/AGENTS.md).
- `DatasetWriterTests/` — writer contracts, run on `InMemoryDataset`. See [`DatasetWriterTests/AGENTS.md`](DatasetWriterTests/AGENTS.md).
- `Models/` — the immutable index and delta models. See [`Models/AGENTS.md`](Models/AGENTS.md).

## Gotchas

- `BackgroundService` here behaves like the 9.x hosting package: `ExecuteAsync` runs inside `StartAsync` until its first await. Tests that depend on .NET 10 start ordering go in `RxDatasets.StorageTests`.
- `DistributedLockServiceTests` injects `RxSchedulerClock` over a `TestScheduler` as the service's `IClock` and moves time with `Advance`; its start helper still yields with a real `Task.Delay(100)`.
- Several tests run on the wall clock: `DatasetReaderReinitializationTests` spins for 11 seconds; the page-token expiry tests in `DataReaderTests` wait 0.3 s and 1.1 s against a `PaginationCacheDuration` of 1 second (their names say 5); `CountChildren_PerformanceValidation...` asserts under 100 ms; `Models/ImmutableIndexNodeTests.TryGetValue_DoesNotAllocate` asserts zero allocated bytes.
