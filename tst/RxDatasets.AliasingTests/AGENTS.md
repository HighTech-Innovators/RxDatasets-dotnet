# tst/RxDatasets.AliasingTests

Unit tests for `src/RxDatasets.Aliasing`: the pattern engine in `PatternsAndExpressions` (`DI.cs`) and the `AliasDatasetReader<T>` that reads a dataset by alias. Everything runs in process on `InMemoryDataset<T>`; nothing is mocked.

## Files

- `AliasDatasetReaderTests.cs` — `AliasDatasetReader<T>` over an `InMemoryDataset<TestEntity>` built with four `AliasDefinition`s: resolve, `BatchGet`, alias moves, deletes, `List`/`CountChildren` by alias parent, a 1111-entity count.
- `AliasDatasetReaderDITests.cs` — `AddDatasetAlias<T>` registration on `RxDatasets.InMemoryDataset.DI.ConfigureServices`, `GetDatasetReader<T>(aliasName)`, and `GetAliasesFor`.
- `CreateAliasFactoryDelegateTests.cs` — `PatternsAndExpressions.CreateAliasFactoryDelegate`: id-pattern variables, `{.field}` paths, `[]` array iteration, `/[..N]`, `/[M..N]` slicing, multi-pattern routing, on the `Relation` message.
- `EvaluateJsonPathTests.cs` — the path navigation and slicing behind the factory, driven through `CreateAliasFactoryDelegate`.
- `ExtractValueSlicingTests.cs` — the internal test hook `PatternsAndExpressions.TestExtractValueSlicing`.
- `StrictAliasFactoryTests.cs` — `CreateStrictAliasFactory` (throws on unparsable bytes), `DefinitionHash` (order-insensitive), and `AddDatasetAliases` rejecting a conflicting write with `AliasConflictException`.
- `testentity.proto` — `TestEntity { id, repeated aliases }`, generated `Internal` into `RxDatasets.AliasingTests.Types`.
- `relation.proto` — `Relation` with a `single` / `multi_product` / `multi_device` oneof, generated `Internal` into `RxDatasets.AliasingTests.Types.Relation`; the shape the pattern tests navigate.
- `RxDatasets.AliasingTests.csproj` — project file; compiles both protos with `Grpc.Tools`.

## Gotchas

- `AliasDatasetReaderTests` and `AliasDatasetReaderDITests` take aliases from `TestEntity.aliases` through a hand-written extractor; the pattern files take them from `Relation` through `PatternsAndExpressions`. Pick the message that matches the layer under test.
- An alias on `InMemoryDataset` resolves as soon as the write returns, so these tests have no waits or timeouts.
