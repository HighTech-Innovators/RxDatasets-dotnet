# src/RxDatasets.Aliasing

Alias lookups for a dataset: alias-name registration, the `AliasDatasetReader<T>` view, and the alias pattern
language. Concepts, write rules (uniqueness, conflicts, definition changes) and usage live in
[`README.md`](README.md); read it before changing alias behaviour. The checking and storing itself is in core
(`AliasStaging`, `DatasetWriter`).

## Files

- `DI.cs` — two classes.
  - `DI`: `AddDatasetAlias<T>(aliasName, aliasFactory, definitionHash)` adds an `AliasDefinition` to
    `AliasOptions<T>` (read by the core writer and by `InMemoryDataset`) and registers a keyed `IDatasetReader<T>`
    plus a non-keyed `AliasDatasetReader<T>`; `AddDatasetAliases<T>(parser, idPatterns, (aliasName, aliasPattern)...)`
    groups patterns per alias name into one strict factory and hash; `GetDatasetReader<T>(aliasName)`;
    `GetGrpcReaderService<T>(name)`.
  - `PatternsAndExpressions`: `CreateStrictAliasFactory`, `DefinitionHash`, `EvaluateAliasPattern`,
    `ExtractPatternVariables`, the field-path evaluator with array and value slicing, and the older
    `CreateAliasFactoryDelegate` and `EvaluateIdPatternExpression`.
- `AliasDatasetReader.cs` — `IDatasetReader<T>` view resolving aliases through the source reader's `IAliasIndex`;
  history methods throw; internal `EvaluateAliases` feeds `GetAliasesFor`.
- `AliasingExtensions.cs` — `IServiceProvider.GetAliasesFor<T>(entityId, dto)`: factory output of every registered
  alias name, empty where evaluation fails.
- `README.md` — user documentation.
- `RxDatasets.Aliasing.csproj` — references core; `InternalsVisibleTo RxDatasets.AliasingTests`.

## Gotchas

- The registered `IDatasetReader<T>` must also implement `IAliasIndex` (core `DatasetReader`, `DatasetReaderWriter`,
  `InMemoryDataset`). Otherwise the alias reader throws `ArgumentException` on first resolve, not at registration.
- Two factories, two contracts. `CreateStrictAliasFactory` (behind `AddDatasetAliases`) takes id variables from the
  entity name and throws on bytes it can't evaluate, so the writer rejects the write. `CreateAliasFactoryDelegate`
  reads the message's `Id` property by reflection and swallows errors; only tests call it.
- The strict factory returns duplicates; core `AliasStaging` drops them.
- The id pattern is chosen by `/`-segment count alone: the first pattern with as many segments as the entity name.
- Field paths map snake_case segments to PascalCase properties by reflection (`.linked_item.item_id` →
  `LinkedItem.ItemId`), allow one array operator per path, and drop the whole alias pattern for an entity when a
  field is missing or empty.
- A custom factory keeps stale stored aliases until its `definitionHash` changes; the writer recomputes only on a hash
  mismatch.
- `GetGrpcReaderService<T>` looks up a keyed `GrpcReaderServiceImpl<T>`, which nothing in the repo registers keyed.
- `TailHistory` throws `NotImplementedException`; the other history methods throw `InvalidOperationException`.
