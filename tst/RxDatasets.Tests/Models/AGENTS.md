# tst/RxDatasets.Tests/Models

Pure unit tests of the index models in `src/RxDatasets/Models` and the protobuf index types: no I/O, no mocks.

## Files

- `IndexKeysTests.cs` — `IndexKeys.Split`: a name becomes a node path of (fixed, variable) segment pairs plus a leaf template and variable (`t/T/s/S/items/I/values` → path `t,T,s,S`, template `items/{}/values`, variable `I`); round-trips every real id and alias pattern; shared template and variable instances.
- `ImmutableIndexNodeTests.cs` — `ImmutableIndexNode<T>`: set and lookup, copy-on-write builder, live counts, `EnumerateAll` and `CountLive` matching the legacy flat `List`, zero-allocation lookup.
- `ImmutableDatasetStateTests.cs` — `ImmutableDatasetState`: reading legacy flat and tree snapshots, writing only the tree, applying flat and tree deltas without touching the original state, `ConvertToTree`.
- `DeltaIndexEnumerationTests.cs` — `DeltaIndex` and snapshot enumeration (`EnumerateNewEntities`, `FindChange`, history entries without duplicates) over flat and tree deltas alike.
- `AliasStateTests.cs` — aliases in the index: `AddedAliases` diffs in deltas, full sets and the definitions hash in snapshots, the reverse map in `ImmutableDatasetState`.

## Gotchas

- `[Theory]` cases with a `bool tree` parameter run once on a legacy flat delta and once on the same delta after `ConvertToTree` and a protobuf round trip; a new enumeration test covers both formats the same way.
- `IndexKeysTests.RealPatterns` copies every `__idpattern__` and `__aliaspattern__` from the DTOflow protos (`data-platform/protos/*.proto`, outside this repo); refresh it by hand when those patterns change.
