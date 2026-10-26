# src/RxDatasets/Models

The on-disk protobuf messages and the immutable in-memory index built from them: a tree keyed by entity-name segments,
shared structurally between versions so readers keep the state they hold while a writer builds the next one.

## Files
- `DatasetEntities.proto` — the storage messages: `SnapshotIndex`/`SnapshotNode`/`SnapshotEntity`,
  `DeltaIndex`/`DeltaNode`/`DeltaEntity`/`DeletedEntity`, `AliasRef`, `AliasOnlyChange`, `DeltaInfo`, `SnapshotInfo`,
  `ProcessedVersionsInfo`. Authoritative over `../diskFormat.md` for field names and numbers.
- `Legacy-Entities.proto` — `ObsoleteV1LatestVersion` (`latestVersion.pb`) and `ObsoleteV2VersionInfo`
  (`VersionIndex.pb`), parsed only by the version-discovery fallback in `ObjectStorageReader`.
- `IndexKeys.cs` — `IndexKeys`: the one mapping of a name onto the tree (`Split`, `LeafSegmentCount`, `LeafName`,
  `ParseTemplate`, `Join`, `NextSegment`) plus a capped, process-wide pool of shared template and variable strings;
  `MemoryKeyComparer`.
- `ImmutableIndexNode.cs` — `ImmutableIndexNode<TValue>` (children by collection → id, leaves by template → variable,
  `LiveCount`): `TryGetValue`, `EnumerateAll(prefix, where)`, `CountLive`; `Builder` (`Set`, `Get`, `Remove`, `Child`,
  `SetLeaf`, `ToImmutable`) copies only the touched path. `IIndexValue.IsLive` decides what counts as live.
- `ImmutableDatasetState.cs` — `ImmutableDatasetState(Root, SnapshotVersion, LatestVersion)` with
  `AliasDefinitionsHash` and the reverse alias maps (`Aliases`, `ResolveAlias`, `ListAliases`); `FromProto`/`ToProto`
  for snapshot indexes; internal `AliasMapsBuilder`.
- `ImmutableEntities.cs` — `ImmutableSnapshotEntity` (versions, position, length, `Crc32`, `Aliases`, `IsLive`,
  `ToProto`/`FromProto`); `ImmutableDeltaEntity` (unused); internal `AliasRefExtensions`.

## Gotchas
- **Generated types are public and in the global namespace.** The `.proto` files declare no `csharp_namespace`, and
  `Access="Private"` in the csproj does not make the message classes internal (checked in `obj/`).
- **Two formats, one writer format.** Version fields: legacy number 1, current 32; tree `root` 34; 33 is reserved. The
  flat lists/map are read, never written: writers call `ConvertToTree` on deltas, and `ImmutableDatasetState.ToProto`
  writes only the tree. Every process must be tree-aware before the first tree write (see `../diskFormat.md`).
- **`IndexKeys.LeafSegmentCount` is the on-disk layout.** Changing it changes the tree of every new file.
- **Deleted entities stay in the index** as tombstones (`IsLive == false`); `EnumerateAll` returns them unless `where`
  filters them, and `LiveCount`/`CountLive` exclude them.
- **`LastInsertedVersion` and `LastDeletedVersion` hold every such version**, not only the last. The current change type
  is the one at the highest version, with ties going New, then Changed (`DatasetExtensions.GetLatestChangeType`).
- **`Crc32 == 0` means "no CRC" (legacy)**; writers then treat the entity as changed or compare bytes.
- **Reverse alias maps** are built only from live tree entities; legacy flat snapshot entries carry no aliases.
  `Aliases` is swapped together with `Root`.
- `ImmutableDeltaEntity.ToProto` drops aliases, previous version, tracking id and SLA timestamp; nothing uses it.
