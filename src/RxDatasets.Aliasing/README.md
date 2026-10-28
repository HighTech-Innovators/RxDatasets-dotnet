# RxDatasets.Aliasing - looking entities up by alias

## Overview

An alias is another name for an entity, computed from its data: for example the ESL a link is on
(`t/{tenantid}/s/{storeid}/esls/{barcode}/link`). Each alias name of a dataset maps its alias values to entities.

Aliases are part of the dataset index:

- **Checked on write.** An alias value is unique within its alias name. The writer evaluates the aliases of every
  put, in the order the operations arrive, and rejects a write that would leave an alias owned by two live entities
  with `AliasConflictException` (nothing of the write is stored). A payload the alias factory can't evaluate is
  rejected with `ArgumentException`.
- **Stored in the index.** Deltas store each entity's added and removed aliases, snapshots each entity's full set.
  Nothing is stored or read beyond the index files that are written and read anyway.
- **Read with the index.** `DatasetReader`, `DatasetReaderWriter` and `InMemoryDataset` implement `IAliasIndex`, from
  the same state as their entities. With `WithSharedReaderWriter()` an alias resolves as soon as the write that
  claimed it returns; a read-only replica resolves exactly the aliases of the version it is at.

### Moving an alias to another entity

Free it first: delete the old owner, or change it to no longer have the alias, either earlier in the same
`PutOrDeleteMany` or in an earlier request. Freeing it later in the same request does not count. `PutAll` and
`PutAllChildren` free the aliases of the entities they replace before the first item is checked.

This is stricter than a last-writer-wins store: a client that relied on taking over an alias now gets a conflict.

### Changing the alias definitions

Every delta and snapshot carries a hash of the alias definitions. When a writer starts with other definitions than the
stored hash, it recomputes the aliases of all entities from the data (one sequential pass) before it reports ready, and
stores the result as one delta. An alias that now comes out on more than one entity goes to the entity with the
newest version, then the lowest name; the others lose just that alias. Such conflicts are logged and counted in the
`rxdatasets.aliases.rebuild_conflicts` metric (meter `RxDatasets.Aliases`).

## Registration

From the alias patterns of the DTO (as in the DTOflow templates):

```csharp
services.AddDatasetAliases<Link>(
    Link.Parser,
    ["t/{tenantid}/s/{storeid}/links/{linkid}"],
    ("by_storeesl", "{.id/[..4]}/esls/{.single.esl.barcode}/link"),
    ("by_item", "t/{tenantid}/s/{storeid}/items/{.single.linked_item.item_id}/links/{linkid}"));
```

A pattern combines literal text with placeholders:

- `{tenantid}`: a variable of the id pattern, taken from the entity's id as is.
- `{.field}`: a field of the entity (`{.a.b}`, `{.list[].c}`, `{.id/[..4]}`), as is: one alias per value of a list.
- `{~.field}`: like `{.field}`, but each value is encoded as one id segment (`IdSegment.Encode`): every UTF-8 byte
  outside `A-Z a-z 0-9 - . _` becomes `~` and two lowercase hex digits, so `951a/b` gives `951a~2fb`. Use it for raw
  values, such as codes that may contain `/` or any other character. A `~xx` with lowercase hex is kept, so encoding
  an encoded value changes nothing.

Or with a factory of your own, which gets the entity name and its bytes, and throws on bytes it can't evaluate. The
definition hash must change whenever the factory would give other aliases:

```csharp
services.AddDatasetAlias<Link>("by_storeesl",
    (name, bytes) => [AliasOf(Link.Parser.ParseFrom(bytes.Span))],
    definitionHash: "by_storeesl-v1");
```

The order of registration doesn't matter: the dataset's writer reads the definitions from `AliasOptions<T>`.

## Reading

```csharp
var byStoreEsl = serviceProvider.GetDatasetReader<Link>("by_storeesl");
var link = await byStoreEsl.Get("t/T/s/S/esls/B1/link");
var links = await byStoreEsl.BatchGet(["t/T/s/S/esls/B1/link", "t/T/s/S/esls/B2/link"], null);
var (aliases, version) = await byStoreEsl.List("t/T/s/S/esls", CancellationToken.None);
```

`AliasDatasetReader<T>` is a thin view: it resolves aliases through the dataset reader's `IAliasIndex` and gets the
entities from the dataset reader. `List` and `CountChildren` return the aliases below a parent alias; history is not
available by alias.

`serviceProvider.GetAliasesFor(entityId, dto)` returns the (alias name, alias value) pairs the registered aliases give
a DTO, and none when they can't be evaluated, for validating requests before the write.

## Change notifications

`DatasetUpdateReportingPlugin` calls `IDatasetUpdateReporting<T>.OnAliasChanged` for every alias an entity gains
(`ChangeType.New`) or loses (`ChangeType.Deleted`), also when the entity is deleted and when a definitions change
recomputes the aliases.
