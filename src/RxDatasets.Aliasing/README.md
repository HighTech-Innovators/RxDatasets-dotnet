# RxDatasets.Aliasing - looking entities up by alias

## Overview

An alias is another name for an entity, computed from its data: for example the customer an order belongs to
(`t/{tenantid}/s/{siteid}/customers/{customerid}/orders/{orderid}`). Each alias name of a dataset maps its alias values to entities.

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

From alias patterns, as declared in a record type's proto:

```csharp
services.AddDatasetAliases<Order>(
    Order.Parser,
    ["t/{tenantid}/s/{siteid}/orders/{orderid}"],
    ("by_customer", "t/{tenantid}/s/{siteid}/customers/{.customer_id}/orders/{orderid}"),
    ("by_item", "t/{tenantid}/s/{siteid}/items/{.lines[].sku}/orders/{orderid}"));
```

Or with a factory of your own, which gets the entity name and its bytes, and throws on bytes it can't evaluate. The
definition hash must change whenever the factory would give other aliases:

```csharp
services.AddDatasetAlias<Order>("by_customer",
    (name, bytes) => [AliasOf(Order.Parser.ParseFrom(bytes.Span))],
    definitionHash: "by_customer-v1");
```

The order of registration doesn't matter: the dataset's writer reads the definitions from `AliasOptions<T>`.

## Reading

```csharp
var byCustomer = serviceProvider.GetDatasetReader<Order>("by_customer");
var order = await byCustomer.Get("t/T/s/S/customers/C1/orders/O1");
var orders = await byCustomer.BatchGet(["t/T/s/S/customers/C1/orders/O1", "t/T/s/S/customers/C1/orders/O2"], null);
var (aliases, version) = await byCustomer.List("t/T/s/S/customers/C1", CancellationToken.None);
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
