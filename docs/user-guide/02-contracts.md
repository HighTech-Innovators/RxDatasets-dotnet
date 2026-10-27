# 2. Service contracts

Every record type, per major version, gets three gRPC services: a **Reader**, a **Writer** and a **History**
service. This chapter describes them as the [DataVlow server](10-datavlow-server.md) serves them. The protos are the
`proto-template` in the DataVlow server repository: the `__dtoname__/__dtoversion__/*_service.proto` files, plus the
shared request and response types in `type/*.proto`.

A server you write yourself on RxDatasets doesn't have to use these protos, but every operation below maps directly
onto `IDatasetReader<T>`, `IDatasetWriter<T>` and `GrpcReaderServiceImpl<T>`. [Building a server](04-building-a-server.md)
shows how.

Service names follow `{package}.{type}.v{N}.ReaderService` (`WriterService`, `HistoryService`). The package
prefix is configurable in the DataVlow server, for example `hti.rxdatasets.datavlow.order.v2.ReaderService`.

## Reader

The reader is stateless and read-only.

| RPC | What it does |
|---|---|
| `Read(id, alias_name?)` | One record. `NOT_FOUND` if it doesn't exist, which includes after a delete. |
| `BatchRead(ids, parent?, alias_name?)` | Many records. **Missing ids are left out of the response**, so compare with the request to find deletes. `parent` is a hint when all ids share an ancestor. |
| `ReadChildren(parent, page_size, page_token, alias_name?)` | The records under a parent, paged. |
| `ListChildren(parent, page_size, page_token, alias_name?)` | The **ids** under a parent, paged. |
| `CountChildren(parent, alias_name?)` | The number of records under a parent, without reading them. |
| `BatchLookup(ids, alias_name?, include_dto)` | Which ids exist and what they resolve to. See below. |
| `BatchListChildren(parents, ...)` | The children of many parents in one paged call. |
| `BatchCountChildren(parents, alias_name?)` | One count per parent, in request order. |
| `List(parent, page_size, page_token)` | Deprecated: use `ReadChildren`. |

"Under a parent" means every record whose id starts with the parent's segments, at any depth, plus a record whose id
is the parent itself. An empty parent means the whole dataset.

### Paging

- **Every listing call is paged, whether or not you set `page_size`.** Without it you get one page of the default
  size, not everything. **Follow `next_page_token` until it is empty.**
- A page can hold fewer items than `page_size` while more remain, for example when a byte budget ends the page. Only
  an empty token means the end.
- Keep every other request field the same while you follow a token.
- Page tokens are kept in the memory of the server instance that issued them, for `PaginationCacheDuration`
  seconds (default 60). An expired token, or one sent to another instance, fails with `FAILED_PRECONDITION`. Start
  the listing again when that happens.
- Don't depend on the exact limits. The DataVlow server's defaults: page size 500 (at most 1000), at most 1000 ids or
  parents per batch call, at most 500 records or 5000 ids per page, and a page budget of about 3 MiB.

### BatchLookup: existence and alias resolution

- It returns one entry per requested id, **in request order**. Duplicates are kept.
- `requested_id` echoes the id you asked for, and `id` is the canonical id. **An empty `id` means not found.**
- With `include_dto = false` (the default) only the indexes are read, which makes it the cheap way to ask "does
  this exist?".
- The alias index and the records are read from one consistent state, so a write in between can't drop or re-point
  a result.

Use `BatchLookup` for existence checks and alias resolution, and `BatchRead` when you only want the records that
exist.

### BatchListChildren

- Pages walk the parents in request order, and `page_size` counts children across all parents.
- A parent that is split across pages ends its page with `incomplete = true`. The next page starts with the same
  parent again, possibly with zero children.
- A parent without children still appears, as an empty group, so every requested parent appears at least once.
- With `alias_name`, `ids` are alias ids, and the canonical ids are in the records.

## Writer

Every write request carries an `sla_timestamp` and an optional `tracking_id`; see [SLA timestamps](#sla-timestamps)
and [Tracking ids](#tracking-ids). For a streaming call they are given per item.

| RPC | What it does |
|---|---|
| `Put(record)` | Create or replace one record. |
| `Delete(id)` | Delete one record. Deleting a missing id does nothing. |
| `DeleteMany(stream id)` | Delete several records. |
| `DeleteChildren(ancestor)` | Delete everything under an ancestor (a whole site, for example), in one version. The ancestor itself stays. |
| `PutMany(stream record)` | Create or update many records. Deletes nothing. The normal bulk write. |
| `PutOrDeleteMany(stream record or deleted id)` | A mixed stream of puts and deletes. Deletes only what you name. The most flexible write. |
| `PutAllChildren(stream record)` | For every distinct parent in the stream, replace **all** its children with the ones in the stream: "here is site01's full catalogue". |
| `PutAll(stream record)` | Replace the **whole dataset**: every record not in the stream is deleted. Only for reloading global reference data. |

Every successful call is one atomic change: all of it is stored as one new dataset version, or none of it is. The
response is `WriteResponse { new_version, num_created, num_updated, num_deleted }`. When nothing changed,
`new_version` is the current version and the counts are zero.

Within one streaming call, the last operation per id wins, and when an id appears more than once the earliest SLA is
kept.

Errors a writer can get:

| Status | Cause | What to do |
|---|---|---|
| `UNAVAILABLE` | The dataset is still loading, is reloading, the server is stopping, or this instance doesn't hold the lock | Retry with backoff |
| `INVALID_ARGUMENT` | The id doesn't match the type's id pattern, an alias can't be computed, or an alias doesn't match its pattern | Fix the data |
| `ALREADY_EXISTS` | An alias is already taken by another record | Fix the data |
| `PERMISSION_DENIED` | The token's tenant doesn't own the id, or the caller may not write this type | Fix the caller |

A server that runs several writers on one dataset also sees RxDatasets' `DBConcurrencyException` when another writer
committed first (see [Locking](07-locking-and-deployment.md)). The writer reloads by itself; map it to `ABORTED` or
`UNAVAILABLE` so the client retries.

### Identical writes are no-ops

A put whose bytes equal the stored record changes nothing. The server compares a checksum and then the bytes,
stores nothing, doesn't count it in the response, and **sends no change notification**.

This is what makes the whole system idempotent, and it is the server's job, not the consumer's:

- A consumer never reads its own output to decide whether to write. It computes and writes, and an unchanged result
  ends that branch of the chain.
- **Design record types for repeatable serialization:** no protobuf `map` (use a `repeated` message with a key), and
  `repeated` fields in a stable order. See [Record types and versioning](01-concepts.md#record-types-and-versioning).

## History

| RPC | What it does |
|---|---|
| `GetHistory(id, limit, min_version?, alias_name?)` | The changes of one record, newest first. |
| `StreamHistory(parent, min_version, max_version?, alias_name?, wait_if_none)` | The changes of every record under a parent, **newest first**, from `max_version` (or the latest) down to just above `min_version`. `wait_if_none` first waits until the dataset is past `min_version`. |
| `TailHistory(parent, since_version, alias_name?, follow)` | The changes under a parent **oldest first**, starting after `since_version`. With `follow = true` the stream stays open and delivers new changes as they are written. |
| `ReadHistoric(id, version or timestamp)` | The record as it was written at a version. |

```proto
message HistoryEntry {
  uint64 version = 1;                      // the dataset version of this change
  ChangeType change_type = 2;              // NEW | CHANGED | DELETED
  google.protobuf.Timestamp timestamp = 3;
  uint64 previous_version = 4;             // the version that changed this record before, 0 if none
  string id = 5;
  string tracking_id = 6;
  google.protobuf.Timestamp sla_timestamp = 7;
  string dto_type = 8;                     // e.g. "order.v2", so mixed results describe themselves
}
```

History comes from the stored deltas, so it reaches back as far as deltas are kept. With the default
`Cleanup:KeepAllDeltas = true`, that is all the way. If deltas were cleaned, `StreamHistory` stops at the first
missing one and `TailHistory` skips over the gap.

## Following changes

A service that keeps a local view of a record type (a cache, a search index, a projection) needs the current state
and then every change after it. There are three ways to get them.

1. **A change queue (between services, the normal way).** Read the current state with `ReadChildren`, then consume
   your queue. Queues are durable in a production queue service, so a consumer that was down gets what it missed.
   See [Change queues and consumers](03-change-queues.md).
2. **A history cursor (when you need an exact, gap-free feed).** Read the current state, note the version you read
   it at, then call `TailHistory(parent, since_version = v, follow = true)`. Apply each entry, by reading the record
   or with `ReadHistoric` at that version, and store the last version you applied. After a restart, continue from
   it.
3. **A change-feed RPC on the reader.** RxDatasets' `GrpcReaderServiceImpl<T>.ListChanges` (used by the sample
   protos and by several per-type servers) returns the ids that were created, changed and deleted since a version,
   paged:

   ```csharp
   var request = new ListChangesRequest { FromDatasetVersion = lastProcessed, PageSize = 1000 };
   while (true)
   {
       var page = await reader.ListChangesAsync(request, cancellationToken: ct);
       Apply(page.NewEntities, page.ChangedEntities, page.DeletedEntities);   // ids only; BatchGet the records
       if (page.NextPageToken.Length == 0)
       {
           lastProcessed = page.ToDatasetVersion;   // store it only after the last page
           break;
       }
       request.PageToken = page.NextPageToken;
   }
   ```

   With `WaitForChanges = true`, the call waits until the dataset moves past `FromDatasetVersion` (a long poll),
   so a loop around it needs no sleep. The change types are net over the whole range: a record created and then
   deleted inside the range doesn't appear, one deleted and created again appears as changed, and one created and
   then changed appears as created. A `FromDatasetVersion` beyond the latest version fails with `INVALID_ARGUMENT`.
   This keeps working after cleaning, because the index remembers per record the version of its last change, and
   snapshots keep deleted records as tombstones.

Whichever you use: **a stored version is only meaningful for the dataset that issued it.** If a dataset is ever
recreated from scratch (an in-memory test server that restarts, a wiped bucket), its versions start again at 1, and
every consumer's cursor is wrong. Reset the consumers too.

## SLA timestamps

`sla_timestamp` says **when the change must have been processed**. Change queues deliver in SLA order, earliest
first:

- a timestamp near now, or in the past, is urgent;
- a timestamp further in the future is processed after more urgent work.

Set it to the deadline that really applies. A user waiting on a screen gets now plus a few seconds. A nightly reload
gets now plus hours. When every write says "now", the queue can't tell urgent work from background work.

- **No SLA on a write.** A missing protobuf `Timestamp` looks like the epoch, which would put it at the head of every
  queue as decades overdue. The server therefore replaces a missing, zero or invalid SLA with **now plus a default
  offset** (`ChangeQueueService:DefaultSlaOffset` in the DataVlow server, two hours by default). An SLA in the recent
  past that a caller really set stays as it is: urgent.
- **Manual and operator writes** should use a far-future SLA (now plus 24 hours, for example), so they queue behind
  real work, and a recognisable tracking id such as `ops-manual-<name>`.
- RxDatasets stores the SLA with the change and passes it to every reporter, so any queue or bus can use it.

## Tracking ids

A `tracking_id` identifies **one operation**: a user action, an API call or a scheduled job. Every write, queue call
and report caused by that operation carries the same id, in every service. It is the only thread through an
asynchronous chain.

- The service that **starts** the operation generates it, normally as a UUID, and never reuses it.
- Every service that takes part **passes it on unchanged**: a consumer copies the incoming notification's
  `tracking_id` onto every write it makes, and logs it.
- At most 128 characters, and no `/` (so a history query can tell a tracking id from a record id).
- It is diagnostic data, **not a credential**. Never authorize on it.
- RxDatasets stores it with each change. It shows up in history entries, in reporters and in change notifications.

### Across synchronous calls

When one service calls another over HTTP, two headers keep two different things apart (the names are a suggestion):

| Header | Value | Who sets it |
|---|---|---|
| `X-Tracking-Id` | The tracking id of an operation | Internal services only. **Remove it at every external ingress.** |
| `X-Transaction-Id` | The client's own correlation id: any value, not unique, never parsed | The client only. Forward it unchanged; never create or replace it. |

Never use a client's transaction id as a tracking id: it isn't guaranteed to be unique. When a service receives a
call:

1. **A valid `X-Tracking-Id` is present:** use it on every write, downstream call and log line. Don't create a new
   one.
2. **Otherwise you are the point of entry**, if you author the operation: you write yourself, or you make two or more
   downstream calls that may write. Create the id on arrival and send it on every downstream call, reads included.
3. **Pass-through** is fine when you only read, or make at most one downstream call that may write: create no id, and
   the downstream service becomes the point of entry.

gRPC calls to the DataVlow server carry `tracking_id` in the request message, not in a header.

A component that **debounces or batches** changes from several operations into one write can't carry each
operation's id. It treats the batch as an operation of its own, with its own id, and logs the ids it combined.

### Registering where an operation started

The contract template also defines an operation tracking service: `RegisterTrackingOrigin` (the point of entry
registers the operation once, right before its first write) and `ReportDtoChangesHandledWithoutWrite` (a consumer
reports notifications that ended without a write, with a reason: `nop`, `data_error` or `merged`). Together they let
a history view show where an operation started and where each branch of it ended. The DataVlow server doesn't serve
this service yet; a deployment that wants it provides it as a separate component.

If you use it, give operations keys from a curated list kept with your contracts: lowercase and domain first
(`order.create`, `order.cancel`, `catalog.resync`), one key per user-visible operation (not per endpoint version),
the same key wherever the same operation starts, and never reuse a retired key.

## Security and tenant isolation

These are the rules the DataVlow server applies when JWT authentication is on (see
[Configuration](10-datavlow-server.md#configuration)). A server you write yourself should apply the same ones.

- **The tenant comes from the caller's token** (a claim), never from the URL or the body. Services build ids from it.
- **The server checks every id again:** the tenant segment of each id that is read or written must be the token's
  tenant. Isolation is by id prefix, so the storage needs no tenant column.
- A **root (super) tenant** may read every tenant and is the only one that may write global records.
- Writes are rejected when the id doesn't match the type's id pattern, or when its tenant isn't the token's tenant.
- Restrict bulk deletes: `PutAll` on a tenant-scoped type can delete other tenants' data, so refuse it there.
- Change queues carry ids only. Isolation is enforced when the record is read from the server.

### Which token to send

All calls take the same JWT as the rest of the system, as gRPC metadata: `Authorization: Bearer {jwt}`.

The server enforces **tenant isolation only**, not roles. Any valid token for tenant X can read every record of
tenant X. Role or scope checks are the calling service's job, and they happen **before** the read.

**A read that serves an end user's request forwards that user's token.** Don't swap in a service (machine-to-machine)
token: the user's real permissions travel only with the user's token, so a role check followed by a service-token
read enforces nothing. It is the most common way a backend-for-frontend's role check becomes decoration.

```csharp
// Wrong: tenant-wide service credentials, although the result goes straight back to this caller.
var order = await orderReader.ReadAsync(new ReadDtoRequest { Id = orderId });

// Right: forward the bearer token the incoming request arrived with.
var order = await orderReader.ReadAsync(new ReadDtoRequest { Id = orderId },
    new Metadata { { "Authorization", $"Bearer {incomingToken}" } });
```

Service-to-service flows (queue consumers, batch jobs) have no user request, so a service token is right there.
A service that keeps an in-memory cache filled with a service token must apply every role check itself on every
read, or the cache becomes the leak.

## HTTP/JSON

A server can annotate the reader with `google.api.http` and serve each RPC as plain HTTP/JSON with the same token,
for example `GET /order.v2/t/acme/s/site01:readChildren`. That helps browser tools and clients without gRPC. The
DataVlow server doesn't serve JSON transcoding; the multi-type server in [Real-world servers](09-real-world-servers.md)
does, with ASP.NET Core's gRPC JSON transcoding.
