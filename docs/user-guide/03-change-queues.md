# 3. Change queues and consumers

A change queue service (CQS) tells consuming services which records changed. This chapter describes its contract,
how a consumer uses it, and the patterns that keep consumers correct.

RxDatasets doesn't contain a queue service. It calls a **reporter** for every change it stores
([Reporters](08-dataset-lifecycle.md#reporters)), and a queue is fed by such a reporter. What exists:

- The [DataVlow server](10-datavlow-server.md) has an **in-memory** queue plugin (`DataVlow.Server.ChangeQueue.InMemory`).
  It is meant for development and tests: queues live in the memory of one server instance and are lost on restart.
- A production deployment runs a durable queue service with the same contract. Durable queues, retry backoff and
  dead letters, described below, are what such a service adds.

The contract is `change_queue_service.proto`, gRPC service `DtoChangeQueueService`.

## Why a queue, and not plain pub/sub

- **Priority.** A change a user waits for must overtake a nightly batch that was queued earlier. The queue sorts by
  `sla_timestamp`.
- **Batching by scope.** Taking one notification at a time is slow when many records in one scope change together.
  The queue batches notifications that share an id prefix.
- **Explicit acknowledgement.** A notification stays until it is acknowledged. A consumer that crashes gets it again;
  nothing is lost.

## Queues

- **One queue per consuming service**, named after the service and stable across deployments. All instances of the
  service share it, and each notification goes to one instance.
- The service **creates its own queue at startup** with `CreateOrConfigureQueue`. The call is idempotent, and its
  subscription list **replaces** the previous one, so configuring on every start never leaves old subscriptions
  behind.

```proto
message CreateOrConfigureQueueRequest {
  string queue_name = 1;
  repeated DtoTypeSubscription subscriptions = 2;  // replaces the existing subscriptions
  int32 depth = 3;                                 // default prefix depth for batching
}
message DtoTypeSubscription {
  string dto_type   = 1;   // e.g. "order.v2"
  string alias_name = 2;   // optional: only alias-mapping notifications
}
```

## Notifications

```proto
message DtoChangeNotification {
  string ack_token = 1;
  string dto_type  = 2;
  oneof urn {
    string dto_id    = 3;   // this record was created, changed or deleted
    string parent_id = 6;   // something under this parent needs processing
  }
  string alias_name = 7;    // set on alias notifications
  google.protobuf.Timestamp sla_timestamp = 4;
  string tracking_id = 5;
}
```

A notification carries **ids, not data**. That keeps queues small, and a consumer never processes a stale payload:
when it reads, the server has the latest value. A record that no longer exists is the delete signal.

| Form | Meaning |
|---|---|
| `dto_id` | The record was created, changed or deleted. Read it. |
| `dto_id` and `alias_name` | The **alias mapping** at that alias id changed: it points to another record now, or to none. A change to the target record's content doesn't cause one. |
| `parent_id` | Process the group under that parent. Only `ReEnqueue` and `SelfEnqueue` create these; normal writes never do. |

## Priority and batching by prefix

The queue is sorted by `sla_timestamp`, earliest first. The first notification of a `BatchDequeue` is the most
urgent one in the whole queue. The rest of the batch comes from the **same id prefix** as the first, up to
`prefix_depth` segments:

- `prefix_depth = 2`: the same tenant (`t/{tenantId}`)
- `prefix_depth = 4`: the same site (`t/{tenantId}/s/{siteId}`)

Example, a queue in SLA order:

| # | id | SLA |
|---|---|---|
| 1 | `t/acme/s/site01/orders/o-1` | 10:00:01 |
| 2 | `t/acme/s/site02/orders/o-7` | 10:00:02 |
| 3 | `t/acme/s/site01/orders/o-3` | 10:00:05 |

`BatchDequeue(max = 2, prefix_depth = 4)` returns 1 and 3, which share `t/acme/s/site01`. It skips 2, although 2
is more urgent than 3. The consumer can then read both with one `BatchRead` and that parent as the hint.

At an equal SLA, longer ids come before shorter ones. Duplicate notifications for the same record merge while they
wait in the queue.

## Consume, process, acknowledge

```
CreateOrConfigureQueue("order-invoicer",
  subscriptions = [{dto_type: "order.v2"}, {dto_type: "customer.v1"}], depth = 4)

while running:
  batch = BatchDequeue(queue = "order-invoicer", max_notifications = 20,
                       prefix_depth = 4, wait_time_milliseconds = 30000)
  if batch is empty: continue                      // the long poll timed out

  ids     = [n.dto_id for n in batch]
  records = Reader.BatchRead(ids, parent = common_prefix(ids, depth = 4))

  for n in batch:
    if n.dto_id not in records: handle_delete(n)   // not found = deleted
    else: process(records[n.dto_id])

  BatchAcknowledge("order-invoicer", tokens_that_succeeded)
```

- Acknowledge **after** processing. A crash before the acknowledgement means the notification comes back.
- Acknowledging an unknown or already acknowledged token is ignored, so retrying an acknowledgement is safe.
- A notification that isn't acknowledged within its **lease** goes back on the queue. The in-memory queue uses a
  lease of two minutes and puts it back right away. A production queue service adds a backoff per attempt and, after
  its last attempt, moves it to a **dead-letter store**, from which an operator recovers it by hand.
- There is no "nack". A consumer fails a notification by not acknowledging it.

## Handling errors

### Classify per notification

| Class | Examples (gRPC status) | Action |
|---|---|---|
| **Permanent** | `INVALID_ARGUMENT`, `NOT_FOUND` (when the record must exist), `ALREADY_EXISTS`, `PERMISSION_DENIED`, `FAILED_PRECONDITION`, `OUT_OF_RANGE`, `UNIMPLEMENTED`, bad data | Log it with the `tracking_id`, then **acknowledge**: retrying can't help. |
| **Transient** | `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `INTERNAL`, **`CANCELLED`** | Log a warning and **don't acknowledge**: it comes back. |
| **Unknown** | anything else | Treat it as **transient**. If it keeps failing, it ends in the dead-letter store instead of being lost. |

- Walk the exception's inner exceptions: gRPC errors are often wrapped.
- `CANCELLED` is transient. A shutdown in the middle of a write shows up that way, and treating it as permanent
  would acknowledge an aborted write.
- Classify **per notification**, not per batch. One try/catch around the whole batch loses the ability to
  acknowledge the rest.
- After a permanent error the log line is the only trace left, so **always include the `tracking_id`**.

```
ok = []
for n in batch:
  try:
    process(n)                                         // not found on read = delete
    ok.add(n.ack_token)
  catch Permanent e:
    log.error("permanent {id} {type} trackingId={tid}", ...); ok.add(n.ack_token)
  catch Transient e:
    log.warn(...)                                      // not acknowledged: comes back
  catch e:
    log.error("unexpected, treated as transient", ...) // not acknowledged: comes back
BatchAcknowledge(queue, ok)                            // a subset is fine
```

### Batch consumers: the positive acknowledgement list

As soon as a consumer **regroups** a batch (per site, say, so one `BatchRead` serves many notifications), or several
notifications can lead to the **same piece of work**, use these patterns as well.

**Anti-pattern: acknowledge everything at the end.** One try/catch around the batch, then an acknowledgement of
every token, fails silently in two ways. One bad item either throws away the work on the other 99 (none
acknowledged, all redone) or is acknowledged with them (a real change silently dropped).

1. **Keep a positive list.** Add a token only where its work is **provably done**: a committed write, a confirmed
   "nothing to do", or a permanent error. Never start with "all" and subtract the failures.
2. **Acknowledge unroutable notifications right away.** A notification that can't be parsed or routed will never
   succeed. Add it before grouping, or it comes back until it is dead-lettered.
3. **Group, then isolate failures per group.** Give each group its own try/catch. Transient: leave the whole group
   out, so it comes back together. Permanent: log every `tracking_id` of the group and acknowledge it. One group's
   failure doesn't touch another group's tokens.
4. **"Nothing found" is success.** A query that finds nothing to do (the related records are already gone) is a
   result. Acknowledge it. Check every early `return` and `continue`: one that skips "add the token" makes the
   notification come back until it is dead-lettered.
5. **Many tokens, one piece of work.** When several notifications lead to one piece of work, keep a
   `workId -> tokens` map, do the work once, and apply its outcome to **all** of its tokens.
6. **Acknowledge once,** with the final list, after all processing. An acknowledgement can't be taken back if a later
   item fails.

```
for workId, tokens in workToTokens:
  try:   do_work(workId); ok.addAll(tokens)
  catch Transient: log.warn(...)                          // all of them come back together
  catch e:         log.error(workId, trackingIds(tokens), e); ok.addAll(tokens)
BatchAcknowledge(queue, ok)
```

None of this means more calls: it is bookkeeping in memory. Keep reads and writes batched, and isolate failures at
the level of your processing, not of your I/O. The one fair extra call is a fallback on failure: when a bulk write
fails without saying which item caused it, retry the items one by one, only to find the culprit.

**Checklist for a batch consumer**

- [ ] Tokens are collected in a positive list, never as "all minus failures".
- [ ] Unroutable notifications are added before grouping.
- [ ] Each group or piece of work has its own try/catch.
- [ ] Every catch decides permanent or transient, and only transient errors leave the token out. `CANCELLED` is
      transient.
- [ ] Every permanent-error log line has the `tracking_id`s.
- [ ] Every early exit, "nothing to do" included, adds its tokens.
- [ ] Notifications that lead to the same work are grouped by that work.
- [ ] `BatchAcknowledge` is called once, at the end.

## Pattern: alias-watch cleanup

Use this when a consumer owns records whose lifetime depends on a **sibling** record that isn't their parent in the
id hierarchy, so the delete notification carries the wrong key.

Example: `shipment.v1` records are keyed `t/{t}/s/{s}/shipments/{shipmentid}`. A consumer writes `packingslip.v1`
records keyed by order: `t/{t}/s/{s}/orders/{orderid}/packingslips/{n}`. When a shipment is deleted, its
notification carries the shipment id, but the consumer needs the order id.

1. The producer declares an alias that maps each shipment onto an order-keyed id:
   `__alias_name__ by_order __aliaspattern__ t/{tenantid}/s/{siteid}/orders/{orderid}/shipment __aliasexpression__ ...`.
2. The consumer subscribes with `{dto_type: "shipment.v1", alias_name: "by_order"}`.
3. When the shipment is deleted, its alias mapping disappears, and an alias notification arrives with
   `dto_id = t/.../orders/{orderid}/shipment` and `alias_name = by_order`. Now the consumer has the order id.
4. Per batch, the consumer makes **one** `BatchLookup(ids = alias ids, alias_name = "by_order", include_dto =
   false)`. Entries with an empty `id` are the deletes: `DeleteChildren(t/.../orders/{orderid}/packingslips)`.
   Entries with an `id` are still linked and are processed as usual.

The consumer takes the key from `requested_id`. It never rebuilds it from a payload.

## Heavy work: ReEnqueue and SelfEnqueue

### ReEnqueue: split a heavy notification

Some notifications mean far more work than one lease can cover: a change to a shared `taxrules` record affects every
order in a tenant with 500 sites. Doing it inline has two problems. A failure halfway through redoes everything once
the lease runs out, and urgent notifications wait behind a low-priority job.

`ReEnqueue` hands the notification **back** and streams smaller replacements into the **same** queue. The original
is acknowledged when the stream commits.

```proto
rpc ReEnqueue(stream ReEnqueueRequest) returns (ReEnqueueResponse);

message ReEnqueueRequest {
  oneof payload {
    Header    header = 1;  // exactly one, first: queue_name and the original's ack_token
    SplitItem item   = 2;  // zero or more
    Footer    footer = 3;  // exactly one, last: the commit
  }
}
message SplitItem {
  string dto_type = 1;
  oneof urn { string dto_id = 2; string parent_id = 3; }
  string alias_name = 4;
}
message ReEnqueueResponse { bool acknowledged = 1; int32 items_enqueued = 2; }
```

- **Atomic in the safe direction.** The original is acknowledged only after the footer arrives and every item is
  queued. Items without the acknowledgement can happen and are harmless: the original comes back after its lease,
  and the retry queues the items again, which merge with the ones already queued. An acknowledgement without the
  items can't happen. If the client disconnects before the footer, the call ends `CANCELLED`, queued items stay,
  and the original isn't acknowledged.
- **The footer is the commit,** not the end of the stream. That separates "incomplete" from "intentionally empty".
- **Inheritance.** Items carry no SLA, tracking id or ack token: they take the SLA and tracking id of the original,
  and get a new ack token when they are dequeued.
- **Tiers.** At an equal SLA, longer ids come first, so finer pieces run before the coarser rest. Split into
  `parent_id` items per site first; when such an item is dequeued, split it into `dto_id` items per record. Urgent
  changes slot in between by SLA.
- **Long streams are fine.** The server extends the original's lease while the stream is open (every 10 seconds or
  10,000 items).
- **No items** (header and footer only) is the same as acknowledging the original.

| Condition | Status |
|---|---|
| queue name empty or unknown | `NOT_FOUND` |
| ack token unknown, already acknowledged or expired | `FAILED_PRECONDITION` |
| first message isn't a header, or a second header | `INVALID_ARGUMENT` |
| item without a type, or without `dto_id` and `parent_id` | `INVALID_ARGUMENT` |
| disconnect before the footer | `CANCELLED` (items stay, original not acknowledged) |

### SelfEnqueue: work an operator starts

`SelfEnqueue` puts notifications on the caller's **own** queue without an ack token. It is the building block of
"reprocess this" admin endpoints.

```proto
message SelfEnqueueRequest {
  string queue_name  = 1;                         // your own queue, by convention
  repeated SplitItem items = 2;
  string reason      = 3;                         // required; logged for audit
  string tracking_id = 4;                         // required
  google.protobuf.Timestamp sla_timestamp = 5;    // optional; defaults to now
}
```

- A missing SLA means **now**, because an operator who triggers work wants it done now.
- Retrying is safe: items merge with the ones already queued. Retry on `UNAVAILABLE` and `DEADLINE_EXCEEDED`.
- An unknown queue gives `NOT_FOUND`.

## Anatomy of a consuming service

Almost every service in a DataVlow system has the same shape:

```mermaid
sequenceDiagram
    participant API as Callers
    participant S as Service
    participant DV as DataVlow server
    participant Q as Change queue

    opt the service has an API
        API->>S: request
        S->>DV: Put / Delete owned records
    end
    S->>Q: CreateOrConfigureQueue(own queue, subscriptions) at startup
    loop long poll
        S->>Q: BatchDequeue (most urgent first)
        Q-->>S: notifications (type and id only)
        S->>DV: BatchRead current records
        Note over S: compute; a missing record is a delete
        S->>DV: PutOrDeleteMany owned records (identical writes are no-ops)
        S->>Q: BatchAcknowledge
    end
```

| Rule | What it means |
|---|---|
| **The owner is the writer** | One owner per record type; everyone else only reads it. |
| **One queue per service** | Named after the service, created by it at startup; its subscription list replaces the old one. |
| **Ids, not data** | Read the record again; an id that doesn't resolve is a delete. |
| **Outputs are projections** | Never read your own output to patch it or to decide whether to write. |
| **Just write** | Don't compare before writing; the server drops identical writes. Record types must serialize repeatably. |
| **Priority by SLA** | Set a meaningful `sla_timestamp` on every write. |
| **Classify errors** | Transient: don't acknowledge. Permanent: log with the `tracking_id`, then acknowledge. Unknown: transient. |
| **Acknowledge per item** | A positive list; failures isolated per group; "nothing found" is success. |
| **Split heavy work** | `ReEnqueue` huge fan-outs into tiers. |
| **Carry the tracking id** | On every write and log line. |
| **Large payloads in LFS** | The record holds the content-addressed path. |
| **Don't cache records** | Keep no record payloads in memory between batches: each notification goes to one instance, so it can't invalidate a cache spread over instances. Read what a batch needs with one `BatchRead`. Ids may be cached; LFS files and records whose id contains a content hash never change and can be cached forever. |
| **Keep one instance running** | A pull consumer gets no inbound traffic, so a platform that scales on requests won't wake it. |
| **Forward user tokens** | Use the end user's token when acting for a user, a service token otherwise. |

A service's own documentation then only lists what is particular to it: the record types it **owns** and why, its
**subscriptions** and what it does on each, its **API**, and where it deviates from the rules.

## Large files

Records hold structured data. Megabytes of binary in a record bloat storage, slow reads and make every change heavy.
Large file storage (LFS) keeps such payloads in an object store, and the record holds only a path. It isn't part of
RxDatasets: it is a convention on top of any object store that supports create-only writes.

### Content addressing

A file's name is the **SHA-256 of its bytes** (lowercase hex) plus its extension, so a path always means the same
bytes. That extends "identical writes are no-ops" to files: the same bytes give the same path, the same record, a
no-op write, and nothing happens downstream. It also gives deduplication, safe re-uploads after a crash, and paths
that are permanent cache keys.

```
/{system}/t/{tenantId}/{category}/{sha256}.{ext}    a tenant's file
/{system}/{category}/{sha256}.{ext}                 a global file
```

`{system}` names the part of the system that owns the file (`invoicing`, `media`), which keeps systems apart and
allows per-system access rules. Document the path on the record field:

```proto
message Invoice {
  // __idpattern__ t/{tenantid}/s/{siteid}/orders/{orderid}/invoice
  string id = 1;
  string pdf_path = 2;   // LFS path: /invoicing/t/{tenantid}/invoices/{sha256}.pdf
}
```

### Writing and reading

1. Compute the SHA-256 of the bytes and build the path.
2. Upload with a **create-only** (if-none-match) condition.
3. "Already exists" (HTTP 412 on most object stores) is **success**: the same hash means the same bytes.
4. Then `Put` the record with the path.

Don't check with a GET first: it races and costs a call; the condition is the mechanism. The store should refuse
overwrites. Readers fetch by path and can **cache files forever**. External clients go through a read-only front
that checks the caller's tenant against the `t/{tenantId}` segment.

### Removing files

Writers never delete files. Removing files no record refers to is a platform job, and it must wait: delete only files
that have been unreferenced for longer than a grace period (hours to days), longer than any writer's gap between
upload and `Put`. So a writer must not keep a long-lived cache of "already uploaded" paths: once unreferenced, a file
may be removed, and the cache would skip the upload when the same content comes back.
