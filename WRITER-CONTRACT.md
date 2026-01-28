# Dataset Writer API Contract

**Version**: 1.0  
**Status**: Living Document  
**Scope**: Write Operations for RxDatasets

---

## Table of Contents

1. [Common Behavioral Contract](#common-behavioral-contract)
2. [API Specifications](#api-specifications)
3. [Acceptance Test Scenarios](#acceptance-test-scenarios)

---

## Common Behavioral Contract

### Response Structure

All write operations return a `WriteResponse` with identical structure:

```protobuf
message WriteResponse {
  uint64 new_dataset_version = 1;  // Resulting dataset version after operation
  uint32 num_added_entities = 2;   // Count of entities created
  uint32 num_updated_entities = 3; // Count of entities modified
  uint32 num_deleted_entities = 4; // Count of entities deleted
  repeated DetailedChange detailed_changes = 5;  // Individual entity change details (optional, requires X-Debug header)
}

enum ChangeType {
  NEW = 0;          // Entity was newly created
  CHANGED = 1;      // Entity was modified
  DELETED = 2;      // Entity was deleted
}

message DetailedChange {
  string entity_id = 1;        // Name of the entity that changed
  ChangeType change_type = 2;  // Type of change (NEW, CHANGED, or DELETED)
}
```

#### Detailed Changes Field (Field 5) - Optional Feature

**Availability**: The `detailed_changes` field is **optional** and populated only when explicitly requested via the **`X-Debug` HTTP header**.

**Header Requirement**:
- Header name: `X-Debug` (case-insensitive, e.g., `x-debug`, `X-DEBUG`)
- Header value: Any non-empty value (value is ignored; presence triggers population)
- Not required: If header is absent, `detailed_changes` is empty
- Multiple values: First value triggers population

**Behavior**:
- When `X-Debug` header is **present**: Response includes detailed change information for each modified entity
- When `X-Debug` header is **absent**: Response does not populate field 5 (defaults to empty list)
- **Backward Compatible**: Existing clients ignoring field 5 are unaffected
- **No Version Change**: This is a non-breaking change; protocol version remains unchanged

**Change Tracking Details**:
- Each entry in `detailed_changes` represents one entity modification
- Three change types are reported: `NEW` (created), `CHANGED` (updated), `DELETED` (deleted)
- Unchanged entities are not included in the list
- For `PutOrDeleteMany` with net effect consolidation, only the final state is reported

**Example Responses**:

*Without X-Debug header* (or with empty detailed_changes):
```protobuf
WriteResponse {
  new_dataset_version: 42
  num_added_entities: 1
  num_updated_entities: 2
  num_deleted_entities: 1
  detailed_changes: []  // Empty because X-Debug header was not provided
}
```

*With X-Debug header*:
```protobuf
WriteResponse {
  new_dataset_version: 42
  num_added_entities: 1
  num_updated_entities: 2
  num_deleted_entities: 1
  detailed_changes: [
    DetailedChange { entity_id: "user/new-user-123", change_type: NEW },
    DetailedChange { entity_id: "user/existing-user-456", change_type: CHANGED },
    DetailedChange { entity_id: "user/another-user-789", change_type: CHANGED },
    DetailedChange { entity_id: "order/old-order-001", change_type: DELETED }
  ]
}
```

**Performance Impact**:
- Without `X-Debug` header: Baseline performance (~<50ms for typical operations)
- With `X-Debug` header: Minimal overhead for change tracking (~<100ms for <1000 entities)
- Large batches (10k+ entities): Response size scales with number of changes, not total batch size

### Universal Behavioral Rules

#### Version Semantics

- **Monotonic Increment**: Dataset version increments by exactly 1 when changes are applied
- **No-Op Stability**: Operations resulting in zero net changes return current version unchanged
- **Atomicity**: All operations within a single call succeed or fail together
- **Sequential Consistency**: Version numbers form a strictly ordered sequence

#### Change Detection

- **Content-Based**: Change detection uses CRC32 comparison of entity byte content
- **Idempotency**: Re-applying identical content reports no change
- **Delete Idempotency**: Deleting non-existent or already-deleted entities is ignored (no-op)

#### Detailed Change Tracking (Optional)

- **X-Debug Header Control**: Detailed change tracking is enabled by the `X-Debug` HTTP header
- **Case-Insensitive Matching**: Header name matching is case-insensitive (`X-Debug`, `x-debug`, `X-DEBUG`)
- **Presence-Based Triggering**: Header presence triggers population regardless of value
- **Optional Field**: Field 5 (`detailed_changes`) is optional; absent header results in empty list
- **Per-Operation**: Change tracking is evaluated per operation call (each Put, Delete, etc.)

#### Entity Naming

- **Validation**: Entity names must be non-null, non-empty strings
- **Optional Regex**: May enforce additional name validation via configuration regex
- **Case Sensitivity**: Entity names are case-sensitive
- **Hierarchical Structure**: Names may use `/` separator for parent-child relationships
  - Format: `{kind}/{id}/{kind}/{id}/...`
  - Ancestors must have even number of path segments

#### Concurrency & Isolation

- **Thread Safety**: All operations are thread-safe via internal locking
- **Reader Isolation**: Readers never observe partial/intermediate operation state
- **Write Serialization**: Concurrent writes are serialized; order is non-deterministic
- **Cancellation Support**: All operations support cancellation via gRPC context cancellation

#### Error Handling

- **Initialization Required**: Operations fail with `FAILED_PRECONDITION` status if writer not initialized
- **Name Validation**: Invalid names fail with `INVALID_ARGUMENT` status
- **Storage Failures**: Underlying storage errors fail with `INTERNAL` or `UNAVAILABLE` status codes
- **Cancellation**: Cancelled operations fail with `CANCELLED` status

#### Performance Characteristics

- **Batch Efficiency**: Batch operations (Many/All) are significantly more efficient than sequential singles
- **Streaming**: Streaming RPC operations process data incrementally, avoiding memory accumulation
- **CRC32 Optimization**: CRC32 comparison avoids byte-level comparison when possible

---

## API Specifications

### 1. Put

**Signature**:

```protobuf
rpc Put(Entity) returns (WriteResponse);

message Entity {
  string name = 1;
  bytes entity_bytes = 2;
}
```

**Behavior**:

- Creates entity if it doesn't exist
- Updates entity if content differs (by CRC32)
- No-op if entity exists with identical content

**Specifics**:

- Single entity operation
- Name validation enforced
- CRC32-based change detection
- **X-Debug Support**: If `X-Debug` header is present, response includes detailed change in `detailed_changes` field

---

### 2. Delete

**Signature**:

```protobuf
rpc Delete(DeleteRequest) returns (WriteResponse);

message DeleteRequest {
  string name = 1;
}
```

**Behavior**:

- Deletes entity if it exists and is not already deleted
- No-op if entity doesn't exist or is already deleted

**Specifics**:

- Single entity operation
- Silent on non-existent entities
- **X-Debug Support**: If `X-Debug` header is present, response includes detailed change in `detailed_changes` field

---

### 3. PutMany

**Signature**:

```protobuf
rpc PutMany(stream Entity) returns (WriteResponse);
```

**Behavior**:

- Upserts multiple entities (create or update)
- Skips entities with identical content
- **No implicit deletes**: Existing entities not in stream remain unchanged

**Specifics**:

- Streaming batch operation
- Name validation per entity (invalid names logged and skipped)
- CRC32-based change detection per entity
- Operations on same entity: last occurrence wins
- **X-Debug Support**: If `X-Debug` header is present, response includes detailed changes for all modified entities

---

### 4. PutAll

**Signature**:

```protobuf
rpc PutAll(stream Entity) returns (WriteResponse);
```

**Behavior**:

- Replaces entire dataset with streamed entities
- **Implicit deletes**: Entities not in stream are deleted
- Upserts entities in stream (create or update)

**Specifics**:

- Streaming batch operation
- Name validation per entity (invalid names logged and skipped)
- CRC32-based change detection
- Entities not in stream but in dataset → deleted
- Operations on same entity: last occurrence wins
- **X-Debug Support**: If `X-Debug` header is present, response includes detailed changes for all modified/deleted entities

---

### 5. PutAllChildren

**Signature**:

```protobuf
rpc PutAllChildren(stream Entity) returns (WriteResponse);
```

**Behavior**:

- Replaces all descendants of parent entities with streamed entities
- **Implicit deletes**: Descendants not in stream are deleted
- Upserts entities in stream (create or update)
- **Parent Extraction**: Determines parent from each entity's name

**Specifics**:

- Streaming batch operation
- Entities without valid parent are skipped
- Parent = entity name with last `/{kind}/{id}` removed
- Deletes descendants of identified parents not present in stream
- Operations on same entity: last occurrence wins
- **X-Debug Support**: If `X-Debug` header is present, response includes detailed changes for all modified/deleted children

---

### 6. DeleteMany

**Signature**:

```protobuf
rpc DeleteMany(stream DeleteRequest) returns (WriteResponse);
```

**Behavior**:

- Deletes multiple entities by name
- Skips non-existent or already-deleted entities

**Specifics**:

- Streaming batch operation
- Silent no-ops for non-existent entities
- Operations on same entity: last delete wins (still results in delete)
- **X-Debug Support**: If `X-Debug` header is present, response includes detailed changes for all deleted entities

---

### 7. DeleteChildren

**Signature**:

```protobuf
rpc DeleteChildren(DeleteChildrenRequest) returns (WriteResponse);

message DeleteChildrenRequest {
  string ancestor = 1;
}
```

**Behavior**:

- Deletes all entities whose names start with `{ancestor}/`
- Ancestor validation: must have even number of path segments
- Skips already-deleted descendants

**Specifics**:

- Prefix-based matching
- Automatically appends `/` to ancestor if not present
- `ArgumentException` if ancestor has odd number of path segments
- Silent no-ops for already-deleted descendants
- **X-Debug Support**: If `X-Debug` header is present, response includes detailed changes for all deleted descendants

---

### 8. PutOrDeleteMany

**Signature**:

```protobuf
rpc PutOrDeleteMany(stream PutOrDelete) returns (WriteResponse);

message PutOrDelete {
  string name = 1;
  bool is_delete = 2;
  bytes entity_bytes = 3;  // null/empty if is_delete=true
}
```

**Behavior**:

- Mixed put and delete operations in single transaction
- **Net Effect Reporting**: Multiple operations on same entity report net effect only
- Delete: `is_delete=true` or `entity_bytes=null`
- Put: `is_delete=false` and `entity_bytes!=null`
- **No implicit deletes**: Only explicit deletes in stream

**Specifics**:

- Streaming batch operation
- Operations on same entity consolidated to final state
- Final state compared to original state for net effect
- Delete non-existent → no-op
- Put identical content → no-op
- Delete then put identical → no-op
- Delete then put different → update
- Put then delete → delete
- Multiple puts → last put determines final state
- **X-Debug Support**: If `X-Debug` header is present, response includes detailed changes (only reporting net effect, not intermediate operations)

**Net Effect Examples**:

```
[Delete(A), Put(A, same)]     → 0 created, 0 changed, 0 deleted
[Delete(A), Put(A, different)] → 0 created, 1 changed, 0 deleted
[Put(A, v1), Put(A, v2), Put(A, original)] → 0 created, 0 changed, 0 deleted
[Put(A, different), Delete(A)] → 0 created, 0 changed, 1 deleted
[Put(B, new), Delete(B)]       → 0 created, 0 changed, 0 deleted  // B didn't exist
```

---

## Acceptance Test Scenarios

### Format

Each scenario describes the **initial state**, **operation**, and **expected published changes**:

- `X created` = entities added (didn't exist or were deleted)
- `Y changed` = entities updated (existed with different content)
- `Z deleted` = entities removed (existed and now deleted)

### Convention

- `version` = dataset version number
- `E(name, data)` = entity with name and content
- `CRC(data)` = CRC32 hash of data

---

### Put Operation

#### AC-PUT-001: Create New Entity

**Initial State**:

```
version: 100
entities: []
```

**Operation**:

```
Put(name="user/123", entity_bytes=[0x01, 0x02, 0x03])
```

**Expected**:

```
version: 101
1 created, 0 changed, 0 deleted
```

---

#### AC-PUT-002: Update Existing Entity

**Initial State**:

```
version: 100
entities: [E("user/123", [0x01, 0x02, 0x03])]
```

**Operation**:

```
Put(name="user/123", entity_bytes=[0x04, 0x05, 0x06])
```

**Expected**:

```
version: 101
0 created, 1 changed, 0 deleted
```

---

#### AC-PUT-003: Put Identical Content (No-Op)

**Initial State**:

```
version: 100
entities: [E("user/123", [0x01, 0x02, 0x03])]
```

**Operation**:

```
Put(name="user/123", entity_bytes=[0x01, 0x02, 0x03])
```

**Expected**:

```
version: 100
0 created, 0 changed, 0 deleted
```

---

#### AC-PUT-004: Resurrect Deleted Entity with Same Content

**Initial State**:

```
version: 100
entities: [E("user/123", [deleted])]
previous_crc32: CRC([0x01, 0x02, 0x03])
```

**Operation**:

```
Put(name="user/123", entity_bytes=[0x01, 0x02, 0x03])
```

**Expected**:

```
version: 101
1 created, 0 changed, 0 deleted
```

**Note**: Deleted entities are treated as non-existent.

---

### Delete Operation

#### AC-DELETE-001: Delete Existing Entity

**Initial State**:

```
version: 100
entities: [E("user/123", [0x01, 0x02, 0x03])]
```

**Operation**:

```
Delete(name="user/123")
```

**Expected**:

```
version: 101
0 created, 0 changed, 1 deleted
```

---

#### AC-DELETE-002: Delete Non-Existent Entity (No-Op)

**Initial State**:

```
version: 100
entities: []
```

**Operation**:

```
Delete(name="user/999")
```

**Expected**:

```
version: 100
0 created, 0 changed, 0 deleted
```

---

#### AC-DELETE-003: Delete Already-Deleted Entity (No-Op)

**Initial State**:

```
version: 100
entities: [E("user/123", [deleted])]
```

**Operation**:

```
Delete(name="user/123")
```

**Expected**:

```
version: 100
0 created, 0 changed, 0 deleted
```

---

### PutMany Operation

#### AC-PUTMANY-001: Create Multiple New Entities

**Initial State**:

```
version: 100
entities: []
```

**Operation**:

```
PutMany([
  E("user/1", [0x01]),
  E("user/2", [0x02]),
  E("user/3", [0x03])
])
```

**Expected**:

```
version: 101
3 created, 0 changed, 0 deleted
```

---

#### AC-PUTMANY-002: Mixed Create and Update

**Initial State**:

```
version: 100
entities: [E("user/1", [0x01]), E("user/2", [0x02])]
```

**Operation**:

```
PutMany([
  E("user/1", [0xFF]),      // update
  E("user/2", [0x02]),      // no-op
  E("user/3", [0x03])       // create
])
```

**Expected**:

```
version: 101
1 created, 1 changed, 0 deleted
```

---

#### AC-PUTMANY-003: No Implicit Deletes

**Initial State**:

```
version: 100
entities: [E("user/1", [0x01]), E("user/2", [0x02]), E("user/3", [0x03])]
```

**Operation**:

```
PutMany([
  E("user/1", [0xFF])
])
```

**Expected**:

```
version: 101
0 created, 1 changed, 0 deleted
```

**Note**: `user/2` and `user/3` remain unchanged.

---

#### AC-PUTMANY-004: Duplicate Names - Last Wins

**Initial State**:

```
version: 100
entities: [E("user/1", [0x01])]
```

**Operation**:

```
PutMany([
  E("user/1", [0x02]),
  E("user/1", [0x03]),
  E("user/1", [0x01])   // back to original
])
```

**Expected**:

```
version: 100
0 created, 0 changed, 0 deleted
```

**Note**: Final state matches original; net effect is no-op.

---

### PutAll Operation

#### AC-PUTALL-001: Replace Entire Dataset

**Initial State**:

```
version: 100
entities: [E("user/1", [0x01]), E("user/2", [0x02])]
```

**Operation**:

```
PutAll([
  E("user/3", [0x03]),
  E("user/4", [0x04])
])
```

**Expected**:

```
version: 101
2 created, 0 changed, 2 deleted
```

**Note**: `user/1` and `user/2` implicitly deleted.

---

#### AC-PUTALL-002: Implicit Deletes with Updates

**Initial State**:

```
version: 100
entities: [E("user/1", [0x01]), E("user/2", [0x02]), E("user/3", [0x03])]
```

**Operation**:

```
PutAll([
  E("user/1", [0xFF]),      // update
  E("user/2", [0x02]),      // no-op
  E("user/4", [0x04])       // create
])
```

**Expected**:

```
version: 101
1 created, 1 changed, 1 deleted
```

**Note**: `user/3` implicitly deleted.

---

#### AC-PUTALL-003: Empty Stream Deletes All

**Initial State**:

```
version: 100
entities: [E("user/1", [0x01]), E("user/2", [0x02])]
```

**Operation**:

```
PutAll([])
```

**Expected**:

```
version: 101
0 created, 0 changed, 2 deleted
```

---

#### AC-PUTALL-004: Identical Content - No Changes

**Initial State**:

```
version: 100
entities: [E("user/1", [0x01]), E("user/2", [0x02])]
```

**Operation**:

```
PutAll([
  E("user/1", [0x01]),
  E("user/2", [0x02])
])
```

**Expected**:

```
version: 100
0 created, 0 changed, 0 deleted
```

---

### PutAllChildren Operation

#### AC-PUTALLCHILDREN-001: Replace Children of Single Parent

**Initial State**:

```
version: 100
entities: [
  E("project/p1/task/t1", [0x01]),
  E("project/p1/task/t2", [0x02]),
  E("project/p2/task/t3", [0x03])
]
```

**Operation**:

```
PutAllChildren([
  E("project/p1/task/t1", [0xFF]),
  E("project/p1/task/t3", [0x03])
])
```

**Expected**:

```
version: 101
1 created, 1 changed, 1 deleted
```

**Note**: Parent `project/p1` detected from entities. Children of `project/p1` are `task/t1` and `task/t2`. Result:

- `task/t1` updated
- `task/t2` deleted (not in stream)
- `task/t3` created under `project/p1`
- `project/p2/task/t3` unaffected (different parent)

---

#### AC-PUTALLCHILDREN-002: Multiple Parents in Stream

**Initial State**:

```
version: 100
entities: [
  E("project/p1/task/t1", [0x01]),
  E("project/p2/task/t2", [0x02])
]
```

**Operation**:

```
PutAllChildren([
  E("project/p1/task/t3", [0x03]),
  E("project/p2/task/t4", [0x04])
])
```

**Expected**:

```
version: 101
2 created, 0 changed, 2 deleted
```

**Note**: Both parents' children replaced.

---

#### AC-PUTALLCHILDREN-003: Entity Without Valid Parent Skipped

**Initial State**:

```
version: 100
entities: []
```

**Operation**:

```
PutAllChildren([
  E("root", [0x01])  // no parent
])
```

**Expected**:

```
version: 100
0 created, 0 changed, 0 deleted
```

---

### DeleteMany Operation

#### AC-DELETEMANY-001: Delete Multiple Existing Entities

**Initial State**:

```
version: 100
entities: [E("user/1", [0x01]), E("user/2", [0x02]), E("user/3", [0x03])]
```

**Operation**:

```
DeleteMany(["user/1", "user/3"])
```

**Expected**:

```
version: 101
0 created, 0 changed, 2 deleted
```

---

#### AC-DELETEMANY-002: Mixed Existing and Non-Existent

**Initial State**:

```
version: 100
entities: [E("user/1", [0x01]), E("user/2", [0x02])]
```

**Operation**:

```
DeleteMany(["user/1", "user/999"])
```

**Expected**:

```
version: 101
0 created, 0 changed, 1 deleted
```

---

#### AC-DELETEMANY-003: All Non-Existent (No-Op)

**Initial State**:

```
version: 100
entities: [E("user/1", [0x01])]
```

**Operation**:

```
DeleteMany(["user/999", "user/888"])
```

**Expected**:

```
version: 100
0 created, 0 changed, 0 deleted
```

---

#### AC-DELETEMANY-004: Duplicate Names in Stream

**Initial State**:

```
version: 100
entities: [E("user/1", [0x01]), E("user/2", [0x02])]
```

**Operation**:

```
DeleteMany(["user/1", "user/1", "user/1"])
```

**Expected**:

```
version: 101
0 created, 0 changed, 1 deleted
```

**Note**: Deleting same entity multiple times results in single delete.

---

### DeleteChildren Operation

#### AC-DELETECHILDREN-001: Delete All Descendants

**Initial State**:

```
version: 100
entities: [
  E("project/p1/task/t1", [0x01]),
  E("project/p1/task/t2", [0x02]),
  E("project/p1/milestone/m1", [0x03]),
  E("project/p2/task/t3", [0x04])
]
```

**Operation**:

```
DeleteChildren(ancestor="project/p1")
```

**Expected**:

```
version: 101
0 created, 0 changed, 3 deleted
```

**Note**: `project/p2/task/t3` unaffected.

---

#### AC-DELETECHILDREN-002: No Matching Descendants (No-Op)

**Initial State**:

```
version: 100
entities: [E("project/p1/task/t1", [0x01])]
```

**Operation**:

```
DeleteChildren(ancestor="project/p999")
```

**Expected**:

```
version: 100
0 created, 0 changed, 0 deleted
```

---

#### AC-DELETECHILDREN-003: Invalid Ancestor Path (Odd Segments)

**Initial State**:

```
version: 100
entities: []
```

**Operation**:

```
DeleteChildren(ancestor="project/p1/task")  // 3 segments = odd
```

**Expected**:

```
INVALID_ARGUMENT status
```

---

#### AC-DELETECHILDREN-004: Ancestor with Trailing Slash

**Initial State**:

```
version: 100
entities: [
  E("project/p1/task/t1", [0x01]),
  E("project/p1/task/t2", [0x02])
]
```

**Operation**:

```
DeleteChildren(ancestor="project/p1/")
```

**Expected**:

```
version: 101
0 created, 0 changed, 2 deleted
```

**Note**: Trailing slash handled correctly.

---

### PutOrDeleteMany Operation

#### AC-PUTORDELETEMANY-001: Mixed Put and Delete Operations

**Initial State**:

```
version: 100
entities: [
  E("user/1", [0x01]),
  E("user/2", [0x02])
]
```

**Operation**:

```
PutOrDeleteMany([
  PutOrDelete(name="user/1", is_delete=false, entity_bytes=[0xFF]),  // update
  PutOrDelete(name="user/2", is_delete=true),                         // delete
  PutOrDelete(name="user/3", is_delete=false, entity_bytes=[0x03])   // create
])
```

**Expected**:

```
version: 101
1 created, 1 changed, 1 deleted
```

---

#### AC-PUTORDELETEMANY-002: Delete Then Put Same Content (No-Op)

**Initial State**:

```
version: 100
entities: [E("user/1", [0x01])]
```

**Operation**:

```
PutOrDeleteMany([
  PutOrDelete(name="user/1", is_delete=true),                        // delete
  PutOrDelete(name="user/1", is_delete=false, entity_bytes=[0x01])  // put back same
])
```

**Expected**:

```
version: 100
0 created, 0 changed, 0 deleted
```

**Note**: Net effect is no change.

---

#### AC-PUTORDELETEMANY-003: Delete Then Put Different Content

**Initial State**:

```
version: 100
entities: [E("user/1", [0x01])]
```

**Operation**:

```
PutOrDeleteMany([
  PutOrDelete(name="user/1", is_delete=true),                        // delete
  PutOrDelete(name="user/1", is_delete=false, entity_bytes=[0xFF])  // put different
])
```

**Expected**:

```
version: 101
0 created, 1 changed, 0 deleted
```

**Note**: Net effect is update.

---

#### AC-PUTORDELETEMANY-004: Multiple Puts Ending with Original

**Initial State**:

```
version: 100
entities: [E("user/1", [0x01])]
```

**Operation**:

```
PutOrDeleteMany([
  PutOrDelete(name="user/1", is_delete=false, entity_bytes=[0x02]),
  PutOrDelete(name="user/1", is_delete=false, entity_bytes=[0x03]),
  PutOrDelete(name="user/1", is_delete=false, entity_bytes=[0x01])  // back to original
])
```

**Expected**:

```
version: 100
0 created, 0 changed, 0 deleted
```

**Note**: Net effect is no change.

---

#### AC-PUTORDELETEMANY-005: Put Then Delete

**Initial State**:

```
version: 100
entities: [E("user/1", [0x01])]
```

**Operation**:

```
PutOrDeleteMany([
  PutOrDelete(name="user/1", is_delete=false, entity_bytes=[0xFF]),
  PutOrDelete(name="user/1", is_delete=true)
])
```

**Expected**:

```
version: 101
0 created, 0 changed, 1 deleted
```

---

#### AC-PUTORDELETEMANY-006: Create Then Delete New Entity (No-Op)

**Initial State**:

```
version: 100
entities: []
```

**Operation**:

```
PutOrDeleteMany([
  PutOrDelete(name="user/999", is_delete=false, entity_bytes=[0xFF]),
  PutOrDelete(name="user/999", is_delete=true)
])
```

**Expected**:

```
version: 100
0 created, 0 changed, 0 deleted
```

**Note**: Entity never existed; net effect is no-op.

---

#### AC-PUTORDELETEMANY-007: Complex Multi-Entity Transaction

**Initial State**:

```
version: 100
entities: [
  E("user/1", [0x01]),
  E("user/2", [0x02]),
  E("user/3", [0x03])
]
```

**Operation**:

```
PutOrDeleteMany([
  PutOrDelete(name="user/1", is_delete=true),                        // delete existing
  PutOrDelete(name="user/1", is_delete=false, entity_bytes=[0xFF]), // re-create different
  PutOrDelete(name="user/2", is_delete=false, entity_bytes=[0x02]), // no-op
  PutOrDelete(name="user/3", is_delete=true),                        // delete
  PutOrDelete(name="user/4", is_delete=false, entity_bytes=[0x04])  // create new
])
```

**Expected**:

```
version: 101
1 created, 1 changed, 1 deleted
```

**Note**:

- `user/1`: updated (delete+put different)
- `user/2`: no change (put identical)
- `user/3`: deleted
- `user/4`: created

---

## Revision History

| Version | Date       | Changes                                |
|---------|------------|----------------------------------------|
| 1.0     | 2026-01-28 | Initial comprehensive contract release |

---

**End of Contract Document**
