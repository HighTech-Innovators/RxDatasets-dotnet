# Data Model: Extended WriteResponse with Detailed Changes

**Feature**: Spec 067 - Extend WriteResponse with Optional Change Details  
**Created**: January 28, 2026  
**Proto Package**: `hightechinnovators.dtos.type`

## Entity Definitions

### 1. ChangeType Enum (NEW)

```protobuf
enum ChangeType {
  NEW = 0;          // Entity was newly created in this operation
  CHANGED = 1;      // Entity was modified (content changed) in this operation
  DELETED = 2;      // Entity was deleted in this operation
}
```

**Semantics**:
- `NEW`: Entity did not exist before and was created by this operation
- `CHANGED`: Entity existed and its content was updated (CRC32 differs)
- `DELETED`: Entity was removed from the dataset
- No fourth value for "unchanged" - unchanged entities are excluded from detailed_changes per specification

**Usage**: Indicates the net effect change type for each entity in detailed_changes list

---

### 2. DetailedChange Message (NEW)

```protobuf
message DetailedChange {
  string entity_id = 1;           // Unique identifier of the changed entity
  ChangeType change_type = 2;     // Type of change (NEW, CHANGED, or DELETED)
}
```

**Fields**:
- `entity_id` (string): The entity's unique identifier (same as `Entity.name`)
  - Non-empty string
  - Case-sensitive
  - May contain `/` separators for hierarchical structure
  - Format same as existing entity naming convention

- `change_type` (ChangeType): The type of change made to this entity
  - Enum value (0=NEW, 1=CHANGED, 2=DELETED)
  - Required field (no optional modifier)

**Invariants**:
- One DetailedChange entry per changed entity
- No duplicate entity_ids in the repeated field
- Unchanged entities MUST NOT appear in the list
- List is sorted by entity_id for consistency (implementation detail)

**Relationships**:
- DetailedChange.entity_id corresponds to Entity.name in write operations
- Number of NEW entries ≤ WriteResponse.num_added_entities (same count)
- Number of CHANGED entries ≤ WriteResponse.num_updated_entities (same count)
- Number of DELETED entries ≤ WriteResponse.num_deleted_entities (same count)

---

### 3. WriteResponse Message (EXTENDED)

```protobuf
message WriteResponse {
  uint64 new_dataset_version = 1;          // Existing: dataset version after operation
  uint32 num_added_entities = 2;           // Existing: count of newly created entities
  uint32 num_updated_entities = 3;         // Existing: count of modified entities
  uint32 num_deleted_entities = 4;         // Existing: count of deleted entities
  repeated DetailedChange detailed_changes = 5;  // NEW: Optional detailed change list
}

```

**Changes**:
- **New Field**: `repeated DetailedChange detailed_changes` (field number 5)
  - **Optional**: populated only if X-Debug header present in request
  - **Empty when**: no entities changed OR X-Debug header absent
  - **Proto3 semantics**: repeated fields are naturally optional/absent

**Backward Compatibility**:
- ✅ Existing clients ignore the new field
- ✅ New clients handle absent field gracefully (empty repeated field)
- ✅ No changes to field numbers 1-4
- ✅ No required fields added
- ✅ Proto3 field addition is non-breaking

**Data Consistency Rules**:
1. `sum(DetailedChange entries by change_type) ≤ num_added_entities + num_updated_entities + num_deleted_entities`
   - Detailed changes can never exceed summary counts
   
2. For any batch operation with X-Debug header:
   - `Count of entries with change_type=NEW == num_added_entities`
   - `Count of entries with change_type=CHANGED == num_updated_entities`
   - `Count of entries with change_type=DELETED == num_deleted_entities`
   - (Counts must match exactly when detailed_changes present)

3. Atomicity guarantee (per WRITER-CONTRACT):
   - All entries in detailed_changes represent successfully committed changes
   - No partial failures; if operation fails, entire response failed
   - No entries for attempted-but-failed operations

---

## Serialization Notes

### Proto3 JSON Encoding

When serialized to JSON (e.g., for gRPC JSON/text protocols):

```json
{
  "new_dataset_version": 42,
  "num_added_entities": 3,
  "num_updated_entities": 2,
  "num_deleted_entities": 1,
  "detailed_changes": [
    { "entity_id": "user/123", "change_type": "NEW" },
    { "entity_id": "user/456", "change_type": "NEW" },
    { "entity_id": "user/789", "change_type": "NEW" },
    { "entity_id": "post/111", "change_type": "CHANGED" },
    { "entity_id": "post/222", "change_type": "CHANGED" },
    { "entity_id": "comment/999", "change_type": "DELETED" }
  ]
}
```

### Binary Encoding

- Field 5 uses standard proto repeated field encoding
- No special field encoding
- Each DetailedChange is encoded as nested message

### Size Characteristics

For large batches:
- Minimal overhead: each entry ~(8 + length(entity_id)) bytes
- Example: 10,000 entities with avg 16-char IDs = ~240KB uncompressed
- Scales with actual changes, not batch size (critical optimization)

---

## Storage Provider Implications

### IDiskStorage<TEntity> Interface

No changes required to the storage provider interface. DetailedChange collection happens at:
- **DatasetWriter level**: Aggregate changes from write operations
- **DtoServerBuilder/Testhost level**: Detect X-Debug header and include detailed_changes
- **GrpcReaderServiceImpl level**: Return WriteResponse with detailed_changes when X-Debug present

The storage providers continue to return only summary counts; detailed change tracking is application-level concern.

---

## Change Type Decision Logic

For each entity in a batch operation, the system determines change_type as follows:

1. **If entity did not exist before AND is present after**: `CHANGE_TYPE_NEW`
2. **If entity existed before AND exists after with same content**: NOT included in detailed_changes
3. **If entity existed before AND exists after with different content**: `CHANGE_TYPE_CHANGED`
4. **If entity existed before AND is deleted**: `CHANGE_TYPE_DELETED`
5. **If entity did not exist before AND was deleted (no-op)**: NOT included in detailed_changes

This logic implements the "net effect" consolidation from Spec 002, where multiple operations on the same entity are consolidated into the final net result.

---

## Example Scenarios

### Scenario 1: Mixed Operations without X-Debug Header

**Request**: PutOrDeleteMany([Create(A), Update(B), Delete(C)])
**X-Debug Header**: Absent
**Response**:
```json
{
  "new_dataset_version": 42,
  "num_added_entities": 1,
  "num_updated_entities": 1,
  "num_deleted_entities": 1,
  "detailed_changes": []  // Empty - no header present
}
```

### Scenario 2: Same Operations with X-Debug Header

**Request**: PutOrDeleteMany([Create(A), Update(B), Delete(C)])
**X-Debug Header**: Present (`X-Debug: 1` or `X-Debug: true`)
**Response**:
```json
{
  "new_dataset_version": 42,
  "num_added_entities": 1,
  "num_updated_entities": 1,
  "num_deleted_entities": 1,
  "detailed_changes": [
    { "entity_id": "A", "change_type": "NEW" },
    { "entity_id": "B", "change_type": "CHANGED" },
    { "entity_id": "C", "change_type": "DELETED" }
  ]
}
```

### Scenario 3: Net Effect Consolidation

**Request**: PutOrDeleteMany([Delete(X), Create(X, data)])
**X-Debug Header**: Present
**Response**:
```json
{
  "new_dataset_version": 42,
  "num_added_entities": 0,
  "num_updated_entities": 0,
  "num_deleted_entities": 0,
  "detailed_changes": []  // Net effect: nothing changed
}
```

**Explanation**: Spec 002 consolidates operations. Delete then Create of same entity with same data = no net change.

### Scenario 4: Large Batch with Many Unchanged Entities

**Batch Request**: 10,000 entity operations (only 5 actually change)
**X-Debug Header**: Present
**Response**:
```json
{
  "new_dataset_version": 42,
  "num_added_entities": 2,
  "num_updated_entities": 3,
  "num_deleted_entities": 0,
  "detailed_changes": [
    { "entity_id": "entity/123", "change_type": "NEW" },
    { "entity_id": "entity/456", "change_type": "NEW" },
    { "entity_id": "entity/789", "change_type": "CHANGED" },
    { "entity_id": "entity/999", "change_type": "CHANGED" },
    { "entity_id": "entity/111", "change_type": "CHANGED" }
  ]  // 5 entries despite 10,000 operations - response size scales with actual changes
}
```

---

## Implementation Responsibilities

### Proto Definition
- **File**: `testapps/DTOs/proto/hightechinnovators/dtos/type/writer_service_types.proto`
- **Action**: Add ChangeType enum and DetailedChange message; extend WriteResponse

### Application Level (DatasetWriter, etc.)
- **Responsibility**: Collect detailed changes during write operations
- **Trigger**: Conditional on request headers (X-Debug detection at service layer)

### Service Layer (GrpcReaderServiceImpl, Testhost)
- **Responsibility**: Detect X-Debug header; conditionally populate detailed_changes field
- **Implementation**: Header-aware response wrapper

### Testing
- **Unit**: Each change type transition (NEW, CHANGED, DELETED)
- **Integration**: Full write operations with X-Debug header present/absent
- **All Providers**: Test against each storage provider (Local, S3, Azure, GCS, Minio)
