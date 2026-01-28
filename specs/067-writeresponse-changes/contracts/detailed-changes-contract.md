# Proto Contract: Extended WriteResponse with Detailed Changes

**Feature**: Spec 067 - Extend WriteResponse with Optional Change Details  
**Proto Package**: `hightechinnovators.dtos.type`  
**Status**: Design Phase  
**Related**: WRITER-CONTRACT.md, Spec 002 (PutOrDeleteMany)

---

## Proto Changes

### 1. Add ChangeType Enum

**File**: `testapps/DTOs/proto/hightechinnovators/dtos/type/writer_service_types.proto`

**Addition**:
```protobuf
enum ChangeType {
  NEW = 0;
  CHANGED = 1;
  DELETED = 2;
}
```

**Position**: Add immediately before or after WriteResponse definition

---

### 2. Add DetailedChange Message

**File**: `testapps/DTOs/proto/hightechinnovators/dtos/type/writer_service_types.proto`

**Addition**:
```protobuf
message DetailedChange {
  string entity_id = 1;
  ChangeType change_type = 2;
}
```

**Position**: Add immediately before WriteResponse definition

---

### 3. Extend WriteResponse Message

**File**: `testapps/DTOs/proto/hightechinnovators/dtos/type/writer_service_types.proto`

**Current Definition**:
```protobuf
message WriteResponse {
  uint64 new_dataset_version = 1;
  uint32 num_added_entities = 2;
  uint32 num_updated_entities = 3;
  uint32 num_deleted_entities = 4;
}
```

**Updated Definition**:
```protobuf
message WriteResponse {
  uint64 new_dataset_version = 1;
  uint32 num_added_entities = 2;
  uint32 num_updated_entities = 3;
  uint32 num_deleted_entities = 4;
  repeated DetailedChange detailed_changes = 5;
}
```

**Change Details**:
- **Field Number**: 5 (next available)
- **Field Type**: `repeated DetailedChange`
- **Optional Semantics**: Implicitly optional in proto3 (repeated fields default to empty list)
- **Breaking**: NO - Backward compatible with existing clients

---

## Service Contract Changes

### Affected RPC Operations

All write operations return WriteResponse with the extended contract:

1. **Put(Entity) → WriteResponse**
   - Existing: Returns version + counts
   - New: Optionally includes detailed_changes if X-Debug header present

2. **Delete(DeleteRequest) → WriteResponse**
   - Existing: Returns version + counts for delete operation
   - New: Optionally includes detailed_changes (single entity) if X-Debug header present

3. **PutAll(stream Entity) → WriteResponse**
   - Existing: Returns version + counts for batch
   - New: Optionally includes detailed_changes for all entities if X-Debug header present

4. **PutMany(stream Entity) → WriteResponse**
   - Existing: Returns version + counts for batch
   - New: Optionally includes detailed_changes for all entities if X-Debug header present

5. **PutOrDeleteMany(stream PutOrDelete) → WriteResponse**
   - Existing: Returns version + counts with net effect consolidation
   - New: Optionally includes detailed_changes (with net effect) if X-Debug header present

### HTTP Header Contract

**Header Name**: `X-Debug`
**Header Value**: Any non-empty string (value ignored, presence is sufficient)
**Matching**: Case-insensitive (per HTTP 2.0 / RFC 7540 specification)
**Semantics**: Triggers inclusion of `detailed_changes` in WriteResponse

**Examples**:
- `X-Debug: true` → detailed_changes included
- `X-Debug: 1` → detailed_changes included
- `X-Debug: ` (empty value) → detailed_changes included
- `x-debug: yes` → detailed_changes included (case-insensitive)
- Header absent → detailed_changes empty

---

## Backward Compatibility Guarantee

### Proto3 Safety

1. **Field Addition**: New field (5) added to existing message
   - ✅ Existing clients ignore unknown field 5
   - ✅ New clients handle empty field gracefully
   - ✅ Wire format compatible

2. **Repeated Field Semantics**
   - ✅ proto3 repeated fields default to empty list
   - ✅ No required fields added
   - ✅ No field numbers changed

3. **Client Compatibility**
   - ✅ Old clients: receive WriteResponse without detailed_changes (uses field 1-4)
   - ✅ New clients: receive WriteResponse with empty detailed_changes (field 5 absent unless X-Debug)

### Service Compatibility

1. **Existing Behavior Preserved**
   - ✅ WriteResponse structure unchanged for field 1-4
   - ✅ num_added/updated/deleted counts unchanged
   - ✅ new_dataset_version semantics unchanged

2. **Performance Impact**
   - ✅ No change when X-Debug header absent
   - ✅ Minimal overhead when present (conditional field population)

---

## Generated Code Impact

### C# Generated Classes

**Writer.cs** (generated from writer_service_types.proto):
```csharp
public enum ChangeType
{
    New = 0,
    Changed = 1,
    Deleted = 2,
}

public sealed class DetailedChange : IMessage<DetailedChange>
{
    public string EntityId { get; set; }
    public ChangeType ChangeType { get; set; }
}

public sealed class WriteResponse : IMessage<WriteResponse>
{
    public ulong NewDatasetVersion { get; set; }
    public uint NumAddedEntities { get; set; }
    public uint NumUpdatedEntities { get; set; }
    public uint NumDeletedEntities { get; set; }
    public RepeatedField<DetailedChange> DetailedChanges { get; } // NEW
}
```

### Notes on Code Generation
- Google.Protobuf handles enum to int mapping automatically
- RepeatedField<T> collection type for the repeated field
- C# property naming: CamelCase conversion from snake_case proto

---

## Testing Contract

### Unit Test Requirements

```csharp
// Test ChangeType enum values
[Test] public void ChangeType_HasCorrectValues()
{
    Assert.Equal(0, (int)ChangeType.New);
    Assert.Equal(1, (int)ChangeType.Changed);
    Assert.Equal(2, (int)ChangeType.Deleted);
}

// Test DetailedChange message
[Test] public void DetailedChange_Serialization()
{
    var change = new DetailedChange { EntityId = "test", ChangeType = ChangeType.New };
    var serialized = change.ToByteArray();
    var deserialized = DetailedChange.Parser.ParseFrom(serialized);
    Assert.Equal("test", deserialized.EntityId);
    Assert.Equal(ChangeType.New, deserialized.ChangeType);
}

// Test WriteResponse with detailed changes
[Test] public void WriteResponse_WithDetailedChanges()
{
    var response = new WriteResponse
    {
        NewDatasetVersion = 42,
        NumAddedEntities = 1,
        NumUpdatedEntities = 0,
        NumDeletedEntities = 0,
    };
    response.DetailedChanges.Add(new DetailedChange { EntityId = "a", ChangeType = ChangeType.New });
    
    Assert.Single(response.DetailedChanges);
}
```

### Integration Test Requirements

- Put operation with/without X-Debug header
- Delete operation with/without X-Debug header
- PutAll/PutMany batch operations with/without X-Debug header
- PutOrDeleteMany with net effect consolidation and X-Debug header
- Case-insensitive header matching (x-debug, X-DEBUG, X-Debug all work)
- Large batch operations (10k+ entities) with X-Debug header
- Verify detailed_changes accurately reflects changes across all storage providers

---

## Proto File Example (Complete)

```protobuf
syntax = "proto3";

package hightechinnovators.dtos.type;

option java_multiple_files = true;
option java_outer_classname = "WriterServiceTypesProto";
option java_package = "com.hightechinnovators.dtos.type";
option csharp_namespace = "Hightechinnovators.Dtos.Type";

enum ChangeType {
  NEW = 0;
  CHANGED = 1;
  DELETED = 2;
}

message DetailedChange {
  string entity_id = 1;
  ChangeType change_type = 2;
}

message WriteResponse {
  uint64 new_dataset_version = 1;
  uint32 num_added_entities = 2;
  uint32 num_updated_entities = 3;
  uint32 num_deleted_entities = 4;
  repeated DetailedChange detailed_changes = 5;
}

message DeleteRequest {
  string name = 1;
}

message PutOrDelete {
  string name = 1;
  bytes entity_bytes = 2;
  bool is_delete = 3;
}
```

---

## Deployment Notes

### Proto Regeneration

After modifying the .proto files, regenerate C# classes:
```bash
# gRPC/protobuf code generation (handled by build system)
dotnet build  # Automatically regenerates proto code

# Or manually via protoc if needed:
protoc --csharp_out=. --grpc_csharp_out=. writer_service_types.proto
```

### Version Compatibility

- **Proto**: Uses proto3 syntax (no version number)
- **NuGet Packages**: Google.Protobuf (latest stable)
- **Generated Code**: Compatible with .NET 8.0+

### Client Library Updates

Clients built against old WriteResponse proto can still:
- Receive WriteResponse without knowing about field 5
- Handle empty detailed_changes gracefully
- Continue operation without changes

---

## Related Specifications

- **WRITER-CONTRACT.md**: Atomic write operation guarantees
- **Spec 002**: PutOrDeleteMany net effect consolidation rules
- **Writer-service.proto files**: Service definitions using WriteResponse
