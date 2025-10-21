# gRPC Reader Service Contracts for Indexes

**Feature**: Dataset Indexes (Aliases) for Entities  
**Branch**: `004-introduce-the-concept`  
**Date**: October 21, 2025

## Overview

This document defines the gRPC service contract extensions to support index-based reading operations.

---

## Modified Service Definition

The existing gRPC reader service is extended with an optional `indexName` parameter on read operations.

### Protobuf Definition Changes

#### ReadRequest (Extended)
```protobuf
message ReadRequest {
  string path = 1;                    // Entity path (primary ID or alias)
  optional string indexName = 2;      // Index name for routing (optional)
}
```

#### BatchReadRequest (Extended)
```protobuf
message BatchReadRequest {
  repeated string paths = 1;          // Entity paths (primary IDs or aliases)
  optional string indexName = 2;      // Index name for routing (optional)
}
```

#### CountChildrenRequest (Extended)
```protobuf
message CountChildrenRequest {
  string parent_path = 1;             // Parent path (primary ID or alias prefix)
  optional string indexName = 2;      // Index name for routing (optional)
}
```

---

## Service Method Signatures

### Read Operation
**Request**: `ReadRequest { path, indexName? }`  
**Response**: `ReadResponse { data }`  
**Behavior**:
- If `indexName` is null → use direct reader with path as primary ID
- If `indexName` is provided → resolve index, resolve alias via index, read via underlying reader
- If alias not found → return NotFoundException

### BatchRead Operation
**Request**: `BatchReadRequest { paths[], indexName? }`  
**Response**: `BatchReadResponse { data[] }`  
**Behavior**:
- If `indexName` is null → use direct reader with paths as primary IDs
- If `indexName` is provided → resolve aliases via index, batch read via underlying reader
- Non-existent aliases are silently skipped (not included in response)

### CountChildren Operation
**Request**: `CountChildrenRequest { parentPath, indexName? }`  
**Response**: `CountChildrenResponse { count }`  
**Behavior**:
- If `indexName` is null → use direct reader with parentPath as primary ID path
- If `indexName` is provided → resolve index, count children with parentPath prefix, return count
- Returns 0 if no children found

---

## Routing Logic

### Server Implementation Pseudocode

```
OnRead(request):
  if request.indexName is null:
    return directReader.Read(request.path)
  else:
    index = container.GetIndex(request.indexName)
    primaryName = index.ResolvePath(request.path)
    return directReader.Read(primaryName)

OnBatchRead(request):
  if request.indexName is null:
    return directReader.BatchRead(request.paths)
  else:
    index = container.GetIndex(request.indexName)
    primaryNames = request.paths.map(p => index.ResolvePath(p))
    return directReader.BatchRead(primaryNames)

OnCountChildren(request):
  if request.indexName is null:
    return directReader.CountChildren(request.parentPath)
  else:
    index = container.GetIndex(request.indexName)
    return index.CountChildren(request.parentPath)
```

---

## Error Handling

### Possible Errors

| Scenario | Error | HTTP Status | gRPC Status |
|----------|-------|-------------|------------|
| Alias not found in Read | NotFoundException | 404 | NOT_FOUND |
| Index name not registered | NotFoundException | 404 | NOT_FOUND |
| Invalid alias format | InvalidArgumentException | 400 | INVALID_ARGUMENT |
| Underlying reader error | (propagated) | varies | varies |
| Batch with all invalid aliases | Success (empty response) | 200 | OK |

---

## Example: Client Usage

### C# Client

```csharp
// Using index
var response = await client.ReadAsync(new ReadRequest
{
    Path = "items/item1/links/l001",
    IndexName = "links_by_item"
});

// Without index (direct read)
var response = await client.ReadAsync(new ReadRequest
{
    Path = "l001"
    // indexName omitted
});
```

### JavaScript Client

```javascript
// Using index
const response = await client.read({
  path: "items/item1/links/l001",
  indexName: "links_by_item"
});

// Without index
const response = await client.read({
  path: "l001"
});
```

---

## Backward Compatibility

✅ **Fully backward compatible**:
- `indexName` is optional (default: null)
- Existing clients omit `indexName` parameter
- Server defaults to direct reader when `indexName` is null
- No breaking changes to existing operations

---

## Implementation Requirements

1. **Index Registry** at server startup:
   - Collect all registered indexes from DI container
   - Make available to gRPC service handler

2. **Index Lookup**:
   - Resolve index by name safely
   - Handle missing index gracefully (raise NotFoundException)

3. **Alias Resolution**:
   - Call `index.Read()` or `index.CountChildren()` for routed operations
   - Propagate exceptions appropriately

4. **Testing**:
   - Test with indexName = null (direct reader)
   - Test with valid indexName (routed to index)
   - Test with invalid indexName (error handling)
   - Test mixed scenarios (batch with some valid, some invalid aliases)

---

## Future Enhancements

- Support for multiple index routing in single request
- Alias transformation/normalization before resolution
- Metrics/tracing for index-based operations
- Cache-aware routing decisions

---

**Status**: ✅ **Contracts Defined**
