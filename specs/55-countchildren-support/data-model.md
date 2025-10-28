# Data Model: CountChildren Support

**Feature**: CountChildren Support for ReaderService  
**Date**: October 28, 2025  
**Phase**: 1 - Design & Contracts

## Core Entities

### CountChildrenRequest

**Purpose**: Encapsulates the request parameters for counting child entities.

**Fields**:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| parent | string | Yes | The parent path for which to count child entities |

**Validation Rules**:

- `parent` must be a valid non-null string
- Empty string interpreted as root-level parent
- Malformed paths will result in RpcException with InvalidArgument status
- Special characters permitted (determined by storage provider validation)

**State**: Stateless request object

---

### CountChildrenResponse

**Purpose**: Returns the count of child entities for a given parent.

**Fields**:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| count | uint32 | Yes | The number of child entities under the specified parent |

**Semantics**:

- Value range: 0 to 4,294,967,295
- Count of 0 returned for: non-existent parents, parents with no children
- Reflects current dataset state at time of query
- No pagination or filtering applied

**State**: Stateless response object

---

### IDatasetReader Interface Extension

**New Method**:

```csharp
Task<uint> CountChildren(string parent, CancellationToken cancellationToken = default);
```

**Parameters**:

- `parent` (string): The parent path to count children for
- `cancellationToken` (CancellationToken): For cancellation support

**Returns**: uint - The count of direct children under the specified parent

**Exceptions**:

- `ArgumentException`: For malformed parent paths
- `OperationCanceledException`: If operation is cancelled
- Other storage-provider specific exceptions

**Semantics**:

- Returns count reflecting current dataset state
- Does not traverse hierarchically (counts direct children only)
- Non-existent parent returns 0
- Consistent with other IDatasetReader methods in async pattern

---

## Relationships and Dependencies

### Data Model Relationships

```
CountChildrenRequest
    ├── parent path string
    └── maps to entry in dataset hierarchy

CountChildrenResponse
    ├── count result
    └── represents child count at query time

IDatasetReader<T>
    ├── existing interface extended with CountChildren method
    └── implementation delegates to storage provider List() method
```

### Validation Rules

| Rule | Scope | Action |
|------|-------|--------|
| Non-null parent | Input validation | Allow empty string (root), reject null |
| Parent format | Storage provider dependent | Delegate to existing validation |
| Count range | Output validation | uint32 ensures valid range |
| Concurrent modifications | Consistency | Return snapshot at query time |

---

## State Transitions

**Query Lifecycle**:

1. Client submits CountChildrenRequest with parent path
2. GrpcReaderServiceImpl validates request
3. Delegates to IDatasetReader.CountChildren()
4. Storage provider lists names for parent (no entity data fetched)
5. Count calculated from name list length
6. CountChildrenResponse returned with count value
7. Response delivered to client

**Error States**:

- Malformed parent → RpcException(InvalidArgument)
- Count exceeds uint32 → RpcException(OutOfRange)
- Cancelled operation → RpcException(Cancelled)
- Service shutdown → RpcException(Unavailable)

---

## Integration Points

### With IDatasetReader Implementations

All implementations of IDatasetReader<T> must implement CountChildren:

- Uses existing `List(parent, cancellationToken)` method
- Leverages storage provider name listing capability
- No new storage provider interface changes required

### With gRPC Service Layer

- GrpcReaderServiceImpl<T>.CountChildren() maps gRPC request to interface method
- Error handling converts exceptions to RpcException
- Follows same cancellation and lifetime patterns as other methods
