# PUTORDELETEMANY Operation Contract

**Interface**: `IDatasetWriter<T>.PutOrDeleteMany`  
**Type**: Internal API Contract (no external API changes)  
**Status**: Enhanced behavior, backward compatible

## Operation Contract

### Input Contract
```csharp
// Existing signature - NO CHANGES
Task<WriteResponse> PutOrDeleteMany(
    (string Name, byte[]? Entity, ChangeType Change)[] operations,
    CancellationToken cancellationToken = default
)
```

**Parameters**:
- `operations`: Array of operations on entities
  - `Name`: Entity identifier (string)
  - `Entity`: Serialized entity bytes (null for delete operations)
  - `Change`: Operation type (Create, Update, Delete)
- `cancellationToken`: Standard cancellation token

**Input Validation**:
- Operations array must not be null or empty
- Entity names must be valid (non-null, non-empty)
- Delete operations must have null Entity
- Create/Update operations must have non-null Entity

### Output Contract
```csharp
// Existing response structure - NO CHANGES
public class WriteResponse
{
    public ulong Version { get; }
    public ChangeReport[] Changes { get; }
}

public class ChangeReport
{
    public string EntityName { get; }
    public ChangeType ReportedChange { get; } // BEHAVIOR ENHANCED
    public uint CRC32 { get; }
}
```

**Enhanced Behavior**:
- `ReportedChange` now reflects NET EFFECT of all operations on same entity
- Multiple operations on same entity consolidated before reporting
- CRC32 reflects final entity state after all operations

## Behavioral Contract Changes

### Before (Current Buggy Behavior)
```
Input:  [Delete(A), Put(A, same_data)]
Output: ReportedChange = Deleted  ❌ INCORRECT
```

### After (Fixed Behavior)
```
Input:  [Delete(A), Put(A, same_data)]
Output: ReportedChange = None     ✅ CORRECT

Input:  [Delete(A), Put(A, different_data)]
Output: ReportedChange = Changed  ✅ CORRECT

Input:  [Put(A, v1), Put(A, v2), Put(A, original)]
Output: ReportedChange = None     ✅ CORRECT

Input:  [Put(A, changes), Delete(A)]
Output: ReportedChange = Deleted  ✅ CORRECT

Input:  [Put(B, new), Delete(B)]  // B didn't exist
Output: ReportedChange = None     ✅ CORRECT
```

## Error Handling Contract

### Existing Error Behavior (Maintained)
- Invalid entity names → ArgumentException
- Null operations array → ArgumentNullException  
- Storage errors → StorageException
- Cancellation → OperationCanceledException

### New Error Scenarios
- None - all error handling remains the same

## Performance Contract

### Timing Guarantees (Maintained)
- Operation completion time remains within existing SLA
- Batch size limits remain the same
- Memory usage patterns unchanged

### Enhancement Benefits
- Improved efficiency for multiple operations on same entity
- Reduced storage I/O through operation consolidation
- Better change detection accuracy

## Concurrency Contract

### Thread Safety (Maintained)
- Multiple concurrent PUTORDELETEMANY operations supported
- Internal locking mechanisms preserved
- Atomic batch processing maintained

### Reader Isolation (Enhanced)
- Readers continue to see consistent state during batch processing
- No intermediate operation results visible to readers
- Commit atomicity preserved across storage layer

## Backward Compatibility Guarantees

### API Compatibility
- ✅ Method signature unchanged
- ✅ Input parameter structure unchanged  
- ✅ Output structure unchanged
- ✅ Exception types unchanged

### Behavioral Compatibility
- ✅ Single operations behavior unchanged
- ✅ Non-conflicting batch operations unchanged
- ✅ Error scenarios unchanged
- ✅ Performance characteristics maintained
- ⚠️  **BREAKING**: Multiple operations on same entity now report net effect (this is the intended bug fix)

## Testing Contract

### Unit Test Requirements
```csharp
[Test] void DeleteThenPutSame_ReportsNoChange()
[Test] void DeleteThenPutDifferent_ReportsChanged()  
[Test] void MultiplePutsToOriginal_ReportsNoChange()
[Test] void PutThenDelete_ReportsDeleted()
[Test] void CreateThenDeleteNew_ReportsNoChange()
```

### Integration Test Requirements
```csharp
[Test] void BatchOperation_ReaderSeesNetResult()
[Test] void ConcurrentBatches_ConsistentResults()
[Test] void StorageProviderCompatibility_AllProviders()
```

This contract defines the enhanced behavior while maintaining full backward compatibility except for the intended bug fix behavior.