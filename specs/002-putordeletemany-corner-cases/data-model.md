# Data Model: PUTORDELETEMANY Corner Cases Bug Fix

**Phase**: 1 - Design & Contracts  
**Date**: September 17, 2025  
**Status**: Complete

## Overview

This bug fix enhances existing data model behavior without introducing new entities. The focus is on improving batch operation processing logic to correctly calculate net effects of multiple operations on the same entity.

## Enhanced Entity Behavior

### Batch Operation Processing

**Entity**: `DatasetWriter<T>.Delta` (existing record)  
**Enhancement**: Add operation consolidation logic  
**Fields**:
- `Changes`: Array of operations to be consolidated per entity ID
- `DatasetVersion`: Target version for the batch
- **New**: `ConsolidatedChanges`: Net effect calculation per entity

**Validation Rules**:
- Multiple operations on same entity ID are allowed
- Final state determination follows operation sequence
- CRC32 comparison determines actual changes
- Idempotent behavior: put on non-existent creates, delete on non-existent succeeds

### Change Detection Enhancement

**Entity**: Change calculation logic (not a new entity)  
**Fields**:
- `InitialCRC32`: Entity state before batch operation
- `FinalCRC32`: Entity state after all operations applied
- `NetChangeType`: Calculated result (None, Changed, Deleted)

**State Transitions**:
```
InitialState + Operations → FinalState
Compare(InitialCRC32, FinalCRC32) → NetChangeType

NetChangeType Rules:
- Same CRC32 → None (no change reported)
- Different CRC32 → Changed 
- No final entity → Deleted
- No initial entity + no final entity → None
```

## Operation Consolidation Logic

### Entity Operation Sequence

**Process**: Within single batch, operations on same entity are consolidated

**Algorithm**:
1. Group operations by entity ID
2. For each entity group:
   - Get initial state (if exists)
   - Apply operations in sequence
   - Calculate final state
   - Compare initial vs final CRC32
   - Determine net change type

**Edge Cases**:
- **Create → Delete**: Report 'None' (entity effectively unchanged)
- **Delete → Create (same)**: Report 'None' (net effect is no change)  
- **Delete → Create (different)**: Report 'Changed' (net effect is modification)
- **Update → Update → Original**: Report 'None' (back to original state)
- **Update → Delete**: Report 'Deleted' (final state is deleted)

## Data Integrity Rules

### CRC32-Based Change Detection

**Rule**: Change detection uses CRC32 hash of protobuf-serialized entity bytes  
**Implementation**: Existing `DeltaEntity.crc32` and `SnapshotEntity.crc32` fields  
**Validation**: 
- CRC32 calculated consistently across operations
- Hash comparison determines actual content changes
- Identical hashes indicate no content change regardless of operations performed

### Transaction Isolation

**Rule**: Reader/Writer isolation via storage layer  
**Implementation**: 
- Writers commit batch atomically to storage
- Readers see no changes until commit complete and detected
- Each operation is a transaction boundary

## Backward Compatibility

### API Contract Preservation

**Existing Interface**: `PUTORDELETEMANY` operation signature unchanged  
**Internal Enhancement**: Operation processing logic improved  
**Client Impact**: None - same input/output contract maintained

### Storage Format Compatibility

**File Format**: No changes to disk format (deltas, snapshots, indexes)  
**Serialization**: Uses existing protobuf definitions  
**Migration**: Not required - bug fix only affects processing logic

## Performance Considerations

### Batch Processing Efficiency

**Optimization**: Consolidate operations before applying to storage  
**Benefit**: Reduced I/O operations for multiple operations on same entity  
**Memory**: Temporary grouping structure during batch processing only

### Change Detection Performance

**Mechanism**: Continue using efficient CRC32 comparison  
**Improvement**: Only calculate CRC32 for final state per entity  
**Scalability**: Performance improvement for batches with many operations on same entities

## Testing Data Model

### Test Scenarios Coverage

**Unit Test Entities**:
- `TestEntity`: Simple entity for operation testing
- `BatchOperation`: Test helper for operation sequences
- `ChangeReport`: Expected vs actual change reporting

**Integration Test Entities**:
- Real protobuf entities from testapps
- Storage provider instances (in-memory for testing)
- Reader/Writer separation via testhost

## Validation Rules Summary

1. **Operation Sequencing**: Last operation defines final state
2. **Change Detection**: CRC32 comparison between initial and final states
3. **Idempotent Behavior**: Put/Delete operations handle non-existent entities gracefully
4. **Transaction Atomicity**: Batch operations commit as single unit
5. **Reader Isolation**: Changes not visible until commit and detection complete

This data model enhancement maintains all existing contracts while fixing the incorrect change reporting behavior in batch operations.