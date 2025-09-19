# Research: PUTORDELETEMANY Corner Cases Bug Fix

**Phase**: 0 - Outline & Research  
**Date**: September 17, 2025  
**Status**: Complete

## Research Questions & Findings

### Current PUTORDELETEMANY Implementation

**Research Task**: Analyze current batch operation implementation in DatasetWriter.cs

**Decision**: Bug is located in the Delta processing logic within ApplyDelta method  
**Rationale**: Current implementation processes operations sequentially without considering net effect on same entity  
**Alternatives considered**: 
- Rewrite entire batch processing system (too complex)
- Add post-processing step (would impact performance)
- Fix within existing Delta processing (chosen - minimal impact)

**Key Implementation Details**:
- DatasetWriter.cs contains `ApplyDelta` method that processes batch operations
- Each operation creates a DeltaEntity with CRC32 hash for change detection
- Current logic doesn't consolidate multiple operations on same entity
- Change reporting happens at the individual operation level rather than net effect

### Change Detection Mechanism

**Research Task**: Understand existing CRC32-based change detection system

**Decision**: Leverage existing CRC32 mechanism for proper change detection  
**Rationale**: System already has robust change detection, just need to apply it correctly for net effects  
**Alternatives considered**:
- Implement new change detection (unnecessary duplication)
- Use byte-by-byte comparison (less performant than CRC32)
- Compare protobuf objects directly (current CRC32 is more efficient)

**Key Findings**:
- CRC32 calculated over protobuf bytes stored in DeltaEntity and SnapshotEntity
- Hash comparison determines if entity actually changed
- Existing infrastructure can detect identical content even after operations
- Index file stores CRC32 for each entity for efficient lookup

### Test Infrastructure Analysis

**Research Task**: Identify best approach for realistic server testing using testhost

**Decision**: Use existing testapps/Host and TestClient for realistic scenarios  
**Rationale**: Already provides writer/reader separation via actual file storage  
**Alternatives considered**:
- Docker-based integration tests (explicitly not wanted per requirements)
- In-memory testing (doesn't test real file I/O separation)
- New test harness (testhost already exists and works)

**Key Infrastructure**:
- testapps/Host provides realistic server environment
- testapps/TestClient can simulate reader scenarios
- Tests can verify writer/reader coupling via actual files-on-disk
- Existing test patterns in tst/ can be extended for unit tests

### Batch Operation Logic Analysis

**Research Task**: Understand how multiple operations on same entity should be processed

**Decision**: Implement operation consolidation within batch processing  
**Rationale**: Last operation defines final state, compare against initial state for net effect  
**Alternatives considered**:
- Process all operations and filter results (less efficient)
- Change API to prevent multiple operations on same entity (breaking change)
- Add validation to reject conflicting operations (doesn't solve the use case)

**Key Logic Requirements**:
- Group operations by entity ID within batch
- Apply operations in sequence to determine final state
- Compare final state CRC32 with initial state CRC32
- Report net effect: 'nothing', 'changed', or 'deleted'
- Handle edge cases: create-then-delete, multiple updates, non-existent entities

## Implementation Approach

Based on research findings, the fix approach is:

1. **Enhance Delta Processing**: Modify DatasetWriter.cs to consolidate operations per entity
2. **Net Effect Calculation**: Compare final entity state against initial state using CRC32
3. **Preserve Existing API**: No breaking changes to PUTORDELETEMANY interface
4. **Comprehensive Testing**: Unit tests for logic + integration tests via testhost

## Technical Dependencies Confirmed

- **Google.Protobuf**: For entity serialization and CRC32 calculation ✅
- **System.Reactive**: For existing reactive patterns ✅
- **xUnit/MSTest**: For test framework ✅
- **testhost infrastructure**: For realistic server testing ✅

## Risk Assessment

**Low Risk**: 
- Fix is contained within existing logic
- No API changes required
- Extensive test coverage planned
- Existing CRC32 mechanism is reliable

**Mitigation**:
- Comprehensive test scenarios cover all edge cases
- Realistic testing via testhost ensures real-world compatibility
- Backward compatibility maintained throughout

## Next Steps

Research complete - ready for Phase 1 design and contract generation.