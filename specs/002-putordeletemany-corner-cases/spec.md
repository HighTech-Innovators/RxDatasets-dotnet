# Feature Specification: PUTORDELETEMANY Corner Cases Bug Fix

**Feature Branch**: `002-putordeletemany-corner-cases`  
**Created**: September 17, 2025  
**Status**: Draft  
**Input**: User description: "PUTORDELETEMANY Corner Cases Bug Fix: Fix incorrect change reporting when PUTORDELETEMANY operations contain both delete and put operations for the same entity. Currently reports 'deleted' when it should report 'nothing' (same entity) or 'changed' (modified entity). Need proper handling of put-then-delete, delete-then-put, and multiple operations on same entity within single batch, with comprehensive testing via realistic server setup using testhost."

## Execution Flow (main)
```
1. Parse user description from Input ✓
   → Bug fix for incorrect change reporting in batch operations
2. Extract key concepts from description ✓
   → Identified: batch operations, change detection, entity versioning, test scenarios
3. For each unclear aspect: ✓
   → All scenarios clearly defined with expected behaviors
4. Fill User Scenarios & Testing section ✓
   → Comprehensive test scenarios covering all corner cases
5. Generate Functional Requirements ✓
   → All requirements are testable and specific to bug fix
6. Identify Key Entities ✓
   → Entity operations, change detection, version tracking
7. Run Review Checklist ✓
   → No ambiguities, focused on correct system behavior
8. Return: SUCCESS (spec ready for planning) ✓
```

---

## ⚡ Quick Guidelines
- ✅ Focus on WHAT users need and WHY
- ❌ Avoid HOW to implement (no tech stack, APIs, code structure)
- 👥 Written for business stakeholders, not developers

---

## User Scenarios & Testing *(mandatory)*

### Primary User Story
As a developer using the reactive dataset system, I need batch operations (PUTORDELETEMANY) to correctly report the net effect of changes when multiple operations affect the same entity within a single batch, so that downstream consumers receive accurate change notifications and can maintain consistent data views.

### Acceptance Scenarios
1. **Given** an entity A exists in version Q, **When** I execute PUTORDELETEMANY with (delete A.id, put A with same data), **Then** ListChanges should report 'nothing changed' for entity A
2. **Given** an entity A exists in version R, **When** I execute PUTORDELETEMANY with (delete A.id, put A with different data), **Then** ListChanges should report 'changed A' with the new data
3. **Given** an entity A exists, **When** I execute PUTORDELETEMANY with (put A with changes, put A with original data), **Then** ListChanges should report 'nothing changed' and GET should return original data
4. **Given** an entity A exists, **When** I execute PUTORDELETEMANY with (put A with changes, delete A.id), **Then** ListChanges should report 'deleted A'
5. **Given** no entity B exists, **When** I execute PUTORDELETEMANY with (put B, delete B.id), **Then** ListChanges should report 'nothing changed' for entity B
6. **Given** a batch operation completes, **When** I read data using separate reader instance, **Then** the reader sees the correct net result without internal operation details

### Edge Cases
- **Multiple modifications within single batch**: When the same entity is modified multiple times within a single batch, ultimately the last operation is compared to the 'before' state to determine the net change
- **Alternating put/delete operations**: System handles alternating put/delete operations for the same entity by evaluating the final state against the initial state, regardless of intermediate operations
- **Operations on non-existent entities**: Batch operations referencing entities that don't exist work idempotently - put creates if missing, delete succeeds silently if entity doesn't exist
- **Change detection with identical data**: Change detection uses CRC32 check over the protobuf bytes, stored in the index for each entity ID, to determine if data actually changed even when operations occurred
- **Reader queries during batch processing**: Reader and writer are separated by actual storage layer, so readers see no changes until batch commit is completed AND picked up by the reader side - each call operates as a transaction

## Requirements *(mandatory)*

### Functional Requirements
- **FR-001**: System MUST correctly calculate net effect when batch operations contain both put and delete for same entity
- **FR-002**: System MUST report 'nothing changed' when delete followed by put results in same entity data as before batch
- **FR-003**: System MUST report 'changed entity' when delete followed by put results in different entity data than before batch
- **FR-004**: System MUST report 'nothing changed' when multiple puts for same entity result in original data
- **FR-005**: System MUST report 'deleted entity' when puts followed by delete remove entity from dataset
- **FR-006**: System MUST report 'nothing changed' when put new entity followed by delete in same batch
- **FR-007**: System MUST ensure readers see consistent final state regardless of intermediate batch operations
- **FR-008**: System MUST maintain data integrity when same entity appears multiple times in batch operations
- **FR-009**: System MUST properly sequence operations within batch to determine correct final state
- **FR-010**: System MUST validate change reporting through realistic server scenarios with separate writer and reader instances
- **FR-011**: System MUST ensure change detection works correctly when entity content is identical but operations were performed
- **FR-012**: System MUST handle edge cases where entities are created and deleted within same transaction

### Key Entities *(include if feature involves data)*
- **Batch Operation**: Collection of put and delete operations executed as single unit
- **Entity Change**: Record of how an entity state modified during batch processing
- **Change Report**: Summary of net effects from batch operation for external consumers
- **Entity Version**: Tracking mechanism to identify when entity data actually changes
- **Operation Sequence**: Ordered list of puts and deletes within batch affecting same entity

---

## Review & Acceptance Checklist
*GATE: Automated checks run during main() execution*

### Content Quality
- [x] No implementation details (languages, frameworks, APIs)
- [x] Focused on user value and business needs
- [x] Written for non-technical stakeholders
- [x] All mandatory sections completed

### Requirement Completeness
- [x] No [NEEDS CLARIFICATION] markers remain
- [x] Requirements are testable and unambiguous  
- [x] Success criteria are measurable
- [x] Scope is clearly bounded
- [x] Dependencies and assumptions identified

---

## Execution Status
*Updated by main() during processing*

- [x] User description parsed
- [x] Key concepts extracted
- [x] Ambiguities marked
- [x] User scenarios defined
- [x] Requirements generated
- [x] Entities identified
- [x] Review checklist passed

---