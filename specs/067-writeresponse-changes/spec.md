# Feature Specification: Extend WriteResponse with Optional Change Details

**Feature Branch**: `067-writeresponse-changes`  
**Created**: January 28, 2026  
**Status**: Draft  
**Input**: User description: "Extend WriteResponse with list of changes: make it possible (bool on all put calls, default false) to return a full list of new/changed/deleted ids. This implements gitlab issue #67. For the testhost, control this boolean by the presence of the DEBUG http header (value unimportant)."

**Related**: Spec 002 (PutOrDeleteMany), GitLab Issue #67

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Debugging Write Operations with Detailed Change List (Priority: P1)

A developer is testing the RxDatasets write operations and needs to understand exactly which entities were created, updated, and deleted in a batch operation. Currently, only summary counts are available. The developer wants to retrieve a detailed list of entity IDs that were actually changed during a write operation to validate behavior and debug issues.

**Why this priority**: Critical for debugging and validation during development and testing. The ability to see exact changes enables developers to verify batch operation behavior, identify issues, and validate data consistency. This is essential for the testhost and development workflows.

**Independent Test**: Can be fully tested by making write requests (Put, PutAll, PutMany, PutOrDeleteMany, Delete) with and without the DEBUG header, verifying that detailed change lists are returned only when requested, and delivering the value of enhanced observability into write operation outcomes.

**Acceptance Scenarios**:

1. **Given** a write operation is performed without the `X-Debug` header, **When** the WriteResponse is returned, **Then** the response contains only the summary (version, counts) and no detailed change list
2. **Given** a write operation is performed with the `X-Debug` http header present, **When** the WriteResponse is returned, **Then** the response contains a detailed list of all entity IDs and their change types (New, Changed, Deleted)
3. **Given** a batch operation modifies multiple entities, **When** the response is requested with `X-Debug` header, **Then** the detailed change list distinguishes between newly created entities, updated entities, and deleted entities
4. **Given** a batch operation includes no-op operations (e.g., delete then recreate with same data), **When** the response is requested with `X-Debug` header, **Then** the detailed change list reflects the net effect per spec 002

---

### User Story 2 - Testhost Request/Response Debugging (Priority: P1)

A testhost operator wants to inspect write requests and responses to understand data flow and validate test execution. By including a DEBUG header in requests, the operator can receive enriched responses that show the actual IDs affected by each operation.

**Why this priority**: Critical for testhost operation and end-to-end testing. Provides transparency into which entities are actually affected by operations, enabling test validation and debugging of test scenarios.

**Independent Test**: Can be fully tested by sending write requests to the testhost with DEBUG header, verifying the detailed change information is returned, and enabling testhost operators to validate test data integrity and operation execution.

**Acceptance Scenarios**:

1. **Given** a testhost client sends a write request with `X-Debug` header, **When** the server processes the request, **Then** the response includes detailed change information for all affected entities
2. **Given** a testhost client sends a write request without `X-Debug` header, **When** the server processes the request, **Then** the response contains only summary information to minimize payload size
3. **Given** multiple consecutive write operations in tests, **When** some requests include `X-Debug` header and others do not, **Then** the server correctly includes/excludes detailed changes based on each request's header

---

### Edge Cases

- What happens when a batch operation affects thousands of entities - can the detailed change list be efficiently serialized and transmitted?
- How does the detailed change list behave when some operations succeed and others fail partway through the batch?
- What is the performance impact of collecting and returning detailed change lists for very large batch operations?


## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: WriteResponse message in proto contract MUST be extended with an optional field containing a list of changed entity identifiers and their change types
- **FR-002**: All write operations (Put, PutAll, PutMany, Delete, PutOrDeleteMany) MUST support inclusion of detailed change list in responses when requested
- **FR-003**: The presence of the `X-Debug` HTTP header (with any value) MUST trigger inclusion of detailed change list in the WriteResponse
- **FR-004**: When `X-Debug` header is absent, WriteResponse MUST NOT include the detailed change list (default behavior unchanged)
- **FR-005**: Detailed change list MUST identify each entity by its unique identifier (Name field)
- **FR-006**: For each entity in the detailed change list, the system MUST indicate whether it was newly created, updated, or deleted
- **FR-007**: The detailed change list MUST reflect the net effect of operations per existing spec 002 consolidation rules
- **FR-008**: The change list inclusion MUST be applied consistently across all write operation endpoints (Put, PutAll, PutMany, Delete, PutOrDeleteMany)
- **FR-009**: `X-Debug` header detection in testhost MUST be case-insensitive for the header name (per HTTP header specification)
- **FR-010**: When a write operation produces no changes (no entities affected), the detailed change list (when included) MUST be empty
- **FR-011**: Detailed change list MUST only include entities that actually changed; unchanged entities MUST NOT be included regardless of batch size
- **FR-012**: For a large batch operation where most entities are unchanged, detailed_changes size scales with actual changes, not total batch size

### Key Entities

- **WriteResponse**: Extended proto message containing version, summary counts (num_added_entities, num_updated_entities, num_deleted_entities), and new optional `repeated DetailedChange detailed_changes` field
- **DetailedChange**: Proto message (new) representing one entity change with fields:
  - `string entity_id`: The unique identifier of the changed entity
  - `ChangeType change_type`: Enum value indicating change type (NEW=0, CHANGED=1, DELETED=2)
- **ChangeType**: Proto enum with three values: `NEW = 0; CHANGED = 1; DELETED = 2;`
- **HTTP X-Debug Header**: Request header named "X-Debug" that signals desire for detailed change information; presence is sufficient (value ignored), matching is case-insensitive per HTTP spec

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Write operations without DEBUG header return responses in under 50ms (baseline performance maintained)
- **SC-002**: Write operations with DEBUG header that affect fewer than 1,000 entities return responses in under 100ms
- **SC-003**: All write endpoint tests pass with and without DEBUG header, confirming correct change list inclusion/exclusion
- **SC-004**: Detailed change list for batch operations accurately reflects entity IDs and change types 100% of the time
- **SC-005**: DEBUG header detection works correctly across all major HTTP clients and testing frameworks used with RxDatasets

## Clarifications

### Session 2026-01-28

- Q: What data structure should the `detailed_changes` field in WriteResponse proto message use? → A: Repeated `DetailedChange` message with `entity_id` (string) and `change_type` (enum: NEW, CHANGED, DELETED)
- Q: How should system handle very large batch operations (10k+ entities) with DEBUG header? → A: Include all changed entities in response; IMPORTANT: only report actually-changed entities, explicitly exclude unchanged entities from detailed_changes
- Q: Detailed changes when batch operations partially fail? → A: No partial failures exist by WRITER-CONTRACT atomicity guarantee; all operations within a call succeed or fail together. detailed_changes reflects only successful commit state
- Q: Exact HTTP header name for triggering detailed changes? → A: `X-Debug` (custom header with X- prefix, case-insensitive matching)
- Q: Change type enum values and semantics? → A: Simple enum: `enum ChangeType { NEW = 0; CHANGED = 1; DELETED = 2; }` (no reserved unknown value)

## Assumptions

- DEBUG header value is ignored; presence of the header is sufficient to trigger detailed change collection (following HTTP header semantics where header presence matters)
- "Detailed change list" refers to a structured collection of entity identifiers with their change types, not detailed entity content
- Net effect behavior from spec 002 already consolidates multiple operations on same entity; detailed change list reflects this consolidated net effect
- Testhost and main server code path should both support this feature for consistency
- No breaking changes to WriteResponse contract; new field must be additive (optional in proto3)
