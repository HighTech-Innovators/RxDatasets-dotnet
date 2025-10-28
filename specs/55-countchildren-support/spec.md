# Feature Specification: CountChildren Support for ReaderService

**Feature Branch**: `55-countchildren-support`  
**Created**: October 28, 2025  
**Status**: Draft  
**Input**: User description: "Add CountChildren support to the ReaderService (and to the grpc reader_service of testHost as well). Return type is a message with uint32"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Count Children in Dataset (Priority: P1)

API consumers need to efficiently determine the number of child entities under a specific parent without retrieving all the entities themselves. This enables pagination planning, UI display of total counts, and resource allocation decisions.

**Why this priority**: This is the core functionality requested and provides immediate value for applications that need to display counts or plan pagination strategies.

**Independent Test**: Can be fully tested by calling the CountChildren API with a valid parent path and verifying it returns the correct count as a uint32 value.

**Acceptance Scenarios**:

1. **Given** a parent with 5 child entities, **When** CountChildren is called with that parent path, **Then** the response contains count = 5
2. **Given** a parent with no child entities, **When** CountChildren is called with that parent path, **Then** the response contains count = 0
3. **Given** a non-existent parent path, **When** CountChildren is called, **Then** the response contains count = 0

---

### User Story 2 - gRPC Integration in TestHost (Priority: P2)

Test applications and integration tests need access to the CountChildren functionality through the gRPC ReaderService in the testHost application to validate the feature works end-to-end.

**Why this priority**: Essential for testing and demonstrating the feature but secondary to the core implementation.

**Independent Test**: Can be tested by calling the gRPC CountChildren method through the testHost and verifying it returns the same results as the direct API.

**Acceptance Scenarios**:

1. **Given** the testHost is running with sample data, **When** a gRPC client calls CountChildren, **Then** it receives the correct count response
2. **Given** multiple concurrent gRPC clients, **When** they call CountChildren simultaneously, **Then** all receive consistent and correct responses

---

### Edge Cases

- What happens when a parent path contains special characters or is malformed?
- How does the system handle very large counts that might exceed uint32 maximum value (4,294,967,295)?
- What occurs when the parent path exists but has mixed entity types as children?
- How does the system behave during concurrent modifications to the dataset while counting?

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST provide a CountChildren method in the ReaderService that accepts a parent path parameter
- **FR-002**: System MUST return the count of child entities as a uint32 value wrapped in a gRPC message
- **FR-003**: System MUST handle non-existent parent paths by returning count = 0
- **FR-004**: System MUST implement the CountChildren method in the testHost gRPC ReaderService
- **FR-005**: System MUST ensure count accuracy reflects the current state of the dataset
- **FR-006**: System MUST handle empty parent paths (root level counting) appropriately
- **FR-007**: System MUST validate parent path format and return meaningful error messages for malformed inputs

### Key Entities *(include if feature involves data)*

- **CountChildrenRequest**: Contains the parent path for which to count children
- **CountChildrenResponse**: Contains the uint32 count value representing the number of child entities
- **Parent Path**: String identifier specifying the hierarchical location in the dataset

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: CountChildren API calls complete within 100ms for datasets with up to 10,000 child entities under a single parent
- **SC-002**: System accurately counts children with 100% precision across all supported parent path formats  
- **SC-003**: gRPC CountChildren implementation in testHost matches the behavior of the direct ReaderService implementation in 100% of test cases
- **SC-004**: API handles edge cases gracefully, returning count = 0 for non-existent parents and appropriate error responses only for malformed inputs in 100% of test cases
- **SC-005**: Feature integrates seamlessly with existing ReaderService methods without breaking existing functionality in 100% of regression tests