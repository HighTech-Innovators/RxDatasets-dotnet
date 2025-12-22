# Feature Specification: GCS Write Queue for Rate Limit Optimization

**Feature Branch**: `056-gcs-rate-limit-queue`  
**Created**: 2025-12-22  
**Status**: Draft  
**Input**: User description: "I'd like to have a 'faster' implementation of the issue that GCS allows only 1 write per second for the DeltaInfo and/or ProcessedVersions pb file. Right now, we let the client wait for it for up to 5 x 1.1 seconds; better would be to just 'queue' the write if we KNOW it was written less than 2 seconds ago, and then flush queue writes after we know at least 2 seconds have passed since the last write for that file. Of course, when shutting down, we flush that queue before actually shutting down. and triple check the disposal of the google client before marking ourselves as stopped. I expect 100% of the code changes in GoogleCloudStorage project."

## User Scenarios & Testing

### User Story 1 - Queued Writes Avoid Client Blocking (Priority: P1)

When a client attempts to write a file to GCS (DeltaInfo or ProcessedVersions) within 2 seconds of the last successful write to that file, the request should be queued internally rather than causing the client to wait for rate limit delays.

**Why this priority**: This is the core value proposition - eliminating client-facing latency spikes from rate limiting. Clients get immediate acknowledgment and continue processing without 5+ second waits.

**Independent Test**: Can be tested by simulating rapid successive writes to the same GCS file and verifying the client returns immediately rather than blocking for rate limit handling.

**Acceptance Scenarios**:

1. **Given** a file was written to GCS less than 2 seconds ago, **When** a new write request arrives for that file, **Then** the request is queued and the client operation completes immediately
2. **Given** a queued write is pending, **When** 2 seconds have elapsed since the last successful write, **Then** the queued write is automatically flushed to GCS
3. **Given** multiple writes are queued for the same file, **When** the flush delay is reached, **Then** all queued writes for that file are flushed in order

---

### User Story 2 - Graceful Queue Flushing on Shutdown (Priority: P1)

When the storage provider is shutting down, all pending queued writes must be flushed to GCS before the shutdown completes to ensure no data loss.

**Why this priority**: Data integrity is critical - if the application shuts down with queued writes, those changes would be lost. This ensures a clean shutdown.

**Independent Test**: Can be tested by queuing writes, triggering shutdown, and verifying all queued writes are persisted to GCS before disposal completes.

**Acceptance Scenarios**:

1. **Given** queued writes are pending, **When** shutdown is initiated, **Then** all pending writes are flushed to GCS before returning from Dispose
2. **Given** a flush is in progress, **When** shutdown is initiated, **Then** the shutdown waits for the flush to complete
3. **Given** multiple files have queued writes, **When** shutdown occurs, **Then** all files' queued writes are flushed

---

### User Story 3 - Reliable Google Cloud Client Disposal (Priority: P1)

The Google Cloud Storage client must be properly disposed with verification before marking the storage provider as stopped to prevent resource leaks or connection hangs.

**Why this priority**: Proper resource cleanup is essential for reliability, particularly in Kubernetes environments where pods need to gracefully terminate. Improper disposal can cause shutdown delays or leave connections open.

**Independent Test**: Can be tested by disposing the storage provider and verifying the underlying client is fully released and no dangling connections remain.

**Acceptance Scenarios**:

1. **Given** the storage provider is disposing, **When** Dispose is called, **Then** the Google Cloud client is disposed before completion
2. **Given** the storage provider is disposed, **When** attempted to use after disposal, **Then** appropriate exceptions indicate the storage is no longer available
3. **Given** any pending operations exist, **When** disposal occurs, **Then** pending operations are completed or cancelled before client disposal

---

### Edge Cases

- What happens if a write is queued, then the same file receives another write before the 2-second flush window expires?
- How does the system handle files that haven't been written to in the queue but the application is shutting down?
- What happens if the flush operation fails after 2 seconds have elapsed? Should it retry or propagate the error?
- Can writes to different files be queued simultaneously, or is the queue per-file?
- What happens if the GCS rate limit is hit during a queued write flush?

## Requirements

### Functional Requirements

- **FR-001**: GoogleCloudStorage MUST maintain a write queue for each file being written
- **FR-002**: When a write request arrives and the last write for that file was less than 2 seconds ago, the write MUST be queued instead of blocking the caller
- **FR-003**: The system MUST automatically flush queued writes for a file after 2 or more seconds have elapsed since the last successful write to that file
- **FR-004**: Queued writes for the same file MUST be flushed in the order they were queued
- **FR-005**: When Dispose() is called, the system MUST flush all pending queued writes before releasing resources
- **FR-006**: The Google Cloud Storage client MUST be properly disposed as part of the Dispose() sequence
- **FR-007**: The storage provider MUST support per-file write queuing (different files can have independent queues)
- **FR-008**: All queue management operations MUST be thread-safe
- **FR-009**: The system MUST track the timestamp of the last successful write for each file to determine when 2 seconds have passed
- **FR-010**: Failed flush operations MUST be logged with appropriate context for debugging

### Key Entities

- **WriteQueue**: A queue structure holding pending writes for a specific file, with metadata about the last write timestamp and pending operations
- **QueuedWrite**: A single queued write operation containing the object key and byte content (or stream reference)
- **FlushTimer**: A mechanism to trigger queue flushing when the 2-second window has elapsed since the last successful write

## Success Criteria

### Measurable Outcomes

- **SC-001**: Clients writing to GCS files return immediately (sub-50ms) instead of blocking for 5.5+ seconds when rate limits are encountered
- **SC-002**: No data loss occurs on graceful shutdown - all queued writes are persisted before storage provider disposal completes
- **SC-003**: Zero resource leaks from Google Cloud Storage client - client is fully disposed and released on provider disposal
- **SC-004**: Queued writes do not exceed 2-3 seconds of additional latency before being flushed to GCS
- **SC-005**: System handles 100 or more queued writes across multiple files without memory issues or timeout problems
- **SC-006**: All queue management operations are thread-safe with no race conditions or data corruption under concurrent access

## Assumptions

- The 2-second window is determined by GCS's rate limit behavior (1 write per second to the same object), with a safety margin for clock skew
- Queued writes are in-memory and don't require persistence to disk
- The GoogleCloudStorage class is responsible for all queue management - no external queue system is used
- Write requests should maintain the same error handling semantics as before (failures are propagated after flush attempts)
- The disposal sequence occurs during graceful shutdown, allowing time for queue flushing without strict timeout constraints

## Constraints & Dependencies

- All changes must be contained within the RxDatasets.StorageProvider.GoogleCloud project
- Existing public API signatures should remain compatible where possible to avoid breaking changes
- The solution must not impact other storage providers (Local, S3, Azure Blob, etc.)
- Thread safety must be maintained without introducing significant lock contention
