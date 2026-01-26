# Feature Specification: Lock Handover Mechanism

**Feature Branch**: `001-lock-handover`  
**Created**: 2026-01-23  
**Status**: Draft  
**Input**: User description: Implement a 'request-lock' file mechanism for DistributedLockService to facilitate lock handover between instances. When a new instance starts, it requests the lock. The current lock holder yields. Fallback mechanisms for cleanup and reclaiming the lock are included.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Lock Handover (Priority: P1)

When a new instance of the application starts up, it should be able to cleanly and quickly take over the distributed lock from an existing running instance.

**Why this priority**: Essential for zero-downtime deployments and ensuring the active instance is the most recently started one (often containing new code/config).

**Independent Test**: Start Instance A (acquires lock). Start Instance B. Verify Instance A releases lock and Instance B acquires it.

**Acceptance Scenarios**:

1. **Given** Instance A holds the lock, **When** Instance B starts, **Then** Instance B writes a 'request-lock' file with its GUID.
2. **Given** Instance A holds the lock and sees a 'request-lock' file from Instance B, **When** Instance A checks for requests, **Then** Instance A releases the lock immediately and stops renewing it.
3. **Given** Instance A has released the lock due to a request, **When** Instance B acquires the lock, **Then** Instance B deletes the 'request-lock' file.

---

### User Story 2 - Cleanup and Stability (Priority: P2)

The system must ensure that 'request-lock' files do not persist indefinitely and that the lock is not lost if a requester fails to acquire it.

**Why this priority**: Prevents the system from getting into a state where no one holds the lock or the lock is flapping.

**Independent Test**: Simulate requester crash; Simulate stopper; Simulate orphaned file.

**Acceptance Scenarios**:

1. **Given** an instance has written a 'request-lock' file, **When** the instance stops gracefully, **Then** it deletes its 'request-lock' file (if it matches its GUID).
2. **Given** Instance A released the lock for Instance B, **But** Instance B crashed before acquiring it, **When** a timeout period elapses, **Then** Instance A attempts to re-acquire the lock (without writing a request file).
3. **Given** a 'request-lock' file exists (orphaned), **When** any instance successfully acquires the lock, **Then** it deletes the 'request-lock' file.

## Functional Requirements *(mandatory)*

1.  **Request on Start**: On startup, the `DistributedLockService` MUST write a file named `request-lock` (with appropriate prefix) containing its unique Instance GUID.
    *   **Conflict Resolution**: If the file already exists, it MUST be overwritten (Last Write Wins).
    *   **State Transition**: Service enters `Acquiring` state.
2.  **Monitor for Requests**: The lock holder MUST check for the existence of the `request-lock` file periodically.
    *   This check MUST happen every `PollingInterval` (e.g., 5s), regardless of whether the lock needs renewal.
    *   If the file exists and contains a GUID different from the holder's GUID, the holder MUST release the lock immediately and stop attempts to renew it.
    *   **State Transition**: Service transitions from `Active` to `Yielded`.
3.  **Cleanup on Stop**: When `StopAsync` is called, the service MUST check the `request-lock` file. If the content matches its own GUID, it MUST delete the file.
4.  **Cleanup on Acquire**: Immediately upon successfully acquiring the lock (transitioning from no-lock to has-lock), the service MUST delete the `request-lock` file, regardless of its content.
    *   **State Transition**: Service transitions from `Acquiring` (or `Yielded`) to `Active`.
5.  **Fallback Reclaim**: If the lock holder releases the lock due to a request:
    *   It MUST enter a "yielded" state (`LockServiceState.Yielded`).
    *   It MUST wait for a defined duration `DistributedLockOptions.HandoverTimeout`.
    *   **Validation**: `HandoverTimeout` MUST be strictly greater than `PollingInterval`. If this condition is not met during configuration/startup, the service MUST throw a validation exception (preventing startup).
    *   After the timeout, if the lock is still not held (or just unconditionally try), it MUST resume normal lock acquisition attempts (without treating it as a "new start" request, i.e., don't write `request-lock` again just for this recovery).
    *   **State Transition**: Service transitions from `Yielded` to `Acquiring` (internal logic handles the "don't write file" nuance).

## Success Criteria *(mandatory)*

*   **Handover Speed**: Lock handover between running instances occurs within 2 polling intervals (configurable, e.g., < 10 seconds).
*   **Reliability**: The lock is never permanently lost; if a requester dies, the previous owner or another instance recovers it.
*   **Correctness**: Only one instance holds the lock at a time (maintained by underlying lock mechanism).

## Assumptions *(optional)*

*   The underlying `IDiskStorage` supports atomic write/delete or eventual consistency is handled by the polling nature.
*   Clock synchronization is not strictly required but clocks should be reasonably close for timeouts.
*   "Max amount it should take the lock requester to take the lock" is a known or configurable value (e.g., related to `PollingInterval`).

## Entities *(optional)*

*   **RequestLockFile**: Simple text file containing a GUID string.
*   **InstanceGUID**: A unique identifier generated by each `DistributedLockService` instance at startup.
*   **LockServiceState** (Enum): Represents the internal state machine.
    *   `Idle`: Service is not running or initialized.
    *   `Acquiring`: Service is actively trying to acquire the lock.
    *   `Active`: Service currently holds the lock.
    *   `Yielded`: Service has voluntarily released the lock for handover and is waiting.
*   **DistributedLockOptions**: Configuration object.
    *   `RenewalInterval` (TimeSpan, default 5m)
    *   `PollingInterval` (TimeSpan, default 5s)
    *   `RenewalThreshold` (TimeSpan, default 30s)
    *   `HandoverTimeout` (TimeSpan, default 15s) - **New**

## Clarifications

### Session 2026-01-23

*   Q: How should the handover timeout be configured?
*   A: Add `HandoverTimeout` to `DistributedLockOptions`. It MUST be strictly larger than `PollingInterval`; otherwise, the service must error on startup.
*   Q: What if a 'request-lock' file already exists on startup?
*   A: Overwrite it (Last Write Wins).
*   Q: How frequently should the lock holder check for requests?
*   A: Every `PollingInterval` to ensure timely handover.
*   Q: How should the internal state be managed during handover?
*   A: Use an explicit state machine with `LockServiceState` enum (`Idle`, `Acquiring`, `Active`, `Yielded`).
