# Research: Lock Handover Mechanism

**Feature**: Lock Handover (`001-lock-handover`)
**Date**: 2026-01-23

## Decisions

### 1. Request Lock File Format
**Decision**: Plain text file containing only the GUID.
**Rationale**: Simplicity. No need for JSON or complex parsing. `IDiskStorage` handles bytes/strings.
**Alternatives**: JSON (overkill), Metadata only (some storage providers make metadata access slower/harder).

### 2. State Machine Implementation
**Decision**: Explicit `enum LockWorkflowState { Idle, Acquiring, Active, Yielded }` field in `DistributedLockService`.
**Rationale**: Clarity of flow. The existing `bool HasLock` is insufficient for the `Yielded` state where we explicitly hold off acquiring.

### 3. Handover Timeout location
**Decision**: `DistributedLockOptions.HandoverTimeout`.
**Rationale**: Centralized configuration.

## Unknowns Resolution
*   **Safe Read/Write**: `IDiskStorage` operations are typically atomic for whole file replacements (PUT). We rely on this.
*   **Race Conditions**:
    *   *Race*: A releases, B tries to acquire but C (rogue/old) steals it.
    *   *Mitigation*: The `Yielded` state logic will eventually retry. If C acquired it, B's request file is still there (or B refreshes it?), C should see it and yield.
    *   *Refinement*: B should ensure its request file is present/fresh if it fails to acquire. The default "Last Write Wins" on startup handles the initial request. If B enters a wait loop, it might strictly rely on the initial write. **Clarification**: If B fails to acquire after A yields, does B re-write the request? *Spec says*: "Resume normal lock acquisition attempts".
*   **Polling Frequency**: The check for `request-lock` will be added to the main loop `StartAsync`.

## Implementation Details

*   **File Path**: `{prefix}request-lock`.
*   **Logging**: Log key transitions (Requested -> Yielding, Acquired -> Deleting Request).
