# Quickstart: Testing Lock Handover

## Overview
Since this is an internal mechanism, testing involves running multiple instances of `DistributedLockService` against the same storage backend (or mocked storage).

## Local Testing Steps

1.  **Configure Storage**: Use `LocalDiskStorageProvider` pointing to a temp folder.
2.  **Start Instance A**:
    *   Run a test harness starting Service A.
    *   Observe logs: `State: IDLE -> ACQUIRING -> ACTIVE`.
    *   Verify `lock-file` exists.
    *   Verify `request-lock` does NOT exist (should be cleaned up).
3.  **Start Instance B**:
    *   Run test harness for Service B (same storage path).
    *   Observe logs: `State: IDLE -> ACQUIRING`.
    *   Verify `request-lock` exists with B's GUID.
4.  **Observe Handover**:
    *   Instance A logs: `Detected request from [B-GUID]. Yielding...`.
    *   Instance A state: `ACTIVE -> YIELDED`. (Lock released).
    *   Instance B logs: `Lock acquired`.
    *   Instance B state: `ACQUIRING -> ACTIVE`.
    *   Verify `lock-file` exists (held by B).
    *   Verify `request-lock` is deleted.

## Troubleshooting
*   **Logs**: Look for `DistributedLockService` category.
*   **File System**: Watch the storage folder.
