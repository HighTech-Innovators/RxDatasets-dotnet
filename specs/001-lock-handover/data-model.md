# Data Model: Lock Handover

## Entities

### 1. Request Lock File
*   **Path**: `[Container]/[Prefix]request-lock`
*   **Format**: UTF-8 Text
*   **Content**: `guid-string` (e.g., `550e8400-e29b-41d4-a716-446655440000`)
*   **Lifecycle**:
    *   **Created**: On service start (overwrites).
    *   **Read**: By lock holder every polling interval.
    *   **Deleted**:
        *   By requester when they successfully acquire the lock.
        *   By any instance stopping if file content == own GUID.

### 2. LockServiceState (Enum)
*   **Location**: `RxDatasets.Abstractions` (Public)
```csharp
public enum LockServiceState
{
    Idle,       // Initial state, or stopped
    Acquiring,  // Trying to get lock (Request file written)
    Active,     // Recursively renewing lock (Request file deleted)
    Yielded     // Voluntarily released lock, waiting for handover timeout
}
```

### 3. DistributedLockOptions (Configuration)
```csharp
public class DistributedLockOptions
{
    // ... existing ...
    public TimeSpan HandoverTimeout { get; set; } = TimeSpan.FromSeconds(15);
}
```
