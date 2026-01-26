# Implementation Plan: Lock Handover Mechanism

**Branch**: `001-lock-handover` | **Date**: 2026-01-23 | **Spec**: [specs/001-lock-handover/spec.md](specs/001-lock-handover/spec.md)
**Input**: Feature specification from `specs/001-lock-handover/spec.md`

## Summary

Implement a cooperative lock handover mechanism in `DistributedLockService`. This involves writing a `request-lock` file with the instance GUID on startup, monitoring this file during the lock holding period, and yielding the lock if another instance requests it. This ensures the most recently started instance acquires the lock.

## Technical Context

**Language/Version**: C# .NET 8.0
**Primary Dependencies**: `RxDatasets.Abstractions`, `Microsoft.Extensions.Hosting`, `Microsoft.Extensions.Logging`
**Storage**: `IDiskStorage<TEntity>` (File-based storage abstraction)
**Testing**: xUnit (`RxDatasets.Tests`)
**Target Platform**: .NET 8.0 supported platforms (Windows/Linux)
**Project Type**: Background Service / Library
**Performance Goals**: Lock handover within 2 polling intervals (<10s).
**Constraints**: Must work with existing `IDiskStorage` implementations (Local, S3, Azure, etc.).
**Scale/Scope**: Single file coordination per lock.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

*   **Principles**: Aligns with library-first and simplicity.
*   **Testing**: Feature is independently testable via unit/integration tests.
*   **Observability**: Logging state transitions is required.

**Result**: PASS

## Project Structure

### Documentation (this feature)

```text
specs/001-lock-handover/
├── plan.md              # This file
├── research.md          # Output: Implementation details & safeguards
├── data-model.md        # Output: File formats & State Machine
├── quickstart.md        # Output: Testing guide
├── contracts/           # Output: N/A (Internal coordination only)
└── tasks.md             # Output: Task breakdown
```

### Source Code (repository root)

```text
src/
└── RxDatasets/
    └── BackgroundServices/
        ├── DistributedLockService.cs       # Main logic changes
        └── DistributedLockOptions.cs       # Config updates

RxDatasets.Abstractions/
    └── BackgroundServices/
        └── LockServiceState.cs             # Public State Enum

tst/
└── RxDatasets.Tests/
    └── DistributedLockServiceTests.cs      # New tests
```

**Structure Decision**: Modify existing `DistributedLockService.cs`, add Enum to Abstractions, and add tests.

## Complexity Tracking

N/A
