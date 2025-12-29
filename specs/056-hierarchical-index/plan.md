# Implementation Plan: [FEATURE]

**Branch**: `[###-feature-name]` | **Date**: [DATE] | **Spec**: [link]
**Input**: Feature specification from `/specs/[###-feature-name]/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/commands/plan.md` for the execution workflow.

## Summary

[Extract from feature spec: primary requirement + technical approach from research]

## Technical Context

**Language/Version**: C# .NET 8.0
**Primary Dependencies**: Google.Protobuf, System.Reactive, Microsoft.Extensions.Hosting, Grpc.Core.Api
**Storage**: Protobuf files (Snapshot/Delta) on disk (Local, S3, Azure Blob, etc.)
**Testing**: xUnit
**Target Platform**: Cross-platform (Linux/Windows/macOS)
**Project Type**: Library/Service
**Performance Goals**: >50% memory reduction for 100M items, <30% disk size reduction.
**Constraints**: Backward compatibility with legacy flat files.
**Scale/Scope**: 100M+ items, deep hierarchies.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- **Test-First**: Will follow TDD.
- **Integration Testing**: Will verify backward compatibility with real legacy files.
- **Simplicity**: Keeping the in-memory model single (hierarchical only) simplifies the codebase.

## Project Structure

### Documentation (this feature)

```text
specs/056-hierarchical-index/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
src/
├── RxDatasets/
│   ├── Models/
│   │   ├── DatasetEntities.proto  # Updated schema
│   │   ├── ImmutableIndexNode.cs  # New internal class
│   │   └── ImmutableDatasetState.cs # Updated to use hierarchy
│   ├── DatasetReader.cs           # Updated logic
│   ├── DatasetWriter.cs           # Updated logic
│   └── DataMigrator.cs            # Updated logic
```

**Structure Decision**: Single project library structure.

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| [e.g., 4th project] | [current need] | [why 3 projects insufficient] |
| [e.g., Repository pattern] | [specific problem] | [why direct DB access insufficient] |
