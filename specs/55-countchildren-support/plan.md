# Implementation Plan: CountChildren Support for ReaderService

**Branch**: `55-countchildren-support` | **Date**: October 28, 2025 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/55-countchildren-support/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/commands/plan.md` for the execution workflow.

## Summary

Add CountChildren functionality to RxDatasets ReaderService that efficiently counts child entities under a parent path without fetching entity data. Implementation includes gRPC service method, proto contract definition, and integration in testHost applications. Returns count as uint32 in gRPC message format.

## Technical Context

**Language/Version**: C# .NET 8.0  
**Primary Dependencies**: Google.Protobuf, System.Reactive, Microsoft.Extensions.Hosting, Grpc.Core.Api  
**Storage**: Multiple providers supported (Local, S3, Azure Blob, Google Cloud, Minio)  
**Testing**: xUnit with NSubstitute for mocking  
**Target Platform**: Cross-platform .NET 8 runtime  
**Project Type**: Library with gRPC services - modular architecture  
**Performance Goals**: <100ms response time for up to 10,000 children under single parent  
**Constraints**: Must not fetch entity data, only count; uint32 return type limits to 4.2B max count  
**Scale/Scope**: Reactive dataset system serving protobuf entities via gRPC with real-time change notifications

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

**Status**: PASS - Constitution file contains template placeholder content only. No active architectural constraints to validate against. Proceeding with standard implementation approach following existing RxDatasets patterns.

## Project Structure

### Documentation (this feature)

```text
specs/[###-feature]/
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
├── RxDatasets.Abstractions/        # Core interfaces including IDatasetReader<T>
│   └── IDatasetReader.cs           # ADD: CountChildren method signature
├── RxDatasets/                     # Main implementation
│   ├── DatasetReader.cs            # ADD: Implement CountChildren method
│   ├── GrpcReaderServiceImpl.cs    # ADD: CountChildren gRPC service method (wrapper)
└── RxDatasets.InMemoryDataset/     # In-memory dataset implementation
    └── InMemoryDataset.cs          # ADD: Implement CountChildren method

testapps/
└── DTOs/
    └── proto/hightechinnovators/dtos/
        ├── type/
        │   └── reader_service_types.proto      # ADD: CountChildren messages
        └── [entity]/v1/
            └── reader_service.proto            # ADD: CountChildren RPC method

tests/
├── RxDatasets.Tests/               # Unit tests
│   └── DataReaderCountChildrenTests.cs  # ADD: CountChildren test cases
└── RxDatasets.StorageTests/        # Integration tests
    └── E2eTestsUsingTestHost.cs    # ADD: End-to-end CountChildren tests
```

**Structure Decision**: Implementation is focused on the two IDatasetReader implementations (InMemoryDataset and DatasetReader) plus the gRPC service wrapper. Storage providers are NOT modified - they already provide name enumeration through List() which is sufficient for counting.

## Complexity Tracking

> **No violations detected** - Implementation follows existing patterns in RxDatasets architecture without introducing additional complexity.

---

## Phase Completion Status

### Phase 0: Research (COMPLETE)

**Artifacts Generated**:
- `research.md` - Comprehensive research on all six design decisions with rationale and alternatives

**Key Findings**:
- No new external dependencies required
- Implementation leverages existing storage provider infrastructure
- Error handling: non-existent parent returns 0 (not an error)
- Performance target of <100ms achievable with name-only listing

**All NEEDS CLARIFICATION items resolved**.

### Phase 1: Design & Contracts (COMPLETE)

**Artifacts Generated**:
- `data-model.md` - Entity definitions, validation rules, state transitions
- `contracts/reader_service_types_countchildren.proto` - Message definitions (CountChildrenRequest/Response)
- `contracts/reader_service_rpc.proto` - RPC service method definition
- `quickstart.md` - API usage examples and integration guide
- `.github/copilot-instructions.md` - Updated with feature technology stack

**Design Decisions**:
1. Method signature: `Task<uint> CountChildren(string parent, CancellationToken cancellationToken = default)`
2. gRPC messages follow existing patterns (single-field request/response)
3. Implementation reuses storage provider `List()` for efficient name-only retrieval
4. Error handling: returns count = 0 for non-existent parents
5. Integration: no new storage provider interfaces required

**Architecture Validation**:
- Consistent with existing IDatasetReader interface patterns
- Follows GrpcReaderServiceImpl design patterns
- Compatible with all storage provider implementations
- No breaking changes to existing functionality

---

## Next Steps

**When ready to proceed to Phase 2 (Task Planning)**:
1. Run `/speckit.tasks` command to generate implementation tasks
2. Task breakdown will cover:
   - Proto file updates
   - Interface implementation
   - gRPC service implementation
   - Unit and integration tests
   - End-to-end testing
