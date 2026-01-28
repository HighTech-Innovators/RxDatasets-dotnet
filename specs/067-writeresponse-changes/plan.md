# Implementation Plan: Extend WriteResponse with Optional Change Details

**Branch**: `067-writeresponse-changes` | **Date**: January 28, 2026 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `specs/067-writeresponse-changes/spec.md`
**Related**: GitLab Issue #67, Spec 002 (PutOrDeleteMany)

## Summary

Extend the WriteResponse proto message with a new optional `repeated DetailedChange detailed_changes` field that contains a list of entity identifiers and their change types (NEW, CHANGED, DELETED). Trigger detailed change collection via presence of `X-Debug` HTTP header (case-insensitive). This provides enhanced observability for debugging write operations and testhost validation without breaking backward compatibility. Implementation spans proto contract changes and all write operation endpoints (Put, PutAll, PutMany, Delete, PutOrDeleteMany).

## Technical Context

**Language/Version**: C# .NET 8.0  
**Primary Dependencies**: Google.Protobuf, System.Reactive, Microsoft.Extensions.Hosting, Grpc.Core.Api  
**Storage**: Multiple providers (Local, S3, Azure Blob, Google Cloud, Minio) via IDiskStorage<TEntity> abstraction  
**Testing**: xUnit, integration tests against all storage providers  
**Target Platform**: .NET 8.0 server applications (Linux/Windows)  
**Project Type**: Multi-project solution (RxDatasets.csproj + testhost + test apps)  
**Performance Goals**: Sub-50ms for write operations without X-Debug header; sub-100ms with header for <1000 entities  
**Constraints**: No breaking changes to existing WriteResponse proto contract; must be backward compatible; proto changes must use optional fields  
**Scale/Scope**: Applies to all write operations (Put, PutAll, PutMany, Delete, PutOrDeleteMany) across gRPC and testhost service implementations

## Constitution Check

*GATE: Project appears to follow standard .NET library structure with proto contracts. No RxDatasets-specific constitution visible; implementing with standard practices.*

**Status**: ✅ PASS

Key Compliance Points:
- Proto contract extension: backward compatible (optional field in proto3) ✅
- Storage provider abstraction: leverages existing IDiskStorage<TEntity> ✅
- Testing: integrates with existing xUnit test infrastructure ✅
- HTTP header handling: standard gRPC/testhost patterns ✅
- No breaking changes to public API ✅

## Project Structure

### Documentation (this feature)

```text
specs/067-writeresponse-changes/
├── spec.md              # Feature specification (COMPLETE)
├── plan.md              # Implementation plan (THIS FILE)
├── research.md          # Research findings (NONE NEEDED - spec fully clarified)
├── data-model.md        # Data model specification (THIS PLAN)
├── quickstart.md        # Integration guide (PHASE 1)
├── contracts/           # Proto contracts (PHASE 1)
│   └── detailed-changes-contract.md
└── checklists/
    └── requirements.md  # Quality checklist
```

### Source Code Structure (RxDatasets.sln)

```text
src/
├── RxDatasets/
│   ├── Models/                      # Data models and types
│   ├── GrpcReaderServiceImpl.cs      # gRPC service implementation
│   ├── DtoServerBuilder.cs          # Testhost server builder
│   ├── DatasetWriter.cs             # Core write operations
│   └── ... (other writer files)
│
└── RxDatasets.Abstractions/         # Interface contracts
    └── (WriteResponse proto references)

testapps/
├── Testserver/
│   ├── Services/
│   │   └── WriterService.cs         # Testhost write service
│   └── ... (testhost implementation)
│
├── DTOs/
│   └── proto/
│       └── hightechinnovators/dtos/type/
│           ├── writer_service_types.proto  # WriteResponse definition
│           └── (service definitions)
│
└── ... (other test apps)

tst/
├── RxDatasets.Tests/                # Unit tests
│   ├── WriterTests.cs
│   └── ... (write operation tests)
│
└── RxDatasets.StorageTests/         # Integration tests
    └── ... (per-provider tests)
```

**Structure Decision**: Multi-project C# .NET 8.0 solution. Proto changes in `testapps/DTOs/proto/`, implementation in `src/RxDatasets/` and `testapps/Testserver/`. Tests added to `tst/` alongside existing test suites.

---

## Implementation Phases

### Phase 1: Proto Contract & Data Model Definition

**Duration**: 1 sprint (5-7 days)  
**Deliverables**: Proto files, data model, integration guide  
**Status**: COMPLETE

**Tasks**:
- ✅ Define `ChangeType` enum (NEW=0, CHANGED=1, DELETED=2)
- ✅ Define `DetailedChange` message (entity_id + change_type)
- ✅ Extend `WriteResponse` with `repeated DetailedChange detailed_changes`
- ✅ Document proto contract in [contracts/detailed-changes-contract.md](contracts/detailed-changes-contract.md)
- ✅ Document data model in [data-model.md](data-model.md)
- ✅ Create quick start guide in [quickstart.md](quickstart.md)

**Acceptance Criteria**:
- ✅ Proto changes reviewed and approved
- ✅ Data model documentation complete
- ✅ All constraints documented (backward compatibility, field semantics)
- ✅ Integration examples provided

---

### Phase 2: Implementation (Proto & Services)

**Duration**: 2 sprints (10-14 days)  
**Dependencies**: Phase 1 complete  
**Deliverables**: Proto code generation, service implementation, header handling

**Key Implementation Areas**:

#### 2a. Proto Code Generation
- Update `testapps/DTOs/proto/hightechinnovators/dtos/type/writer_service_types.proto`
  - Add ChangeType enum
  - Add DetailedChange message
  - Add field 5 to WriteResponse
- Regenerate C# code (automatic via dotnet build)
- Verify no breaking changes to existing consumers

#### 2b. Core Change Tracking (DatasetWriter & Related Classes)
- Implement change tracking during write operations
  - Identify newly created entities (didn't exist before)
  - Identify updated entities (CRC32 differs)
  - Identify deleted entities
  - Track via List<DetailedChange> or similar
- Leverage existing change detection logic from spec 002
- Apply net effect consolidation rules
- Exclude unchanged entities from tracking (critical optimization)

#### 2c. Header Detection & Response Population
- **GrpcReaderServiceImpl.cs**: Detect X-Debug header in gRPC context
  - Extract header value (case-insensitive matching)
  - Pass flag to write operation methods
- **DtoServerBuilder.cs**: Detect X-Debug header in testhost HTTP requests
  - Extract from HttpContext.Request.Headers
  - Pass flag to write operation methods
- **DatasetWriter.cs**: Conditionally populate detailed_changes based on flag

#### 2d. All Write Operation Methods
- `Put()`: Single entity - populate detailed_changes if X-Debug
- `Delete()`: Single entity - populate detailed_changes if X-Debug
- `PutAll()`: Stream entities - collect all changes, populate if X-Debug
- `PutMany()`: Stream entities - collect all changes, populate if X-Debug
- `PutOrDeleteMany()`: Mixed operations - apply net effect logic, populate if X-Debug

**Acceptance Criteria**:
- Proto changes compile without errors
- C# code generated correctly (DetailedChange, ChangeType, WriteResponse updated)
- Header detection works case-insensitively
- detailed_changes only populated when X-Debug present
- Net effect consolidation matches spec 002 behavior
- No performance degradation without X-Debug header

---

### Phase 3: Testing (Unit & Integration)

**Duration**: 1-2 sprints (7-14 days)  
**Dependencies**: Phase 2 implementation complete  
**Deliverables**: Test suite, test documentation, test reports

#### 3a. Unit Tests
- ChangeType enum values (0, 1, 2)
- DetailedChange message serialization
- WriteResponse proto backward compatibility
- Header detection logic (case-insensitive)
- Change type determination (NEW, CHANGED, DELETED)
- No-op detection (unchanged entities excluded)

#### 3b. Integration Tests (Per Storage Provider)
- Put + X-Debug header → detailed_changes populated
- Put without X-Debug → detailed_changes empty
- Delete + X-Debug → entity marked DELETED
- PutAll/PutMany + X-Debug → all changes collected
- PutOrDeleteMany + X-Debug → net effect applied
- Case-insensitive header matching (x-debug, X-DEBUG, X-Debug)
- Large batch operations (10k+ entities, only 10 change)
- Net effect consolidation scenarios (delete then create same entity)

#### 3c. Storage Provider Coverage
- Local disk storage
- S3 storage
- Azure Blob storage
- Google Cloud storage
- Minio storage

#### 3d. Performance Testing
- Without X-Debug: baseline performance maintained (sub-50ms)
- With X-Debug (<1000 entities): sub-100ms
- Large batches: response size scales with changes, not batch size

**Acceptance Criteria**:
- ✅ All unit tests pass
- ✅ Integration tests pass for all storage providers
- ✅ Case-insensitive header matching verified
- ✅ Performance targets met
- ✅ Large batch optimization verified (unchanged entities excluded)

---

### Phase 4: Documentation & Deployment

**Duration**: 3-5 days  
**Dependencies**: Phase 3 tests passing  
**Deliverables**: Updated docs, release notes, deployment guide

**Tasks**:
- Update WRITER-CONTRACT.md with new WriteResponse structure
- Update API documentation with X-Debug header details
- Create release notes highlighting:
  - Non-breaking change
  - Proto3 backward compatible
  - New feature: detailed_changes field
  - When to use: debugging, testing, validation
- Update quick start guide with production best practices
- Create migration guide (if needed for clients)

**Acceptance Criteria**:
- ✅ All documentation updated
- ✅ Release notes clear about backward compatibility
- ✅ Quick start guide comprehensive
- ✅ No breaking change statements added
- ✅ Performance guidance included

---

## Risk Mitigation

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|-----------|
| Proto change breaks existing clients | Low | High | Backward compatible design (optional field), thorough integration testing |
| Large batch performance degradation | Low | Medium | Exclude unchanged entities, sub-100ms target, performance testing |
| Header detection edge cases | Low | Medium | Case-insensitive matching per HTTP spec, comprehensive test coverage |
| Storage provider inconsistency | Low | Medium | Test against all providers, unified change tracking logic |
| Net effect consolidation bugs | Low | High | Leverage existing spec 002 logic, thorough edge case testing |

---

## Success Metrics

| Metric | Target | Verification |
|--------|--------|--------------|
| Proto backward compatibility | 100% | Integration tests with old clients |
| Test coverage | ≥95% code coverage | Code coverage report |
| Performance (no X-Debug) | Sub-50ms baseline | Performance tests |
| Performance (with X-Debug) | Sub-100ms for <1000 entities | Performance tests |
| Storage provider support | All 5 providers | Passing integration tests on each |
| Header matching | Case-insensitive | Explicit test cases (x-debug, X-DEBUG, etc.) |

---

## Dependencies & Prerequisites

**External Dependencies**:
- Google.Protobuf (existing)
- System.Reactive (existing)
- Microsoft.Extensions.Hosting (existing)
- Grpc.Core.Api (existing)

**Internal Dependencies**:
- Spec 002 (PutOrDeleteMany net effect consolidation) - COMPLETE
- WRITER-CONTRACT.md (atomic write guarantees) - EXISTS
- Existing proto build infrastructure - EXISTS
- Existing test infrastructure - EXISTS

**No New Dependencies Required** ✅

---

## Rollback Plan

If serious issues discovered post-deployment:

1. **Proto Rollback**: Remove field 5 from WriteResponse proto
   - Clients ignore unknown field → backward compatible removal
   - Regenerate C# code
   - Redeploy

2. **Feature Disable**: Keep proto but don't populate detailed_changes
   - Simpler than full rollback
   - Field remains but always empty
   - Allows gradual fix deployment

**Timeline**: Both options achievable in <1 day with minimal customer impact

---

## Related Documentation

- [spec.md](spec.md) - Feature specification
- [data-model.md](data-model.md) - Data model details
- [quickstart.md](quickstart.md) - Integration guide
- [contracts/detailed-changes-contract.md](contracts/detailed-changes-contract.md) - Proto contract
- [checklists/requirements.md](checklists/requirements.md) - Quality checklist
- WRITER-CONTRACT.md (project root) - Write operation guarantees
- Spec 002 - PutOrDeleteMany corner cases & net effect rules
