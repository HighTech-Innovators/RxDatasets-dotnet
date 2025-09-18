# Implementation Plan: PUTORDELETEMANY Corner Cases Bug Fix

**Branch**: `002-putordeletemany-corner-cases` | **Date**: September 17, 2025 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/002-putordeletemany-corner-cases/spec.md`

## Execution Flow (/plan command scope)
```
1. Load feature spec from Input path ✓
   → Feature spec loaded successfully
2. Fill Technical Context ✓
   → Project Type: C# .NET 8.0 library with gRPC services
   → Structure Decision: Single project with existing RxDatasets architecture
3. Fill the Constitution Check section ✓
   → Constitution is template-based, applying general principles
4. Evaluate Constitution Check section ✓
   → No violations - bug fix follows existing patterns
   → Update Progress Tracking: Initial Constitution Check ✓
5. Execute Phase 0 → research.md ✓
   → No NEEDS CLARIFICATION remain
6. Execute Phase 1 → contracts, data-model.md, quickstart.md ✓
7. Re-evaluate Constitution Check section ✓
   → No new violations
   → Update Progress Tracking: Post-Design Constitution Check ✓
8. Plan Phase 2 → Describe task generation approach ✓
9. STOP - Ready for /tasks command ✓
```

## Summary
Fix incorrect change reporting in PUTORDELETEMANY operations when batch contains both delete and put operations for the same entity. The system currently reports 'deleted' when it should report 'nothing changed' (same entity) or 'changed' (modified entity). The fix requires proper net effect calculation using CRC32-based change detection and comprehensive testing via realistic server setup using testhost.

## Technical Context
**Language/Version**: C# .NET 8.0  
**Primary Dependencies**: Google.Protobuf, System.Reactive, Microsoft.Extensions.Hosting, Grpc.Core.Api  
**Storage**: Multiple storage providers (Azure Blob, S3, Google Cloud, Local Disk, In-Memory)  
**Testing**: xUnit, MSTest, testhost for realistic server scenarios  
**Target Platform**: Cross-platform .NET 8.0 server applications  
**Project Type**: Single library project with existing RxDatasets architecture  
**Performance Goals**: Maintain existing throughput for batch operations  
**Constraints**: Must not break existing API contracts, backward compatibility required  
**Scale/Scope**: Bug fix affecting DatasetWriter.cs batch operation logic and associated tests

## Constitution Check
*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

**Test-First Approach**: ✅ All test scenarios clearly defined in spec  
**Backward Compatibility**: ✅ Fix maintains existing API contracts  
**Observability**: ✅ Change detection uses existing CRC32 logging mechanism  
**Simplicity**: ✅ Bug fix follows existing patterns, no architectural changes  
**Integration Testing**: ✅ Requires testhost-based realistic server testing

## Project Structure

### Documentation (this feature)
```
specs/002-putordeletemany-corner-cases/
├── plan.md              # This file (/plan command output)
├── research.md          # Phase 0 output (/plan command)
├── data-model.md        # Phase 1 output (/plan command)
├── quickstart.md        # Phase 1 output (/plan command)
├── contracts/           # Phase 1 output (/plan command)
└── tasks.md             # Phase 2 output (/tasks command - NOT created by /plan)
```

### Source Code (repository root)
```
src/
├── RxDatasets/
│   ├── DatasetWriter.cs        # Primary fix location
│   ├── Models/
│   └── BackgroundServices/
tst/
├── RxDatasets.Tests/
│   └── DatasetWriterTests/     # Unit tests for batch operations
└── RxDatasets.StorageTests/    # Integration tests with testhost
testapps/
├── Host/                       # Testhost for realistic scenarios
└── TestClient/                 # Client for testing read-back scenarios
```

**Structure Decision**: Single project following existing RxDatasets architecture

## Phase 0: Outline & Research
1. **Extract unknowns from Technical Context**:
   - Current PUTORDELETEMANY implementation details ✓
   - Existing change detection mechanism (CRC32) ✓
   - Test infrastructure for realistic server scenarios ✓

2. **Research completed**:
   - Analyzed existing DatasetWriter.cs implementation
   - Identified ApplyDelta method as core logic location
   - Confirmed CRC32-based change detection in SnapshotEntity
   - Located testhost infrastructure in testapps/

3. **Key findings**:
   - Bug is in batch operation processing logic
   - Need to compare final entity state vs. initial state
   - Existing test patterns can be extended
   - Testhost provides writer/reader separation

**Output**: research.md with implementation approach defined

## Phase 1: Design & Contracts
*Prerequisites: research.md complete*

1. **Data model updates**: 
   - No new entities required
   - Enhanced batch operation logic in existing Delta record
   - Improved change detection using existing CRC32 mechanism

2. **API contracts**:
   - No breaking changes to existing PUTORDELETEMANY interface
   - Internal contract: enhanced Delta processing logic
   - Test contracts: comprehensive edge case scenarios

3. **Test scenarios from user stories**:
   - Delete + Put same entity → 'nothing changed'
   - Delete + Put different entity → 'changed'
   - Multiple puts ending with original → 'nothing changed'
   - Put + Delete → 'deleted'
   - Create + Delete in batch → 'nothing changed'
   - Reader isolation during batch processing

**Output**: data-model.md, /contracts/*, failing tests, quickstart.md

## Phase 2: Task Planning Approach
*This section describes what the /tasks command will do - DO NOT execute during /plan*

**Task Generation Strategy**:
- Generate unit tests for each edge case scenario [P]
- Create integration tests using testhost for reader/writer separation [P]
- Implement enhanced batch operation logic in DatasetWriter.cs
- Update change detection to use net effect calculation
- Add comprehensive test scenarios for all corner cases

**Ordering Strategy**:
- TDD order: Tests first, then implementation
- Unit tests before integration tests
- Core logic before edge case handling
- Mark [P] for parallel test creation

**Estimated Output**: 12-15 numbered, ordered tasks in tasks.md

## Complexity Tracking
*No constitutional violations identified*

## Progress Tracking
*This checklist is updated during execution flow*

**Phase Status**:
- [x] Phase 0: Research complete (/plan command) ✅
- [x] Phase 1: Design complete (/plan command) ✅
- [x] Phase 2: Task planning complete (/plan command - describe approach only) ✅
- [ ] Phase 3: Tasks generated (/tasks command)
- [ ] Phase 4: Implementation complete
- [ ] Phase 5: Validation passed

**Gate Status**:
- [x] Initial Constitution Check: PASS ✅
- [x] Post-Design Constitution Check: PASS ✅
- [x] All NEEDS CLARIFICATION resolved ✅
- [x] Complexity deviations documented (none) ✅

**Generated Artifacts**:
- [x] research.md ✅
- [x] data-model.md ✅  
- [x] contracts/putordeletemany-contract.md ✅
- [x] quickstart.md ✅
- [x] Agent context updated (.github/copilot-instructions.md) ✅

---
*Based on Constitution v2.1.1 - See `/memory/constitution.md`*