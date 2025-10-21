# Implementation Plan: Dataset Indexes (Aliases) for Entities

**Branch**: `004-introduce-the-concept` | **Date**: October 21, 2025 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/004-introduce-the-concept/spec.md`

## Execution Flow (/plan command scope)

```
1. Load feature spec from Input path ✓
   → Feature spec loaded successfully
2. Fill Technical Context ✓
   → Project Type: C# .NET 8.0 library with gRPC services
   → Structure Decision: Single project with existing RxDatasets architecture
3. Fill the Constitution Check section ✓
   → Constitutional requirements apply: test-first, backward compatibility, observability
4. Evaluate Constitution Check section ✓
   → All requirements compatible with existing architecture
   → Update Progress Tracking: Initial Constitution Check ✓
5. Execute Phase 0 → research.md ✓
   → No NEEDS CLARIFICATION remain in spec
6. Execute Phase 1 → data-model.md, contracts/, quickstart.md ✓
7. Re-evaluate Constitution Check section ✓
   → Design maintains all constitutional principles
   → Update Progress Tracking: Post-Design Constitution Check ✓
8. Plan Phase 2 → Describe task generation approach ✓
9. STOP - Ready for /tasks command ✓
```

---

## Summary

Introduce the IDatasetIndex interface and InMemoryReadableIndex implementation to enable alias-based access to entities. The feature provides a read-only index layer that maps semantic aliases to primary entity IDs, maintains in-memory state via the dataset's change feed, and integrates with the gRPC reader service to support transparent alias resolution. "Last one wins" conflict resolution for overlapping aliases in this iteration; future writer hooks will enforce transaction-level conflict detection.

## Technical Context
**Language/Version**: C# .NET 8.0  
**Primary Dependencies**: Google.Protobuf, System.Reactive, Microsoft.Extensions.Hosting, Grpc.Core.Api  
**Storage**: Multiple storage providers (Azure Blob, S3, Google Cloud, Local Disk, In-Memory)  
**Testing**: xUnit, testhost for realistic server scenarios  
**Target Platform**: Cross-platform .NET 8.0 server applications  
**Project Type**: Single library project with existing RxDatasets architecture  
**Performance Goals**: Minimal latency for alias resolution (in-memory hash lookups)  
**Constraints**: Read-only in this iteration; no writer integration yet; IDatasetReader depends on ListChanges() stream  
**Scale/Scope**: New interface, one in-memory implementation, gRPC reader extension, DI helper class

## Constitution Check

GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.

**Test-First Approach**: ✅ All feature requirements clearly testable; contract tests define expected behavior  
**Backward Compatibility**: ✅ New interface; no breaking changes to existing IDatasetReader  
**Observability**: ✅ Index state can be inspected; alias resolution fully traceable  
**Simplicity**: ✅ Feature is focused and composable; clear single responsibility  
**Integration Testing**: ✅ Requires testhost-based testing of gRPC reader extensions  

**PASS**: All constitutional principles upheld

## Project Structure

### Documentation (this feature)

```
specs/004-introduce-the-concept/
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
│   ├── DatasetExtensions.cs        # Add IDatasetIndex registration helpers
│   ├── IndexExtensions.cs          # New: alias resolution utilities
│   ├── Models/
│   │   └── IDatasetIndex.cs        # New interface
│   ├── Indexes/                    # New folder
│   │   └── InMemoryReadableIndex.cs
│   └── GrpcReaderServiceImpl.cs     # Extend with index-aware methods
├── RxDatasets.Abstractions/
│   └── IDatasetIndex.cs            # Interface definition
tst/
├── RxDatasets.Tests/
│   └── Indexes/                    # New
│       ├── InMemoryReadableIndexTests.cs
│       └── IndexGrpcReaderTests.cs
testapps/
├── Host/                           # May need extension for index registration
└── TestClient/                     # May need extension for index testing
```

**Structure Decision**: Single project following existing RxDatasets architecture; new interfaces in Abstractions, implementations in main project

## Phase 0: Outline & Research

1. **Extract unknowns from Technical Context**:
   - Existing IDatasetReader interface and ListChanges() stream behavior
   - Current gRPC reader service structure (GrpcReaderServiceImpl)
   - DI container integration patterns in existing codebase
   - Change notification semantics and backpressure handling

2. **Research completed**:
   - Analyzed IDatasetReader interface: Read(), ListChildren(), CountChildren(), ListChanges()
   - Identified GrpcReaderServiceImpl as extension point for index-aware operations
   - Confirmed System.Reactive usage for change streams
   - Located existing DI patterns in DatasetExtensions.cs (ConfigExtensions)

3. **Key findings**:
   - ListChanges() provides Rx stream of ChangeNotification with ChangeType enum
   - Change feed includes created, modified, deleted events with entity name/content
   - DI uses Microsoft.Extensions.DependencyInjection with extension methods
   - gRPC service receives reader instance, extensible for index-aware routing
   - Change notifications are reliable (no backpressure drops)

4. **Design decisions**:
   - Interface inherits from IDatasetReader but excludes ListChanges (via explicit interface separation)
   - InMemoryReadableIndex maintains two maps: aliasPath → entityName, entityName → aliasArray
   - Initialization: replay all changes from ListChanges() to populate maps
   - Ongoing: subscribe to ListChanges() for delta updates (add, remove, update operations)
   - Conflict handling: "last one wins" for alias collisions (same alias generated by multiple entities)
   - gRPC extension: optional indexName parameter; if provided, route through index; if null, use direct reader

**Output**: research.md with design approach, technology choices, and integration points

## Phase 1: Design & Contracts
*Prerequisites: research.md complete*

1. **Data model** (`data-model.md`):
   - **IDatasetIndex<TEntity>**: Interface with Read(), ListChildren(), CountChildren() methods
   - **InMemoryReadableIndex<TEntity>**: Implementation with alias mapping state
   - **AliasResolutionState**: Internal state tracking maps, previous aliases for diff calculations
   - **IndexRegistration**: DI helper class with generic registration methods
   - No new entities; alias is computed value derived from entity content

2. **API contracts** (`contracts/`):
   - Define IDatasetIndex<T> interface contract with method signatures
   - gRPC service extension: optional indexName parameter in Read, ListChildren, CountChildren
   - Error scenarios: invalid alias paths, non-existent indexes, ambiguous aliases
   - Change feed contract: how alias changes propagate (via underlying reader notifications)

3. **Test scenarios** (contract tests):
   - **Contract tests**:
     - Read via valid alias → returns entity data
     - ListChildren with alias prefix → returns all matching entities
     - CountChildren with alias prefix → returns correct count
     - Invalid alias → throws IndexOutOfRangeException or NotFoundException
     - Read via primary ID when alias exists → still works (no interference)
   
   - **State management tests**:
     - Entity creation → alias added to maps
     - Entity update with same aliases → maps unchanged
     - Entity update with new aliases → maps updated (old removed, new added)
     - Entity deletion → all aliases removed from maps
     - Batch changes → correctly diff and update maps
   
   - **Conflict handling tests**:
     - Two entities generate same alias → last one wins, overwrites previous
     - Alias collision resolution order → deterministic based on change feed order

4. **Quickstart** (`quickstart.md`):
   - Register index: `services.AddDatasetIndex<LinkEntity>("links_by_item", (name, link) => GenerateAliases(link))`
   - Read via alias: `var link = await index.Read("items/item1/links/l001")`
   - ListChildren: `var allItemLinks = await index.ListChildren("items/item1/links")`
   - Integration with gRPC: client calls `Read(indexName: "links_by_item", path: "items/item1/links/l001")`

5. **Agent file update**:
   - Run `.specify/scripts/powershell/update-agent-context.ps1 -AgentType copilot`
   - Add: C# .NET 8.0, System.Reactive, IDatasetIndex interface, InMemoryReadableIndex implementation
   - Update technologies: Microsoft.Extensions.Hosting, Grpc.Core.Api
   - Preserve manual additions between markers

**Output**: data-model.md, /contracts/*, failing tests, quickstart.md, updated copilot-instructions.md

## Phase 2: Task Planning Approach
*This section describes what the /tasks command will do - DO NOT execute during /plan*

**Task Generation Strategy**:
1. **Interface Definition** (Phase 2a):
   - Create IDatasetIndex<T> in RxDatasets.Abstractions [P]
   - Define method signatures: Read(), ListChildren(), CountChildren()
   - Verify inheritance from IDatasetReader (without ListChanges)

2. **Core Implementation** (Phase 2b):
   - Implement InMemoryReadableIndex<T> base structure [P]
   - Implement initialization logic (replay ListChanges stream) [P]
   - Implement ongoing change subscription (deltas)
   - Implement state management (alias maps, diff tracking)

3. **Read Operations** (Phase 2c):
   - Implement Read() with alias resolution → batch read on underlying reader [P]
   - Implement ListChildren() with alias prefix matching [P]
   - Implement CountChildren() with alias filtering [P]

4. **DI & Registration** (Phase 2d):
   - Create IndexExtensions static class with registration methods [P]
   - Implement AddDatasetIndex<T>() extension method
   - Implement type-safe indexing by name

5. **gRPC Integration** (Phase 2e):
   - Extend GrpcReaderServiceImpl with index-aware overloads [P]
   - Implement index resolution by name from DI container
   - Implement request routing (index vs. direct reader)

6. **Unit Tests** (Phase 2f):
   - Basic read/write scenario tests [P]
   - Alias collision and "last one wins" tests [P]
   - Change feed processing and state update tests [P]
   - Edge case tests (empty results, complex hierarchies) [P]

7. **Integration Tests** (Phase 2g):
   - Testhost-based reader/index integration [P]
   - gRPC reader service with index routing
   - End-to-end multi-entity index scenarios

**Ordering Strategy**:
- TDD order: Tests before implementation
- Foundation: Interface + data model before implementation
- Core: In-memory index state management before I/O operations
- Integration: DI + gRPC last, once core is testable
- Mark [P] for parallel test creation (independent scopes)

**Estimated Output**: 18-22 numbered, ordered tasks in tasks.md

**IMPORTANT**: This phase is executed by the /tasks command, NOT by /plan

## Phase 3+: Future Implementation
*These phases are beyond the scope of the /plan command*

**Phase 3**: Task execution (/tasks command creates tasks.md with concrete work items)  
**Phase 4**: Implementation (execute tasks following TDD; all tests must pass)  
**Phase 5**: Validation (run full test suite, execute quickstart.md, performance validation for alias resolution latency)

## Complexity Tracking
*No constitutional violations identified*

All design decisions maintain simplicity and focus:
- Feature is read-only in this iteration (future writer hooks separate concern)
- Alias sub-typing deferred (simplification)
- Change feed notifications deferred (simplification)
- In-memory implementation only (appropriate for scope; distributed index is future work)

## Progress Tracking
*This checklist is updated during execution flow*

**Phase Status**:
- [x] Phase 0: Research complete (/plan command) ✅
  - Design approach determined
  - Integration points identified
  - Technology choices validated
  
- [x] Phase 1: Design complete (/plan command) ✅
  - IDatasetIndex interface defined
  - InMemoryReadableIndex design specified
  - gRPC integration approach determined
  - Test scenarios identified
  - Quickstart documented
  
- [x] Phase 2: Task planning complete (/plan command) ✅
  - Task generation strategy described above
  - Ordering strategy defined
  - Estimated 18-22 tasks
  
- [ ] Phase 3: Tasks generated (/tasks command)
- [ ] Phase 4: Implementation complete
- [ ] Phase 5: Validation passed

**Gate Status**:
- [x] Initial Constitution Check: PASS ✅
- [x] Post-Design Constitution Check: PASS ✅
- [x] All NEEDS CLARIFICATION resolved ✅
- [x] Complexity deviations documented (none) ✅

---

**Ready for /tasks command** - All phases 0-2 complete

*Based on RxDatasets Architecture - See `/specs/001-rxdatasets-reactive-entity/`*
