# Feature Specification: Hierarchical Index for Memory Optimization

**Feature Branch**: `056-hierarchical-index`
**Created**: 2025-12-23
**Status**: Draft
**Input**: User description: "Implement hierarchical index structure for datasets to reduce memory usage and improve performance, mirroring this structure on disk (protobuf) while maintaining backward compatibility."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Hierarchical In-Memory Index (Priority: P1)

The system uses a hierarchical structure (Trie-like) for the in-memory index instead of a flat dictionary. This structure is the **exclusive** in-memory representation; the system does not support a mixed or flat state in memory. This significantly reduces memory overhead for datasets where IDs are path-like strings (e.g., "chains/1/shops/2/products/3"), as common prefixes are stored only once.

**Why this priority**: This is the core requirement to address the memory usage issue described in the feature request.

**Independent Test**: Create a dataset with millions of items using path-like IDs. Measure memory usage of the index process. Compare with the baseline (flat index).

**Acceptance Scenarios**:

1. **Given** a dataset with 1 million items having shared prefixes (e.g., "A/B/C/1" to "A/B/C/1000000"), **When** the index is loaded into memory, **Then** the memory consumption is significantly lower (e.g., >50% reduction) than storing 1 million full string keys.
2. **Given** a dataset with flat IDs (no separators), **When** loaded, **Then** the system functions correctly using the same hierarchical structure (effectively a 1-level hierarchy).

---

### User Story 2 - Hierarchical Disk Format (Priority: P1)

The system persists the index to disk using a hierarchical Protobuf message structure for **both snapshots and deltas**. This mirrors the in-memory structure, reducing file sizes and potentially speeding up loading/saving.

**Why this priority**: Mirrors the in-memory structure to optimize disk space and I/O, and avoids expensive conversion during load/save.

**Independent Test**: Generate a snapshot and a delta for a large hierarchical dataset. Inspect the file size and structure (using `protoc --decode`).

**Acceptance Scenarios**:

1. **Given** a hierarchical in-memory index, **When** a snapshot is created, **Then** the output file uses the nested `SnapshotChildIndex` (or `SnapshotNode`) Protobuf structure.
2. **Given** a set of changes, **When** a delta is created, **Then** the output file uses the nested `DeltaNode` Protobuf structure.
3. **Given** the same dataset, **When** comparing the snapshot/delta file size to the legacy flat format, **Then** the new format is smaller (due to prefix deduplication).

---

### User Story 3 - Backward Compatibility & Migration (Priority: P1)

The system ensures backward compatibility by converting legacy flat **snapshot and delta** indexes to the hierarchical structure **at read time**. The in-memory representation is exclusively hierarchical.

Explicit migration via `DataMigrator` is provided specifically for **delta indices**, as snapshot files are short-lived and will naturally be replaced by hierarchical versions over time.

**Why this priority**: Essential for deploying the feature to existing environments without downtime or data loss.

**Independent Test**:
1. Load legacy flat snapshots and deltas; verify they build a correct hierarchical in-memory index.
2. Run the migrator on a folder of legacy delta files; verify they are converted to the new format.

**Acceptance Scenarios**:

1. **Given** a legacy snapshot file (flat index), **When** the `DatasetReader` loads it, **Then** it successfully builds the hierarchical in-memory index.
2. **Given** a legacy delta file (flat index), **When** the `DatasetReader` loads it, **Then** it is converted on-the-fly to the hierarchical structure for processing.
3. **Given** a set of legacy delta files, **When** the `DataMigrator` is run, **Then** the delta files on disk are rewritten to the new hierarchical format.
4. **Given** legacy files, **When** identifying the format, **Then** the system trivially distinguishes between flat and hierarchical files (e.g., by checking for the presence of nested index fields or separator characters in keys).

---

### User Story 4 - Efficient Delta Application (Priority: P2)

The system applies deltas (changes) to the hierarchical index efficiently. Since deltas often affect specific sub-trees (e.g., updates to a specific shop), the hierarchical structure allows for localized updates.

**Why this priority**: Ensures that the write path (applying updates) remains performant and thread-safe.

**Independent Test**: Apply a large batch of updates to a specific "branch" of the ID space. Measure the time taken to update the index.

**Acceptance Scenarios**:

1. **Given** a loaded hierarchical index, **When** a delta with 1000 changes is applied, **Then** the index is updated correctly and atomically.
2. **Given** concurrent readers, **When** a delta is applied, **Then** readers continue to see a consistent view (snapshot isolation) without blocking or corruption.

---

### Edge Cases

- **Flat IDs**: IDs without separators should be handled gracefully (root level items).
- **Mixed IDs**: A mix of hierarchical and flat IDs in the same dataset.
- **Empty Segments**: IDs with double separators (e.g., `a//b`) or trailing separators.
- **Deep Hierarchy**: Extremely deep nesting levels.
- **Delta Conflicts**: Although deltas usually operate on leaf entities, ensure no issues if a delta implies a structural change (e.g., creating a new branch).

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST represent the dataset index in memory as a hierarchical structure where path segments are nodes.
- **FR-002**: The system MUST serialize the index to disk using a hierarchical Protobuf message structure (introducing `SnapshotNode` and `DeltaNode`).
- **FR-003**: The system MUST deserialize legacy flat `SnapshotIndex` and `DeltaIndex` messages and immediately convert them to the hierarchical in-memory structure. The system MUST NOT maintain a flat or hybrid in-memory index state.
- **FR-004**: The system MUST support atomic application of deltas to the hierarchical index in a thread-safe manner (likely using immutable data structures or copy-on-write).
- **FR-005**: The system MUST maintain the same external API for `IDatasetReader` and `IDatasetWriter` to ensure transparency to consumers.
- **FR-006**: The system MUST use `/` as the default separator for hierarchy, or support a configurable separator if required (assuming `/` based on issue description).
- **FR-007**: The system SHOULD provide a migration capability (via `DataMigrator`) to explicitly upgrade legacy flat **delta index** files to the new hierarchical format. Snapshot migration is not required.
- **FR-008**: The system relies on the presence of separators (default `/`) in IDs to build the hierarchy. It is assumed that valid IDs in large datasets contain at least one top-level element (e.g., `tenant/id` or `type/id`). Flat IDs are supported but treated as root-level items.

### Key Entities

- **SnapshotIndex**: The root Protobuf message, updated to contain a map of `SnapshotChildIndex` in addition to (or replacing) the flat entities map.
- **SnapshotChildIndex**: A new Protobuf message containing a map of `SnapshotEntity` (leaves) and a map of `SnapshotChildIndex` (sub-nodes).
- **ImmutableIndexNode**: The in-memory representation of a node in the hierarchy (internal class).

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Memory usage for the index of a dataset with 100 chains * 100 shops * 10,000 products (100M items) is reduced by at least 50% compared to the flat index.
- **SC-002**: Snapshot index file size on disk is reduced by at least 30% for the same dataset.
- **SC-003**: System successfully loads existing flat snapshot files without data loss or errors (100% backward compatibility).
- **SC-004**: Delta application performance (time to apply a delta) is equal to or better than the current flat index implementation.
