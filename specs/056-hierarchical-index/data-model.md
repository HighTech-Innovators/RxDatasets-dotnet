# Data Model: Hierarchical Index

## Entities

### SnapshotIndex (Protobuf)
The root message for the dataset snapshot.

| Field | Type | Description |
|-------|------|-------------|
| `root` | `SnapshotNode` | The root node of the hierarchical index. |
| `snapshot_version` | `uint64` | The version number of the snapshot. |
| `entities` | `map<string, SnapshotEntity>` | **Legacy**: Flat map of entities. Ignored if `root` is present. |

### SnapshotNode (Protobuf)
A node in the hierarchical index tree.

| Field | Type | Description |
|-------|------|-------------|
| `entities` | `map<string, SnapshotEntity>` | Entities at this level (key is the last path segment). |
| `children` | `map<string, SnapshotNode>` | Child nodes (sub-directories). |

### ImmutableIndexNode (C#)
The in-memory representation of `SnapshotNode`.

| Property | Type | Description |
|----------|------|-------------|
| `Entities` | `ImmutableDictionary<string, SnapshotEntity>` | Entities at this level. |
| `Children` | `ImmutableDictionary<string, ImmutableIndexNode>` | Child nodes. |

## Relationships

- `SnapshotIndex` contains one `SnapshotNode` (Root).
- `SnapshotNode` contains multiple `SnapshotNode` (Children) and `SnapshotEntity` (Leaves).
- `ImmutableIndexNode` mirrors `SnapshotNode` structure.

## Validation Rules

- **Path Segments**: Keys in `entities` and `children` maps MUST NOT contain the separator character (`/`).
- **Uniqueness**: A path cannot be both an entity and a directory (implicit in the structure, but worth noting).
