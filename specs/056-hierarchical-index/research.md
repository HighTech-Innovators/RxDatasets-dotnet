# Research: Hierarchical Index Implementation

## 1. Immutable Hierarchical Data Structure (Trie) in C#

### Decision
Implement a custom **Immutable Trie** where each node is a class containing an `ImmutableDictionary<string, TrieNode>` for children and an optional value for the entity at that path.

```csharp
public class TrieNode
{
    // Using ImmutableDictionary for thread-safe structural sharing
    public ImmutableDictionary<string, TrieNode> Children { get; init; } = ImmutableDictionary<string, TrieNode>.Empty;
    
    // The entity data if this node represents a concrete item (e.g. "a/b" is a file)
    public SnapshotEntity? Entity { get; init; }
}
```

### Rationale
*   **Thread Safety**: `ImmutableDictionary` guarantees that readers always see a consistent snapshot of the data without locking. Writers can create a new version of the root without disturbing active readers.
*   **Structural Sharing**: When a leaf is updated, we only need to copy and update the nodes along the path to the root (Path Copying). The rest of the tree (unaffected branches) is shared between the old and new versions, minimizing memory allocation during updates.
*   **Performance**: `ImmutableDictionary` has O(log N) lookup characteristics. While slightly slower than a mutable `Dictionary`, it is sufficiently fast for index lookups and avoids the complexity of reader-writer locks.
*   **Memory**: Deduplicating common prefixes (e.g., "users/123/profile", "users/123/settings") significantly reduces string storage compared to storing full keys.

### Alternatives Considered
*   **`SortedList` / Array-based Nodes**:
    *   *Pros*: Lower memory overhead for nodes with very few children.
    *   *Cons*: O(N) insertion/update (requires copying the whole array). More complex to implement efficiently with structural sharing.
*   **Mutable Dictionary with Locks**:
    *   *Pros*: Faster single-threaded updates.
    *   *Cons*: Requires complex locking for concurrent readers/writers. Does not naturally support "snapshot" views for readers, which is a core requirement for the dataset reader.
*   **Struct vs Class for Nodes**:
    *   *Decision*: **Class**.
    *   *Reason*: Recursive structs require boxing or unsafe pointers. Classes allow reference sharing, which is essential for the immutable update pattern (sharing unchanged subtrees).

## 2. Protobuf Schema Design for Hierarchy

### Decision
Define a recursive `SnapshotNode` message and add it as a new field to the existing `SnapshotIndex`.

```protobuf
message SnapshotNode {
    // If this node represents a dataset entity, this field is populated.
    SnapshotEntity entity = 1;
    
    // Child nodes, keyed by the path segment (e.g., "users", "123").
    map<string, SnapshotNode> children = 2;
}

message SnapshotIndex {
    // ... legacy fields ...
    map<string, SnapshotEntity> entities = 2; // Deprecated but kept for backward compat if needed
    
    // New hierarchical root
    SnapshotNode root = 33; 
}
```

### Rationale
*   **Direct Mapping**: The schema maps 1:1 to the in-memory Trie structure, simplifying serialization/deserialization.
*   **Space Efficiency**: Common prefixes are stored only once in the hierarchy. The `map` structure in Protobuf is efficient (repeated key-value pairs).
*   **Backward Compatibility**: Adding a new field (`root = 33`) allows new readers to detect the presence of the hierarchical index. Old readers will simply ignore the field (though they won't find data if `entities` is empty, effectively enforcing an upgrade).

### Alternatives Considered
*   **Flat List with Depth/Parent Pointers**:
    *   *Pros*: Simpler schema.
    *   *Cons*: Harder to reconstruct the tree; loses the prefix deduplication benefits on disk.
*   **OneOf Field**:
    *   *Idea*: Use `oneof` to switch between `entities` and `root`.
    *   *Result*: Good, but adding a new field is safer for existing generated code that might not expect a `oneof` change.

## 3. Legacy Delta Conversion

### Decision
Perform **On-the-Fly Conversion** when reading legacy files.

*   **Reading**: When loading a legacy `SnapshotIndex` (where `root` is missing) or `DeltaIndex` (flat list), iterate through the flat keys, split them by the separator (`/`), and insert them into the in-memory Trie.
*   **Writing**: New snapshots are always written in the hierarchical format.
*   **Delta Migration**: The `DataMigrator` will read legacy flat deltas and rewrite them into a new hierarchical delta format (to be defined, likely mirroring `SnapshotNode` but with `DeltaEntity`).

### Rationale
*   **Simplicity**: We must support legacy files anyway. Converting them to the canonical in-memory format at load time ensures the rest of the application only deals with one data structure.
*   **Performance**: Splitting strings and building the Trie is fast enough for loading. The memory savings of the Trie will likely outweigh the CPU cost of conversion.

### Alternatives Considered
*   **Lazy Conversion**:
    *   *Idea*: Keep legacy data flat in memory and only build hierarchy for new writes.
    *   *Cons*: Violates the requirement for a single in-memory representation. Increases complexity of read/write logic (handling two modes).
