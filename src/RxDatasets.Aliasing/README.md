# DatasetAlias - Alias-Based Entity Reader

## Overview

`DatasetAlias` provides a generic `IDatasetReader<T>` wrapper implementation that allows querying entities by their aliases in addition to their primary IDs. This is particularly useful for scenarios where entities can be referenced by multiple alternative identifiers.

## Key Features

- **Bidirectional Mapping**: Maintains automatic mappings between aliases and entity IDs
- **Transparent Alias Resolution**: Queries using aliases are automatically translated to the primary entity ID
- **Background Synchronization**: Continuously synchronizes with the upstream reader to stay current with entity changes
- **Generic Implementation**: Works with any protobuf message type `T`
- **Dependency Injection**: Integrates seamlessly with ASP.NET Core DI container via keyed services
- **Multiple Alias Types**: Supports multiple different alias "names" per DTO type, each registered separately

## Architecture

### AliasDatasetReader<T>

The core implementation class that wraps an existing `IDatasetReader<T>`:

```csharp
public class AliasDatasetReader<T> : IDatasetReader<T>
    where T : class, IMessage<T>, new()
```

#### Key Components

1. **Alias Factory Function**: 
   - Signature: `Func<ReadOnlyMemory<byte>, IEnumerable<string>>`
   - Converts raw entity bytes to a list of aliases for that entity
   - Called during initialization and on each entity update

2. **Internal Mappings**:
   - `_aliasToIdMap`: Maps each alias → original entity ID (for lookups)
   - `_idToAliasesMap`: Maps each entity ID → list of its aliases (for updates/deletes)
   - `_knownIds`: Tracks which entity IDs exist

3. **Initialization**:
   - Loads all entities from version 0 via `GetChangedNames(0)`
   - Builds the complete alias mapping
   - Calls `StartInitialization()` method to trigger background sync

4. **Background Sync**:
   - Continuously monitors `WaitChangedNames()` for upstream changes
   - Updates alias mappings when entities are created or modified
   - Removes aliases when entities are deleted
   - Auto-reconnects on failures with 5-second retry delay

## Usage

### Registration

Register an alias reader in DI container for a specific DTO type and alias name:

```csharp
services.AddDatasetAlias<Link>(
    aliasName: "byitem",
    aliasFactory: entityBytes =>
    {
        // Deserialize entity and extract alias list
        var link = new Link();
        link.MergeFrom(entityBytes);
        
        // Return list of aliases for this link
        return link.GetItemAliases(); // custom logic
    }
);

// Multiple alias types can be registered
services.AddDatasetAlias<Link>(
    aliasName: "bybarcode",
    aliasFactory: entityBytes =>
    {
        var link = new Link();
        link.MergeFrom(entityBytes);
        return link.GetBarcodeAliases();
    }
);
```

### Retrieval

Get the alias reader from the service provider:

```csharp
var aliasReader = serviceProvider.GetDatasetReader<Link>("byitem");

// Query by alias (automatically resolved to primary ID)
var link = await aliasReader.Get("some-alias");

// Batch queries work too
var links = await aliasReader.BatchGet(new[] { "alias1", "alias2" });
```

### Alias Factory Implementation

The alias factory function receives raw protobuf bytes and should return a collection of aliases:

```csharp
Func<ReadOnlyMemory<byte>, IEnumerable<string>> factory = entityBytes =>
{
    var entity = new MyDtoType();
    entity.MergeFrom(entityBytes);
    
    // Extract aliases from entity
    // Example: a Link might have aliases based on item IDs
    var aliases = new List<string>();
    
    if (entity.Single?.LinkedItem != null)
    {
        aliases.Add($"item/{entity.Single.LinkedItem.ItemId}");
    }
    
    return aliases;
};
```

## Proto Example

From `link.v1.proto`, aliases are documented as:

```proto
message Link {
    string id = 1;
    
    // Multiple alias patterns can be defined
    // __aliasname__ __aliaspattern__ byitem ...
    // __aliasname__ __aliaspattern__ bybarcode ...
    
    // ... entity structure
}
```

## Thread Safety

- All internal mappings (`_aliasToIdMap`, `_idToAliasesMap`, `_knownIds`) are protected by a `lock` statement
- Background sync runs on a separate task to avoid blocking queries
- Initialization completes before any queries can proceed

## Implementation Details

### Initialization Sequence

1. Constructor creates the reader (not yet initialized)
2. `StartInitialization()` is called (by DI factory)
3. Loads all entities from version 0
4. Builds alias mappings
5. Starts background sync task
6. Sets initialization complete
7. From this point on, queries succeed

### Query Flow

1. User calls `Get(id)` or `BatchGet(ids)`
2. Waits for initialization if needed
3. Resolves alias → primary ID
4. Delegates to upstream reader with primary ID
5. Returns result

### Background Sync Flow

1. Watches upstream reader for changes via `WaitChangedNames()`
2. For each changed entity:
   - Fetches full entity via `Get()`
   - Removes old aliases if entity already existed
   - Registers new aliases via alias factory
3. Updates version pointer and loops
4. On error: waits 5 seconds and retries

## Limitations

- `List()` and `CountChildren()` operations throw `NotImplementedException` (alias readers focus on ID-based lookups)
- Only supports querying by alias/ID - does not support hierarchical listing
- Changes are only visible via the main reader's `WaitChangedNames()`, not through the alias reader

## Dependencies

- `Google.Protobuf` - Protobuf message support
- `HighTechInnovators.RxDatasets.Abstractions` - IDatasetReader interface
- `Microsoft.Extensions.DependencyInjection` - DI integration
- `Microsoft.Extensions.Logging` - Logging support

## Thread-Safety and Concurrency

The implementation is thread-safe for concurrent reads and updates:

- All mapping operations are locked
- Background sync runs independently without blocking queries
- Read operations (`Get`, `BatchGet`) check out the lock only briefly for resolution
- Initialization is guarded by a `TaskCompletionSource`

## Error Handling

- Invalid aliases return `null` or empty collections
- Failed entity loads during initialization are logged as warnings, not fatal
- Background sync failures are logged and trigger automatic retry
- Missing `StartInitialization()` call raises `InvalidOperationException`

## Logging

The implementation logs at various levels:

- `Information`: Initialization started/completed, sync started/stopped
- `Debug`: Alias registration/removal, query resolution, entity updates
- `Warning`: Failed entity loads, processing errors
- `Error`: Fatal initialization errors

Debug logs can be valuable for troubleshooting alias resolution issues.
