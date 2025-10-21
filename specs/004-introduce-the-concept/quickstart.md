# Quickstart: Dataset Indexes (Aliases)

**Feature**: Dataset Indexes for Entities  
**Branch**: `004-introduce-the-concept`  
**Date**: October 21, 2025

This quickstart demonstrates how to create and use Dataset Indexes to access entities via semantic aliases.

---

## Overview

Dataset Indexes provide alias-based access to entities. Instead of remembering complex primary IDs, you can reference entities via meaningful alias paths derived from their content.

### Example Scenario
A `Link` entity represents a relationship between items and store ESL displays:
```csharp
// Primary entity
Name: "l001"
Content: {
  Items: ["item1", "item2"],
  Barcode: "barcode123"
}

// Generated aliases
- items/item1/links/l001  (item1 can reference this link)
- items/item2/links/l001  (item2 can reference this link)
- storeesl/barcode123/link  (ESL can reference via barcode)
```

All three paths can be used to read the same Link entity.

---

## Setup

### 1. Register the Index at Startup

```csharp
// In your host builder / startup configuration
services.AddDatasetIndex<LinkEntity>(
    indexName: "links_by_item",
    aliasMappingFunction: (entityName, linkEntity) =>
    {
        var aliases = new List<string>();

        // Item-based aliases
        foreach (var item in linkEntity.Items)
        {
            aliases.Add($"items/{item}/links/{entityName}");
        }

        // Barcode-based alias
        aliases.Add($"storeesl/{linkEntity.Barcode}/link");

        return aliases.ToArray();
    }
);
```

### 2. Inject the Index

```csharp
public class LinkService
{
    private readonly IDatasetIndex<LinkEntity> _linkIndex;

    public LinkService(IDatasetIndex<LinkEntity> linkIndex)
    {
        _linkIndex = linkIndex;
    }

    // Now use _linkIndex for queries...
}
```

---

## Common Patterns

### Pattern 1: Read a Single Entity via Alias

```csharp
// Read via item-based alias
var link = await _linkIndex.Read("items/item1/links/l001");

// Read via barcode-based alias
var sameLink = await _linkIndex.Read("storeesl/barcode123/link");

// Both return the same Link entity
Assert.Equal(link.Id, sameLink.Id);
```

### Pattern 2: Batch Read Multiple Entities

```csharp
// Query multiple entities efficiently
var aliasedPaths = new[]
{
    "items/item1/links/l001",
    "items/item2/links/l002",
    "items/item3/links/l003"
};

await foreach (var link in _linkIndex.BatchRead(aliasedPaths))
{
    Console.WriteLine($"Link: {link.Id}");
}
```

### Pattern 3: Count Entities at an Alias Prefix

```csharp
// Count all links associated with item1
var itemLinkCount = await _linkIndex.CountChildren("items/item1/links");
// Returns: 3 (if 3 links have "item1" in their items array)

// Count all ESL links
var eslLinkCount = await _linkIndex.CountChildren("storeesl");
// Returns: total number of links with barcode aliases
```

### Pattern 4: Handle Non-Existent Aliases

```csharp
try
{
    var link = await _linkIndex.Read("items/nonexistent/links/xyz");
}
catch (IndexOutOfRangeException)
{
    Console.WriteLine("Alias not found");
}

// Alternatively, use BatchRead (silently skips non-existent aliases)
var results = _linkIndex.BatchRead(new[] { "invalid/path" });
var count = 0;
await foreach (var _ in results)
{
    count++;
}
Console.WriteLine($"Found {count} entities"); // 0
```

---

## Index Behavior

### State Synchronization

The index is **automatically synchronized** with the underlying dataset:
- When a Link entity is created → its aliases are immediately available
- When a Link is updated (items change) → aliases are recalculated and updated
- When a Link is deleted → its aliases are removed

```csharp
// Index automatically stays in sync
linkWriter.Put("l001", newLinkWithDifferentItems); // May generate new aliases
await Task.Delay(100); // Wait for change feed

// New aliases are now available
var link = await _linkIndex.Read("items/item3/links/l001"); // Works if item3 now in items
```

### "Last One Wins" Conflict Resolution

If two entities generate the same alias, the latest one wins:

```csharp
// Entity A generates: "items/item1/link"
// Entity B (created later) also generates: "items/item1/link"

var link = await _linkIndex.Read("items/item1/link");
// Returns: Entity B (last one wins)

// If B is later deleted, the alias is completely removed
// (doesn't revert back to Entity A)
```

---

## Integration with gRPC Reader Service

### Server Side

The gRPC reader service is extended to support index-based queries:

```csharp
// In your gRPC service definition
// The Read, BatchRead, and CountChildren methods accept optional indexName
```

### Client Side

```csharp
// Client can specify which index to use
var response = await grpcClient.Read(new ReadRequest
{
    IndexName = "links_by_item",  // Optional: if provided, routes through index
    Path = "items/item1/links/l001"
});

// If indexName is null → uses direct reader (no index resolution)
var directResponse = await grpcClient.Read(new ReadRequest
{
    Path = "l001"  // Direct read by primary ID
});
```

---

## Best Practices

### ✅ DO

- **Use meaningful aliases** that reflect business semantics
- **Use CountChildren** for hierarchical aggregation queries
- **Use BatchRead** when querying multiple related entities
- **Rely on automatic synchronization** (index stays up-to-date automatically)
- **Register indexes at startup** to ensure availability throughout application lifetime

### ❌ DON'T

- **Store alias paths as primary IDs** (aliases are derived; use primary ID for persistence)
- **Expect ListChildren results** (use CountChildren for prefix queries instead)
- **Try to write to the index** (read-only in this iteration)
- **Manually synchronize** (index handles this automatically)
- **Create multiple indexes on same entity type** without distinct names (use unique names)

---

## Example: Complete Service

```csharp
public class LinkService
{
    private readonly IDatasetIndex<LinkEntity> _linkIndex;
    private readonly IDatasetReader<LinkEntity> _directReader;

    public LinkService(
        IDatasetIndex<LinkEntity> linkIndex,
        IDatasetReader<LinkEntity> directReader)
    {
        _linkIndex = linkIndex;
        _directReader = directReader;
    }

    // Read via primary ID
    public async Task<LinkEntity> GetByIdAsync(string id)
    {
        return await _directReader.Read(id);
    }

    // Read via item alias
    public async Task<LinkEntity> GetByItemAsync(string itemId, string linkId)
    {
        var aliasPath = $"items/{itemId}/links/{linkId}";
        return await _linkIndex.Read(aliasPath);
    }

    // Read via barcode alias
    public async Task<LinkEntity> GetByBarcodeAsync(string barcode)
    {
        return await _linkIndex.Read($"storeesl/{barcode}/link");
    }

    // Count links for an item
    public async Task<long> CountLinksForItemAsync(string itemId)
    {
        return await _linkIndex.CountChildren($"items/{itemId}/links");
    }

    // Get all links for an item (via batch read)
    public async Task<List<LinkEntity>> GetLinksForItemAsync(string itemId)
    {
        var results = new List<LinkEntity>();
        await foreach (var link in _linkIndex.BatchRead(new[] { $"items/{itemId}/links" }))
        {
            results.Add(link);
        }
        return results;
    }
}
```

---

## FAQ

**Q: Can I create multiple indexes on the same entity type?**  
A: Yes. Register them with different `indexName` parameters. Each index can have different alias mapping logic.

**Q: What happens if an entity's aliases change?**  
A: The index automatically recalculates aliases on update and removes old aliases that are no longer generated.

**Q: Can I write through the index?**  
A: Not in this iteration. Index is read-only. Write directly to the underlying reader/writer.

**Q: What if an alias resolves to multiple entities?**  
A: The latest entity in the change feed wins ("last one wins"). Earlier mapping is overwritten.

**Q: Is the index persistent?**  
A: No, it's in-memory. It's initialized from the underlying dataset's change feed on startup.

**Q: How performant is alias resolution?**  
A: O(1) hash table lookup for alias-to-entity mapping, then delegates to underlying reader. Very fast.

---

## Next Steps

1. **Register your indexes** in startup configuration
2. **Inject IDatasetIndex<T>** into your services
3. **Define alias mapping functions** based on your business logic
4. **Use Read, BatchRead, CountChildren** for queries

For implementation details, see:
- `data-model.md` - Data structures and interfaces
- `contracts/` - gRPC service contracts
- `tasks.md` - Development tasks

---

**Status**: ✅ **Quickstart Ready**
