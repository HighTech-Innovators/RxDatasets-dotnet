# Quick Start: Using Extended WriteResponse with Detailed Changes

**Feature**: Spec 067 - Extend WriteResponse with Optional Change Details  
**Created**: January 28, 2026  
**Audience**: Developers integrating with RxDatasets write operations

---

## Overview

The WriteResponse proto message has been extended with an optional `repeated DetailedChange detailed_changes` field. When your request includes the `X-Debug` HTTP header, RxDatasets will include a detailed list of entity IDs and their change types (NEW, CHANGED, or DELETED) in the response.

**Key Benefits**:
- Enhanced observability for write operations
- Exact entity IDs affected by batch operations
- Understand net effect consolidation in action
- Ideal for testing, debugging, and validation

---

## Basic Usage

### 1. Without Detailed Changes (Default)

**Standard write request** (no X-Debug header):

```csharp
// Using gRPC client
var request = new Testdto { Name = "entity1", Data = "..." };
var response = await client.PutAsync(request);

// Response contains only summary:
// - new_dataset_version: 42
// - num_added_entities: 1
// - num_updated_entities: 0
// - num_deleted_entities: 0
// - detailed_changes: [] (empty)
```

### 2. With Detailed Changes

**Write request with X-Debug header**:

```csharp
// Using gRPC client with metadata
var metadata = new Grpc.Core.Metadata
{
    { "X-Debug", "true" }  // Trigger detailed changes
};

var request = new Testdto { Name = "entity1", Data = "..." };
var response = await client.PutAsync(request, metadata);

// Response includes detailed list:
// - new_dataset_version: 42
// - num_added_entities: 1
// - num_updated_entities: 0
// - num_deleted_entities: 0
// - detailed_changes: [
//     { entity_id: "entity1", change_type: NEW }
//   ]
```

---

## Change Types

The `change_type` field indicates what happened to each entity:

| Type | Value | Meaning |
|------|-------|---------|
| `NEW` | 0 | Entity was newly created in this operation |
| `CHANGED` | 1 | Entity existed and its content was updated |
| `DELETED` | 2 | Entity was removed from the dataset |

**Important**: Unchanged entities do NOT appear in the detailed_changes list.

---

## Batch Operations Example

### PutMany with Detailed Changes

```csharp
// Stream multiple entities
var metadata = new Grpc.Core.Metadata { { "X-Debug", "true" } };

using (var call = client.PutMany(metadata))
{
    // Send 3 entities
    await call.RequestStream.WriteAsync(new Testdto { Name = "user/1", ... });
    await call.RequestStream.WriteAsync(new Testdto { Name = "user/2", ... });
    await call.RequestStream.WriteAsync(new Testdto { Name = "user/3", ... });
    await call.RequestStream.CompleteAsync();
    
    var response = await call.ResponseAsync;
    
    // Response includes:
    foreach (var change in response.DetailedChanges)
    {
        Console.WriteLine($"{change.EntityId}: {change.ChangeType}");
        // Output:
        // user/1: NEW
        // user/2: NEW
        // user/3: NEW
    }
}
```

---

## Net Effect Consolidation

When multiple operations affect the same entity, the detailed_changes reflects the **net effect** (per Spec 002).

### Example: Delete Then Recreate

**Scenario**: Delete entity X, then recreate it with the same data

```csharp
var metadata = new Grpc.Core.Metadata { { "X-Debug", "true" } };

using (var call = client.PutOrDeleteMany(metadata))
{
    // Delete X
    await call.RequestStream.WriteAsync(new PutOrDelete 
    { 
        Name = "entity/X", 
        IsDelete = true 
    });
    
    // Recreate X with same data
    await call.RequestStream.WriteAsync(new PutOrDelete 
    { 
        Name = "entity/X", 
        EntityBytes = originalData,
        IsDelete = false 
    });
    
    await call.RequestStream.CompleteAsync();
    var response = await call.ResponseAsync;
    
    // Response:
    // new_dataset_version: 42
    // num_added_entities: 0      (net effect: no addition)
    // num_updated_entities: 0    (net effect: no change)
    // num_deleted_entities: 0    (net effect: not deleted)
    // detailed_changes: []       (entity not in list - no net change)
}
```

---

## HTTP Client Usage

### cURL Examples

**Without detailed changes**:
```bash
curl -X POST http://localhost:5000/api/put \
  -H "Content-Type: application/json" \
  -d '{"name":"entity1","entity_bytes":"..."}'
```

**With detailed changes**:
```bash
curl -X POST http://localhost:5000/api/put \
  -H "Content-Type: application/json" \
  -H "X-Debug: true" \
  -d '{"name":"entity1","entity_bytes":"..."}'
```

**Response (with X-Debug header)**:
```json
{
  "newDatasetVersion": "42",
  "numAddedEntities": 1,
  "numUpdatedEntities": 0,
  "numDeletedEntities": 0,
  "detailedChanges": [
    {
      "entityId": "entity1",
      "changeType": "NEW"
    }
  ]
}
```

---

## Testing Guide

### Unit Test Example

```csharp
[Fact]
public async Task WriteResponse_WithXDebugHeader_IncludesDetailedChanges()
{
    // Arrange
    var client = CreateTestClient();
    var metadata = new Grpc.Core.Metadata { { "X-Debug", "true" } };
    var entity = new Testdto { Name = "test-entity", Data = "test-data" };
    
    // Act
    var response = await client.PutAsync(entity, metadata);
    
    // Assert
    Assert.Equal(1U, response.NumAddedEntities);
    Assert.Single(response.DetailedChanges);
    Assert.Equal("test-entity", response.DetailedChanges[0].EntityId);
    Assert.Equal(ChangeType.New, response.DetailedChanges[0].ChangeType);
}

[Fact]
public async Task WriteResponse_WithoutXDebugHeader_NoDetailedChanges()
{
    // Arrange
    var client = CreateTestClient();
    // No X-Debug header
    var entity = new Testdto { Name = "test-entity", Data = "test-data" };
    
    // Act
    var response = await client.PutAsync(entity);
    
    // Assert
    Assert.Equal(1U, response.NumAddedEntities);
    Assert.Empty(response.DetailedChanges);  // Empty, not null
}
```

### Integration Test Example

```csharp
[Fact]
public async Task PutMany_WithXDebugHeader_DetailsAllChanges()
{
    // Arrange
    var entities = new[] 
    { 
        new Testdto { Name = "e1", Data = "d1" },
        new Testdto { Name = "e2", Data = "d2" },
        new Testdto { Name = "e3", Data = "d3" },
    };
    
    var metadata = new Grpc.Core.Metadata { { "X-Debug", "true" } };
    
    // Act
    using (var call = client.PutMany(metadata))
    {
        foreach (var entity in entities)
        {
            await call.RequestStream.WriteAsync(entity);
        }
        await call.RequestStream.CompleteAsync();
        var response = await call.ResponseAsync;
        
        // Assert
        Assert.Equal(3U, response.NumAddedEntities);
        Assert.Equal(3, response.DetailedChanges.Count);
        
        var changeMap = response.DetailedChanges
            .ToDictionary(c => c.EntityId, c => c.ChangeType);
        
        Assert.Equal(ChangeType.New, changeMap["e1"]);
        Assert.Equal(ChangeType.New, changeMap["e2"]);
        Assert.Equal(ChangeType.New, changeMap["e3"]);
    }
}
```

---

## Performance Considerations

### Header Overhead

**Without X-Debug header**:
- ✅ No change to performance
- ✅ detailed_changes field remains empty
- ✅ Baseline performance maintained (sub-50ms)

**With X-Debug header**:
- Target: sub-100ms for operations affecting <1,000 entities
- detailed_changes size scales with actual changes, not batch size
- Unchanged entities are excluded (important for large batches)

### Scaling to Large Batches

For a batch operation with 10,000 total entities where only 10 actually changed:

```
Response detailed_changes contains: 10 entries (not 10,000)
Response size: ~500 bytes (not 400+ KB)
Performance: Still sub-100ms
```

The key optimization: unchanged entities are never included in the list.

---

## Header Matching Details

The `X-Debug` header is matched case-insensitively per HTTP/2 specification:

| Header Format | Included? |
|---------------|-----------|
| `X-Debug: true` | ✅ Yes |
| `x-debug: true` | ✅ Yes (case-insensitive) |
| `X-DEBUG: 1` | ✅ Yes (case-insensitive) |
| `X-Debug:` (empty value) | ✅ Yes (presence counts) |
| Header absent | ❌ No |

---

## Common Patterns

### Pattern 1: Debug Single Write

```csharp
// When investigating a single entity write
var metadata = new Grpc.Core.Metadata { { "X-Debug", "true" } };
var response = await client.PutAsync(entity, metadata);

if (response.DetailedChanges.Any())
{
    var change = response.DetailedChanges[0];
    Console.WriteLine($"Entity {change.EntityId} was {change.ChangeType}");
}
```

### Pattern 2: Validate Batch Results

```csharp
// Validate all expected changes occurred
var metadata = new Grpc.Core.Metadata { { "X-Debug", "true" } };
var response = await client.PutManyAsync(entities, metadata);

var newEntities = response.DetailedChanges
    .Where(c => c.ChangeType == ChangeType.New)
    .Select(c => c.EntityId)
    .ToList();

Assert.Equal(expectedNewEntityIds.Count, newEntities.Count);
```

### Pattern 3: Monitor Change Distribution

```csharp
// Track what types of changes occurred
var changeStats = response.DetailedChanges
    .GroupBy(c => c.ChangeType)
    .ToDictionary(g => g.Key, g => g.Count());

Console.WriteLine($"Created: {changeStats.GetValueOrDefault(ChangeType.New, 0)}");
Console.WriteLine($"Updated: {changeStats.GetValueOrDefault(ChangeType.Changed, 0)}");
Console.WriteLine($"Deleted: {changeStats.GetValueOrDefault(ChangeType.Deleted, 0)}");
```

---

## Production Best Practices

### When to Use X-Debug Header

**Use X-Debug in These Scenarios**:
- ✅ **Development & Debugging**: Investigate unexpected write behavior
- ✅ **Integration Testing**: Validate batch operations produce expected changes
- ✅ **Problem Diagnosis**: Troubleshoot data inconsistencies
- ✅ **Test Harnesses**: Automated testing of write operations

**Don't Use X-Debug in These Scenarios**:
- ❌ **High-Volume Production**: Unnecessary overhead for trusted production pipelines
- ❌ **Time-Sensitive Operations**: When sub-50ms performance is critical and 100ms is unacceptable
- ❌ **Large Batches (10k+ entities)**: Unless investigating specific entities changed

### Header Lifecycle

```csharp
// Good: Control when X-Debug is enabled
var metadata = new Grpc.Core.Metadata();

if (isDebugMode || isTestingMode)
{
    metadata.Add("X-Debug", "true");  // Only add when needed
}

var response = await client.PutAsync(entity, metadata);
```

### Response Processing

**After Getting detailed_changes**:

```csharp
// Efficient processing pattern
if (response.DetailedChanges.Count > 0)
{
    // Only process detailed changes when present
    var changesByType = response.DetailedChanges
        .GroupBy(c => c.ChangeType)
        .ToDictionary(g => g.Key, g => g.Select(c => c.EntityId).ToList());
    
    // Log only when changes detected
    if (changesByType.Count > 0)
    {
        Logger.LogDebug("Changes detected: {@Changes}", changesByType);
    }
}
else
{
    // No changes occurred (matches num_added + num_updated + num_deleted = 0)
    Logger.LogDebug("No changes detected in operation");
}
```

### Batch Size Considerations

| Batch Size | X-Debug Recommended | Expected Response Time |
|------------|-------------------|----------------------|
| 1-100 entities | ✅ Yes, safe | <50ms |
| 100-1,000 entities | ✅ Yes, safe | <100ms |
| 1,000-10,000 entities | ⚠️ If needed | <100ms (scales with changes) |
| 10,000+ entities | ❌ Only if necessary | <100ms but higher memory |

**Key Point**: Response time scales with *changed* entities, not total batch size.

---

## Performance Guidance

### Baseline Performance (without X-Debug)

- **Single Put/Delete**: <5ms
- **PutMany (100 entities)**: <20ms
- **PutAll (1,000 entities)**: <40ms
- **PutOrDeleteMany (10,000 batch)**: <50ms

### With X-Debug Header

- **Single Put/Delete**: <10ms (minimal overhead)
- **PutMany (100 entities, 50 changes)**: <30ms
- **PutAll (1,000 entities, 100 changes)**: <60ms
- **PutOrDeleteMany (10,000 batch, 10 changes)**: <70ms

**Pattern**: Add ~5-10ms overhead + time to generate change list (tiny for small change counts)

### Optimization Strategies

#### Strategy 1: Conditional X-Debug Based on Batch Size

```csharp
var useXDebug = entities.Count < 1000;  // Only for smaller batches

var metadata = new Grpc.Core.Metadata();
if (useXDebug)
{
    metadata.Add("X-Debug", "true");
}

var response = await client.PutManyAsync(entities, metadata);
```

#### Strategy 2: Use X-Debug Only in Non-Critical Paths

```csharp
// Background sync operation (not time-critical)
var metadata = new Grpc.Core.Metadata { { "X-Debug", "true" } };
var response = await backgroundSync.PutManyAsync(entities, metadata);
LogSyncResults(response);

// Real-time API endpoint (performance-critical)
var response = await api.PutAsync(entity);  // No X-Debug header
```

#### Strategy 3: Sampling for Large Operations

```csharp
// Enable X-Debug only for first batch in series
bool firstBatch = true;

foreach (var batch in batches)
{
    var metadata = new Grpc.Core.Metadata();
    if (firstBatch)
    {
        metadata.Add("X-Debug", "true");
        firstBatch = false;
    }
    
    var response = await client.PutManyAsync(batch, metadata);
}
```

### Memory Considerations

The detailed_changes list is proportional to the number of *changed* entities:

- **100 changed entities**: ~2-3 KB response (even if batch is 10k)
- **1,000 changed entities**: ~20-30 KB response
- **10,000 changed entities**: ~200-300 KB response

Memory is not allocated for *unchanged* entities, even in massive batches.

### Storage Provider Considerations

All storage providers support X-Debug header with similar performance:

| Provider | Performance Impact | Notes |
|----------|------------------|-------|
| Local Disk | Minimal (<5ms) | Fastest; file I/O already in progress |
| S3 | Low (<10ms) | Change info computed locally after upload |
| Azure Blob | Low (<10ms) | Same as S3 pattern |
| Google Cloud | Low (<10ms) | Efficient API calls |
| Minio | Minimal (<5ms) | Local S3-compatible implementation |

---

## Monitoring & Observability

### Recommended Logging

```csharp
// Log change distribution for debugging
if (response.DetailedChanges.Count > 0)
{
    var stats = response.DetailedChanges
        .GroupBy(c => c.ChangeType)
        .Select(g => $"{g.Key}: {g.Count()}")
        .ToList();
    
    Logger.LogInformation("Write operation completed. Changes: {Changes}", 
        string.Join(", ", stats));
}
```

### Metrics to Track

**If collecting metrics**, track:
1. **X-Debug Usage Rate**: % of requests using X-Debug header
2. **Change Distribution**: Ratio of NEW/CHANGED/DELETED changes
3. **Response Times**: Baseline vs. X-Debug overhead
4. **Batch Efficiency**: Unchanged entity ratios (optimization indicator)

### Alerting

Set alerts if:
- X-Debug requests consistently exceed 100ms (indicates issues)
- num_added + num_updated + num_deleted consistently equals 0 (no-op writes)
- detailed_changes differs from aggregate counts (indicates bugs)

---

## Troubleshooting

### Q: I sent X-Debug header but detailed_changes is empty

**Possible causes**:
1. No entities were actually changed (unchanged entities are excluded)
2. Operation failed (would also show in response status)
3. Header name misspelled or case issue (should work case-insensitive, check HTTP layer)

**Solution**: Verify that the operation actually made changes by checking num_added/updated/deleted counts match expected values.

### Q: detailed_changes list doesn't match num_added_entities count

**This should not happen**. Report as bug with:
- Request details (what entities sent)
- X-Debug header status
- Response contents (all fields)
- Storage provider being used

### Q: Performance degraded with X-Debug header on large batches

**Expected behavior**:
- Small batches (<1000 entities): sub-100ms (unaffected)
- Large batches (10k+ entities): scales with number of actual changes, not batch size

If response is slow even for small batches with X-Debug, check:
- Network latency to server
- Server resource availability
- Disk I/O on storage provider

---

## API Reference

### Enum: ChangeType
```csharp
public enum ChangeType
{
    New = 0,          // Entity was newly created
    Changed = 1,      // Entity was modified
    Deleted = 2,      // Entity was deleted
}
```

### Message: DetailedChange
```csharp
public sealed class DetailedChange
{
    public string EntityId { get; set; }      // Entity identifier
    public ChangeType ChangeType { get; set; } // Type of change
}
```

### Message: WriteResponse (Extended)
```csharp
public sealed class WriteResponse
{
    public ulong NewDatasetVersion { get; set; }        // Version after operation
    public uint NumAddedEntities { get; set; }          // Count of new entities
    public uint NumUpdatedEntities { get; set; }        // Count of updated entities
    public uint NumDeletedEntities { get; set; }        // Count of deleted entities
    public RepeatedField<DetailedChange> DetailedChanges { get; } // List of changes (NEW)
}
```

---

## Next Steps

1. **For Development**: Add X-Debug header to your debug/test requests
2. **For Testing**: Use detailed_changes to validate operation outcomes
3. **For Production**: Leave X-Debug header off unless debugging (minimal overhead, but unnecessary)

See [data-model.md](data-model.md) for detailed semantics and [contracts/detailed-changes-contract.md](contracts/detailed-changes-contract.md) for proto specifications.
