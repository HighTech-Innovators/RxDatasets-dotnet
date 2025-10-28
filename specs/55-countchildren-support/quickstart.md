# CountChildren API - Quickstart Guide

**Feature**: CountChildren Support for ReaderService  
**Date**: October 28, 2025

## Overview

The CountChildren API provides an efficient way to determine the number of child entities under a specific parent path without fetching the entity data itself. This is useful for:

- Displaying total counts in UI
- Planning pagination strategies
- Resource allocation decisions
- Hierarchical data structure validation

## API Contract

### Service Method

```csharp
// Interface definition
Task<uint> CountChildren(string parent, CancellationToken cancellationToken = default);

// gRPC service method
rpc CountChildren(CountChildrenRequest) returns (CountChildrenResponse);
```

### Messages

**CountChildrenRequest**:
```protobuf
message CountChildrenRequest {
    string parent = 1;  // The parent path to count children for
}
```

**CountChildrenResponse**:
```protobuf
message CountChildrenResponse {
    uint32 count = 1;   // Number of child entities (0 to 4,294,967,295)
}
```

## Usage Examples

### Direct API Usage (C#)

```csharp
using RxDatasets;
using RxDatasets.Abstractions;

// Get a dataset reader instance
var reader = GetDatasetReader<MyEntity>();

// Count children under a parent
var childCount = await reader.CountChildren("parent/path");

// Use with cancellation token
var cts = new CancellationTokenSource();
try 
{
    var count = await reader.CountChildren("parent/path", cts.Token);
    Console.WriteLine($"Found {count} child entities");
}
catch (OperationCanceledException)
{
    Console.WriteLine("Count operation was cancelled");
}
```

### gRPC Client Usage (C#)

```csharp
using Hightechinnovators.Dtos.DemodtoV1;
using Grpc.Net.Client;

// Connect to gRPC service
var channel = GrpcChannel.ForAddress("https://localhost:5001");
var client = new ReaderService.ReaderServiceClient(channel);

// Create request
var request = new CountChildrenRequest 
{ 
    Parent = "departments/sales"
};

// Call gRPC method
var response = await client.CountChildrenAsync(request);
Console.WriteLine($"Department has {response.Count} employees");
```

## Behavior

### Return Values

- **Valid parent with children**: Returns count of direct children (1+)
- **Valid parent with no children**: Returns 0
- **Non-existent parent**: Returns 0 (not an error)

### Error Handling

| Condition | Response |
|-----------|----------|
| Malformed parent path | RpcException with InvalidArgument status |
| Operation cancelled | OperationCanceledException / RpcException(Cancelled) |
| Service unavailable | RpcException with Unavailable status |

### Performance Characteristics

- **Target latency**: < 100ms for datasets with up to 10,000 children
- **Memory**: Minimal - uses name listing, no entity data fetched
- **Consistency**: Returns snapshot reflecting current dataset state

## Implementation Notes

### For Library Consumers

The CountChildren method is available on any IDatasetReader implementation:

```csharp
// Works with all dataset reader implementations
IDatasetReader<MyEntity> reader = dataset; // or any implementation
uint count = await reader.CountChildren("parent/path");
```

### For RxDatasets Contributors

Implementation details:

- Method added to `IDatasetReader<T>` interface
- `GrpcReaderServiceImpl<T>` implements the gRPC service method
- Uses existing `List()` infrastructure for name enumeration

## Testing

### Unit Test Example

```csharp
[Fact]
public async Task CountChildren_WithParentHavingChildren_ReturnsCorrectCount()
{
    // Arrange
    var reader = GetMockReader();
    reader.SetupChildren("parent/path", 5);  // 5 children
    
    // Act
    var count = await reader.CountChildren("parent/path");
    
    // Assert
    Assert.Equal(5u, count);
}

[Fact]
public async Task CountChildren_WithNonExistentParent_ReturnsZero()
{
    // Arrange
    var reader = GetMockReader();
    
    // Act
    var count = await reader.CountChildren("nonexistent/path");
    
    // Assert
    Assert.Equal(0u, count);
}
```

### Integration Test Example

```csharp
[Fact]
public async Task CountChildren_GrpcService_ReturnsCorrectCount()
{
    // Arrange
    var testHost = await CreateTestHost();
    var channel = CreateGrpcChannel(testHost);
    var client = new ReaderService.ReaderServiceClient(channel);
    
    // Act
    var response = await client.CountChildrenAsync(
        new CountChildrenRequest { Parent = "test/parent" }
    );
    
    // Assert
    Assert.True(response.Count > 0);
}
```

## Related APIs

- `Task<(List<string> Names, ulong LatestVersion)> List(string parent, CancellationToken ct)` - Fetch actual child names
- `Task<(string name, ReadOnlyMemory<byte>? entityBytes)> Get(string name)` - Fetch individual entity
- `Task<ICollection<TEntity>> BatchGet(...)` - Fetch multiple entities

## Troubleshooting

### Getting 0 count when expecting > 0

- Verify parent path exists and is correctly formatted
- Confirm entities are actually under the specified parent
- Check with `List()` method to see actual child names

### Performance Issues

- Ensure parent paths don't have excessive children (> 10,000)
- For larger datasets, consider pagination using `List()` instead
- Monitor network latency if using gRPC over high-latency connections

### Cancellation Not Responding

- Ensure CancellationToken is properly propagated
- Check service hasn't been shut down during operation
- Verify timeout is set appropriately on gRPC client
