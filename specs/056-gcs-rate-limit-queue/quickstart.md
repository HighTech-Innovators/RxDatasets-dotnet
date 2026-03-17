# Quick Start: GCS Write Queue Implementation

**Feature**: 056-gcs-rate-limit-queue  
**Date**: 2025-12-22  
**Phase**: 1 - Design & Contracts

---

## Overview

This quickstart shows how to use the improved GoogleCloudStorage with write queue optimization. Since queueing is automatic, no code changes are required—but understanding the behavior is useful for testing and troubleshooting.

---

## Basic Usage (No Changes Required)

### Current Working Code

Your existing code continues to work unchanged and automatically benefits from faster writes:

```csharp
using System.Net.Http.Headers;
using Google.Cloud.Storage.V1;
using Microsoft.Extensions.Logging;
using RxDatasets.StorageProvider.GoogleCloud;

// Initialize storage provider
var logger = LoggerFactory.CreateLogger<GoogleCloudStorage<T>>();
var options = new GoogleCloudConfig { BucketName = "my-bucket" };
var storage = new GoogleCloudStorage<T>(logger, new OptionsWrapper<GoogleCloudConfig>(options));

// Upload data - now returns immediately if queued!
var data = Encoding.UTF8.GetBytes("important data");
await storage.UploadBytes("path/to/file.pb", data);
// If last write was < 2 seconds ago, this returns in < 50ms
// If last write was >= 2 seconds ago, proceeds with normal write

// Download, delete operations unchanged
var content = await storage.DownloadBytes("path/to/file.pb");
await storage.DeleteFile("path/to/file.pb");

// Clean shutdown - flushes all queued writes before returning
await storage.DisposeAsync();
```

---

## How It Works Under the Hood

### Scenario 1: Write Within 2 Seconds of Last Write (Queued)

```
Time: 0ms   | storage.UploadBytes("delta.pb", data1) called
            | ✓ No prior writes → proceed with normal write
            | Write succeeds, timestamp recorded

Time: 500ms | storage.UploadBytes("delta.pb", data2) called
            | ⚠ Last write was only 500ms ago (< 2 seconds)
            | ✓ Write is queued (internally)
            | ✓ Returns immediately (< 50ms) to caller
            | Flush scheduled for ~1500ms from now

Time: 2000ms | Flush trigger fires
             | Dequeue and write data2 to GCS
             | ✓ Write succeeds
             | Update timestamp
             | Queue now empty
```

**Result**: Caller gets immediate response instead of 5.5+ second wait.

### Scenario 2: Write After 2-Second Gap (No Queue Needed)

```
Time: 0ms   | storage.UploadBytes("delta.pb", data1)
            | ✓ Write succeeds

Time: 5000ms | storage.UploadBytes("delta.pb", data2)
             | ✓ Last write was 5 seconds ago (>= 2 seconds)
             | ✓ Proceed with normal write (queue not used)
             | Write succeeds

Result: Normal behavior; queue never engaged
```

### Scenario 3: Multiple Writes to Same File in Rapid Succession

```
Time: 0ms   | storage.UploadBytes("delta.pb", update1)
            | ✓ Proceed with normal write → succeeds

Time: 100ms | storage.UploadBytes("delta.pb", update2)
            | ⚠ < 2 seconds since write #1
            | ✓ Queued

Time: 200ms | storage.UploadBytes("delta.pb", update3)
            | ⚠ < 2 seconds since write #1
            | ✓ Queued (multiple pending writes)

Time: 2100ms | Flush fires
             | Dequeue update2 → write to GCS (update2 overwrites original)
             | Dequeue update3 → write to GCS (update3 overwrites update2)
             | Only the LAST write persists
             
Result: Three API calls, but only final state written to GCS
        (same as if they were sequential writes in real-time)
```

---

## Testing the Queue Behavior

### Automated Testing with Testcontainers

All testing is automated using testcontainers (GCS emulator). No manual testing required.

Tests use the `IClock` interface (from RxDatasets.BackgroundServices) for time simulation, enabling fast test execution without actual delays:

### Unit Test: Verify Queuing for Rapid Writes

```csharp
[Fact]
public async Task RapidWritesToSameFileAreQueued()
{
    // Use testcontainer for isolated GCS environment
    using var container = new GcsEmulatorContainer(); // automatically started/stopped
    var logger = new TestLogger();
    
    // Create test clock for time simulation (no actual delays)
    var testClock = new TestClock(); // Allows fast-forwarding time
    var storage = new GoogleCloudStorage<T>(logger, 
        new GoogleCloudConfig { BucketName = container.BucketName },
        clock: testClock);

    // Write 1: Succeeds normally
    var data1 = Encoding.UTF8.GetBytes("version 1");
    var sw = Stopwatch.StartNew();
    await storage.UploadBytes("delta.pb", data1);
    sw.Stop();
    Assert.True(sw.ElapsedMilliseconds < 100);

    // Write 2: Queued (< 2 seconds later) - should return immediately
    var data2 = Encoding.UTF8.GetBytes("version 2");
    sw = Stopwatch.StartNew();
    await storage.UploadBytes("delta.pb", data2);
    sw.Stop();
    Assert.True(sw.ElapsedMilliseconds < 50); // Queued writes are fast
    
    // Verify queued write was logged
    Assert.Contains("Write queued", logger.GetMessages());

    // Fast-forward time to trigger flush (no actual delay)
    testClock.Advance(TimeSpan.FromSeconds(2.1));
    
    // Verify data persisted to GCS
    var content = await storage.DownloadBytes("delta.pb");
    Assert.Equal(data2, content); // Final write persisted
}
```

### Integration Test: Graceful Shutdown Flushes Queue

```csharp
[Fact]
public async Task DisposalFlushesQueuedWrites()
{
    var storage = new GoogleCloudStorage<T>(logger, options);

    // Write 1: Normal
    await storage.UploadBytes("delta.pb", data1);

    // Write 2: Queued (< 2 seconds later)
    var sw = Stopwatch.StartNew();
    await storage.UploadBytes("delta.pb", data2);
    sw.Stop();
    Assert.True(sw.ElapsedMilliseconds < 50); // Immediate return

    // Immediate disposal - should flush before returning
    var disposalSw = Stopwatch.StartNew();
    await storage.DisposeAsync();
    disposalSw.Stop();

    // Disposal waits for queue flush (2 seconds) plus actual write
    Assert.True(disposalSw.ElapsedMilliseconds >= 2000);
    
    // Verify all data was written
    var gcs = GetRealGCSClient();
    var data = await gcs.DownloadObjectAsync(bucket, "delta.pb");
    Assert.Equal(data2, data); // Final write is present
}
```

### Load Test: Multiple Files Queued Simultaneously

```csharp
[Fact]
public async Task MultipleFilesCanHaveQueuesSimultaneously()
{
    var storage = new GoogleCloudStorage<T>(logger, options);

    // Initialize writes for different files
    await storage.UploadBytes("delta_file1.pb", data);
    await storage.UploadBytes("delta_file2.pb", data);
    await storage.UploadBytes("versions_file1.pb", data);

    var tasks = new List<Task>();

    // Send 50 rapid writes across 5 different files
    for (int i = 0; i < 10; i++)
    {
        for (int fileNum = 1; fileNum <= 5; fileNum++)
        {
            var objectKey = $"file{fileNum}/delta.pb";
            var writeData = Encoding.UTF8.GetBytes($"update-{i}-{fileNum}");
            
            // These will queue for files where last write is recent
            tasks.Add(storage.UploadBytes(objectKey, writeData));
        }
    }

    var sw = Stopwatch.StartNew();
    await Task.WhenAll(tasks);
    sw.Stop();

    // All 50 calls return quickly (< 100ms)
    Assert.True(sw.ElapsedMilliseconds < 100);

    // Later: verify all files eventually wrote
    await Task.Delay(2500); // Wait for all flushes
    
    var file1Content = await storage.DownloadBytes("file1/delta.pb");
    Assert.NotEmpty(file1Content); // All files were written
}
```

---

## Monitoring & Debugging

### Log Output Example

When queue operations occur, you'll see logs like:

```
[DEBUG] Write queued for delta.pb (#1234). Last write was 0.5s ago
[DEBUG] Flush started for delta.pb with 3 pending writes  
[DEBUG] Flushed queued write for delta.pb (#1234)
[DEBUG] Flushed queued write for delta.pb (#1235)
[DEBUG] Flushed queued write for delta.pb (#1236)
[INFO]  Waiting for 1 in-flight flush(es) before shutdown
[INFO]  All queued writes flushed, Google Cloud client disposing
```

### Observability Recommendations

Track these metrics in your monitoring system:

1. **Queue depth**: Count of pending writes across all files
2. **Flush latency**: Time from queue entry to GCS write completion
3. **Queue hit rate**: Percentage of writes that hit the queue vs. direct write
4. **Disposal duration**: Time for `DisposeAsync()` to complete (includes queue flush)

---

## Common Patterns

### Pattern 1: Bulk Write with Rate Limiting Automatic

```csharp
public async Task UpdateMultipleVersions(Dictionary<string, byte[]> updates)
{
    foreach (var (key, data) in updates)
    {
        // Each write either executes directly or gets queued
        // No manual rate limit logic needed!
        await storage.UploadBytes(key, data);
    }
}
```

### Pattern 2: Graceful Shutdown in ASP.NET Core

```csharp
services.AddSingleton<IHostedService>(sp =>
    new StorageShutdownService(storageProvider));

public class StorageShutdownService : IHostedService
{
    private readonly GoogleCloudStorage<T> _storage;

    public async Task StopAsync(CancellationToken stoppingToken)
    {
        // Gracefully flush all pending writes before app stops
        await _storage.DisposeAsync();
    }
}
```

### Pattern 3: Monitoring Queue Health

```csharp
public class StorageQueueMonitor
{
    private readonly GoogleCloudStorage<T> _storage;
    private readonly IMetricsCollector _metrics;

    public void ReportQueueStats()
    {
        // Access internal queue info for diagnostics
        var inFlightWrites = GetInFlightWriteCount();
        _metrics.Gauge("gcs.queue.depth", inFlightWrites);
        
        if (inFlightWrites > 100)
            _logger.LogWarning("Queue backlog detected");
    }
}
```

---

## Troubleshooting

### Issue: Writes seem to be taking longer than expected

**Possible cause**: Queue is filling up (maybe GCS is slow).

**Investigation**:
1. Check logs for "Write queued" entries - indicates queue is active
2. Check "Flush failed" warnings - indicates failures during flush
3. Monitor queue depth metrics - increasing queue = backlog

**Solution**: 
- Verify GCS permissions and network connectivity
- Check GCS quota/limits
- Consider spacing out writes more (or accept that they're naturally rate-limited)

### Issue: DisposeAsync() is taking 20+ seconds

**Possible cause**: Large queue of pending writes at shutdown.

**Investigation**:
1. Check logs during shutdown - "Waiting for X in-flight flushes"
2. Check if writes are continuously coming in during shutdown

**Solution**:
- Stop accepting new writes earlier during shutdown
- Implement graceful timeout (e.g., DisposeAsync(TimeSpan timeout))
- Verify application isn't creating new storage instances after shutdown begins

### Issue: Data appears to be lost

**This shouldn't happen** with proper disposal. If it does:

1. **Check**: Was `DisposeAsync()` called? (not just `using` without await)
2. **Check**: Did application crash before DisposeAsync completed?
3. **Check**: Are you checking GCS? (queue flushes may not be visible in logs immediately)

---

## Next Steps

For implementation details, see:
- [data-model.md](../data-model.md) - Queue structure and state management
- [api-contract.md](./api-contract.md) - Detailed API changes and guarantees
- [research.md](../research.md) - Design decisions and rationale
