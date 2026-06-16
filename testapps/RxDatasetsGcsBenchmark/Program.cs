using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.GenericNotifierLogic;
using RxDatasets.StorageProvider.GoogleCloud;
using RxDatasets.StorageProviders;

if (args.Length == 0 || args.Contains("--help"))
{
    Console.WriteLine("Usage: RxDatasetsGcsBenchmark --bucket <bucket> --project <project> [--count <1000>] [--parallelism <10>] [--prefix <benchmark>] [--disable-queue]");
    return;
}

var bucketName = GetArg(args, "--bucket");
var projectId = GetArg(args, "--project");
var count = int.Parse(GetArg(args, "--count") ?? "100");
var parallelism = int.Parse(GetArg(args, "--parallelism") ?? "10");
var prefix = GetArg(args, "--prefix") ?? "benchmark";
var disableQueue = args.Contains("--disable-queue");

if (string.IsNullOrEmpty(bucketName) || string.IsNullOrEmpty(projectId))
{
    Console.Error.WriteLine("Bucket and Project are required.");
    return;
}

Console.WriteLine($"Starting benchmark: Bucket={bucketName}, Project={projectId}, Count={count}, Parallelism={parallelism}, Prefix={prefix}, WriteQueue={!disableQueue}");

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (s, e) =>
{
    Console.WriteLine("Canceling...");
    cts.Cancel();
    e.Cancel = true;
};

try
{
    // Setup Logging
    using var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Information));

    // Setup Storage
    var gcsConfig = Options.Create(new GoogleCloudConfig { BucketName = bucketName, ProjectId = projectId, EnableWriteQueue = !disableQueue });
    var gcsLogger = loggerFactory.CreateLogger<GoogleCloudStorage<BenchmarkDto>>();
    await using var storage = new GoogleCloudStorage<BenchmarkDto>(gcsLogger, gcsConfig, retryInitialization: false);

    var storageConfig = Options.Create(new StorageConfig { CustomRootFolder = prefix });
    var readerLogger = loggerFactory.CreateLogger<ObjectStorageReader<BenchmarkDto>>();
    var reader = new ObjectStorageReader<BenchmarkDto>(readerLogger, storageConfig, storage);

    var providerLogger = loggerFactory.CreateLogger<ObjectStorageProvider<BenchmarkDto>>();
    var notificationsHub = new NotificationsHubSync<BenchmarkDto>();
    var provider = new ObjectStorageProvider<BenchmarkDto>(providerLogger, storageConfig, storage, notificationsHub, reader, startIfInitFails: false);

    // Setup Writer
    var writerConfig = Options.Create(new WriterConfig());
    var writerLogger = loggerFactory.CreateLogger<DatasetWriter<BenchmarkDto>>();
    using var writer = new DatasetWriter<BenchmarkDto>(notificationsHub, writerLogger, provider, writerConfig, retryInitialization: false);

    Console.WriteLine("Initializing storage...");
    await provider.StartAsync(cts.Token);
    Console.WriteLine("Storage initialized.");

    Console.WriteLine("Initializing writer...");
    if (writer.InitializationTask != null)
    {
        await writer.InitializationTask.WaitAsync(cts.Token);
    }
    else
    {
        Console.WriteLine("Warning: Writer InitializationTask is null. Notification might have been missed or not sent.");
    }
    Console.WriteLine("Writer initialized.");

    // Generate Data
    var data = Enumerable.Range(0, count).Select(i => new BenchmarkDto($"Item-{i}", i)).ToList();

    Console.WriteLine($"Writing {count} items with parallelism {parallelism}...");

    var sw = Stopwatch.StartNew();
    long effectiveWrites = 0;

    var options = new ParallelOptions { MaxDegreeOfParallelism = parallelism, CancellationToken = cts.Token };
    var exceptions = new ConcurrentQueue<Exception>();

    try
    {
        await Parallel.ForEachAsync(data, options, async (item, ct) =>
        {
            try
            {
                var bytes = System.Text.Encoding.UTF8.GetBytes(item.ToString());
                var response = await writer.Put(item.Name, bytes, null, null, ct: ct);
                if (response.NumAddedEntities > 0 || response.NumUpdatedEntities > 0)
                {
                    Interlocked.Increment(ref effectiveWrites);
                }
            }
            catch (Exception ex)
            {
                exceptions.Enqueue(ex);
                Console.Error.WriteLine($"Error writing {item.Name}: {ex.Message}");
            }
        });
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine("Operation canceled.");
    }

    sw.Stop();

    Console.WriteLine($"Finished in {sw.Elapsed.TotalSeconds:F2}s");
    if (sw.Elapsed.TotalSeconds > 0)
    {
        Console.WriteLine($"Throughput (Total): {count / sw.Elapsed.TotalSeconds:F2} ops/sec");
        Console.WriteLine($"Throughput (Effective): {effectiveWrites / sw.Elapsed.TotalSeconds:F2} writes/sec");
        Console.WriteLine($"Effective Writes: {effectiveWrites}/{count}");
    }

    if (!exceptions.IsEmpty)
    {
        Console.WriteLine($"Errors encountered: {exceptions.Count}");
    }
    
    await provider.StopAsync(CancellationToken.None);
}
catch (Exception ex)
{
    Console.Error.WriteLine($"Fatal error: {ex.Message}");
}

// Cleanup (optional, maybe add a flag to delete?)
// await storage.DeleteFolder(prefix);

static string? GetArg(string[] args, string name)
{
    var idx = Array.IndexOf(args, name);
    if (idx >= 0 && idx < args.Length - 1)
    {
        return args[idx + 1];
    }
    return null;
}

public record BenchmarkDto(string Name, int Value);

// Simple sync hub for standalone usage
public class NotificationsHubSync<T> : INotificationsHub<T>
{
    private readonly List<object> _subscribers = new();

    public async Task Notify<TNotification>(TNotification notification) where TNotification : INotification
    {
        var handlers = _subscribers.OfType<Func<TNotification, Task>>().ToList();
        foreach (var handler in handlers)
        {
            await handler(notification);
        }
    }

    public IDisposable Subscribe<TNotification>(Func<TNotification, Task> handler) where TNotification : INotification
    {
        _subscribers.Add(handler);
        return new DisposableAction(() => _subscribers.Remove(handler));
    }

    private class DisposableAction : IDisposable { private readonly Action _action; public DisposableAction(Action action) => _action = action; public void Dispose() => _action(); }
}
