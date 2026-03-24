using Google.Cloud.Storage.V1;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.StorageProvider.GoogleCloud;
using RxDatasets.StorageProviders;
using System.Text.RegularExpressions;

if (args.Length < 4)
{
    Console.WriteLine("Usage: GcsToBigtableMigrator <BucketName> <ProjectId> <InstanceId> <TableId> [PrefixFilter]");
    return;
}

var bucketName = args[0];
var projectId = args[1];
var instanceId = args[2];
var tableId = args[3];
var prefixFilter = args.Length > 4 ? args[4] : "";

Console.WriteLine($"Migrating from Bucket: {bucketName} to Bigtable {instanceId}/{tableId} (Project: {projectId})");
if (!string.IsNullOrEmpty(prefixFilter))
{
    Console.WriteLine($"Filter: {prefixFilter}");
}

using var loggerFactory = LoggerFactory.Create(builder =>
{
    builder.AddConsole();
    builder.SetMinimumLevel(LogLevel.Information);
});
var logger = loggerFactory.CreateLogger<Program>();

// Setup GCS Client (Source)
var storageClient = StorageClient.Create();

// Setup Bigtable Client (Destination)
var config = Options.Create(new GoogleCloudConfig
{
    BigtableProjectId = projectId,
    BigtableInstanceId = instanceId,
    BigtableTableId = tableId,
    BucketName = bucketName
});

var rowStore = new GoogleCloudVersionedArtifactRowStore(config);

Console.WriteLine("Scanning GCS bucket...");

// We need to find all unique dataset prefixes.
// A dataset prefix is the path up to "deltas/" or "snapshots/".
var datasets = new HashSet<string>();
var options = new ListObjectsOptions { };

foreach (var obj in storageClient.ListObjects(bucketName, prefixFilter, options))
{
    var name = obj.Name;
    if (name.Contains("/deltas/"))
    {
        var datasetPrefix = name.Substring(0, name.IndexOf("/deltas/", StringComparison.Ordinal)); 
        if (string.IsNullOrEmpty(datasetPrefix)) continue;
        datasets.Add(datasetPrefix);
    }
    else if (name.Contains("/snapshots/"))
    {
        var datasetPrefix = name.Substring(0, name.IndexOf("/snapshots/", StringComparison.Ordinal));
        if (string.IsNullOrEmpty(datasetPrefix)) continue;
        datasets.Add(datasetPrefix);
    }
}

Console.WriteLine($"Found {datasets.Count} datasets.");

foreach (var datasetPrefix in datasets)
{
    // Ensure trailing slash for consistent handling
    var normalizedPrefix = datasetPrefix.EndsWith("/", StringComparison.Ordinal) ? datasetPrefix : datasetPrefix + "/";
    Console.WriteLine($"Processing dataset: {normalizedPrefix}");
    try
    {
        await MigrateDataset(normalizedPrefix, storageClient, rowStore, bucketName);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error migrating dataset {normalizedPrefix}: {ex.Message}");
    }
}

Console.WriteLine("Migration complete.");

static async Task MigrateDataset(string datasetPrefix, StorageClient storageClient, GoogleCloudVersionedArtifactRowStore rowStore, string bucketName)
{
    // 1. List Deltas
    var deltasPrefix = datasetPrefix + "deltas/";
    var deltas = storageClient.ListObjects(bucketName, deltasPrefix);
    
    // Key: version, Value: (IndexObject, DataObject)
    var deltaVersions = new Dictionary<ulong, (Google.Apis.Storage.v1.Data.Object? Index, Google.Apis.Storage.v1.Data.Object? Data)>();
    
    foreach (var obj in deltas)
    {
        if (obj.Name.EndsWith("/", StringComparison.Ordinal)) continue; // skip folders

        // Format: .../deltas/{version}/index.pb or data.pbs
        var parts = obj.Name.Split('/');
        if (parts.Length < 2) continue;
        
        var fileName = parts.Last();
        var versionStr = parts[parts.Length - 2];
        
        if (ulong.TryParse(versionStr, out var version))
        {
            if (!deltaVersions.ContainsKey(version))
                deltaVersions[version] = (null, null);

            var current = deltaVersions[version];
            if (fileName == "index.pb")
                current.Index = obj;
            else if (fileName == "data.pbs")
                current.Data = obj;
            
            deltaVersions[version] = current;
        }
    }

    Console.WriteLine($"  Found {deltaVersions.Count} deltas.");

    foreach (var kvp in deltaVersions)
    {
        var version = kvp.Key;
        var (indexObj, dataObj) = kvp.Value;

        if (indexObj == null || dataObj == null)
        {
            Console.WriteLine($"  [Warn] Delta {version} is incomplete (missing index or data). Skipping.");
            continue;
        }

        var record = new HybridArtifactRecord(
            ObjectNameTypeEnum.Delta,
            version,
            (indexObj.TimeCreatedDateTimeOffset ?? indexObj.UpdatedDateTimeOffset) ?? DateTimeOffset.UtcNow,
            null, // InlineIndex
            null, // InlineData
            indexObj.Name,
            dataObj.Name,
            (long)(indexObj.Size ?? 0),
            (long)(dataObj.Size ?? 0)
        );

        await rowStore.UpsertArtifact(datasetPrefix, record);
    }

    // 2. List Snapshots
    var snapshotsPrefix = datasetPrefix + "snapshots/";
    var snapshots = storageClient.ListObjects(bucketName, snapshotsPrefix);
    
    var snapshotVersions = new Dictionary<ulong, (Google.Apis.Storage.v1.Data.Object? Index, Google.Apis.Storage.v1.Data.Object? Data)>();

    foreach (var obj in snapshots)
    {
        if (obj.Name.EndsWith("/", StringComparison.Ordinal)) continue; 

        var parts = obj.Name.Split('/');
        if (parts.Length < 2) continue;

        var fileName = parts.Last();
        var versionStr = parts[parts.Length - 2];

        if (ulong.TryParse(versionStr, out var version))
        {
             if (!snapshotVersions.ContainsKey(version))
                snapshotVersions[version] = (null, null);

            var current = snapshotVersions[version];
            if (fileName == "index.pb")
                current.Index = obj;
            else if (fileName == "data.pbs")
                current.Data = obj;
            
            snapshotVersions[version] = current;
        }
    }

    Console.WriteLine($"  Found {snapshotVersions.Count} snapshots.");

    foreach (var kvp in snapshotVersions)
    {
         var version = kvp.Key;
        var (indexObj, dataObj) = kvp.Value;

        if (indexObj == null || dataObj == null)
        {
            Console.WriteLine($"  [Warn] Snapshot {version} is incomplete. Skipping.");
            continue;
        }

        var record = new HybridArtifactRecord(
            ObjectNameTypeEnum.Snapshot,
            version,
            (indexObj.TimeCreatedDateTimeOffset ?? indexObj.UpdatedDateTimeOffset) ?? DateTimeOffset.UtcNow,
            null,
            null,
            indexObj.Name,
            dataObj.Name,
             (long)(indexObj.Size ?? 0),
            (long)(dataObj.Size ?? 0)
        );

        await rowStore.UpsertArtifact(datasetPrefix, record);
    }
}
