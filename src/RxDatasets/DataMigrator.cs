using System.Text;
using System.Text.RegularExpressions;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RxDatasets.Abstractions;
using RxDatasets.Configs;
using RxDatasets.Models;
using RxDatasets.StorageProviders;

namespace RxDatasets;

public class DataMigrator<T> : IDataMigrator
{
    private readonly IDiskStorage<T> _storage;
    private readonly ILogger<DataMigrator<T>> _logger;
    private readonly IOptions<StorageConfig> _config;

    public DataMigrator(IDiskStorage<T> storage, ILogger<DataMigrator<T>> logger, IOptions<StorageConfig> config)
    {
        _storage = storage;
        _logger = logger;
        _config = config;
    }

    public async Task<string> MigrateDataset(string migrationName)
    {
        switch (migrationName.ToLowerInvariant())
        {
            case "historysupport":
                return await RunHistorySupportMigration();
            case "hierarchicalindex":
                return await RunHierarchicalIndexMigration();
            default:
                _logger.LogWarning($"Unknown migration: {migrationName}");
                return $"## {typeof(T).Name}\n\nUnknown migration: {migrationName}";
        }
    }

    private async Task<string> RunHistorySupportMigration()
    {
        var sb = new StringBuilder();
        sb.AppendLine($"## {typeof(T).Name}");
        sb.AppendLine();

        void Log(string message)
        {
            _logger.LogInformation(message);
            sb.AppendLine(message);
        }

        Log("Starting HistorySupport migration");

        var prefix = DatasetHelper.GetObjectNamePrefix<T>(_config.Value);
        var deltasPrefix = prefix + FolderNames.DeltasFolderNameWithSlash;
        var snapshotsPrefix = prefix + FolderNames.SnapshotsFolderNameWithSlash;

        // Track versions per entity across all deltas
        var entityVersions = new Dictionary<string, ulong>();

        // Load initial versions from snapshot if available
        await LoadVersionsFromSnapshot(snapshotsPrefix, deltasPrefix, entityVersions, Log);

        try
        {
            // Get all delta index files
            var deltaFiles = await _storage.ListObjects(deltasPrefix);
            var versionFiles = deltaFiles
                .Where(f => f.ObjectKey.EndsWith("/index.pb", StringComparison.Ordinal))
                .Select(f => new
                {
                    Key = f.ObjectKey,
                    Version = ParseDeltaVersionFromKey(f.ObjectKey),
                    Timestamp = f.DateTime
                })
                .Where(x => x.Version.HasValue)
                .OrderBy(x => x.Version!.Value)
                .ToList();

            foreach (var file in versionFiles)
            {
                await ProcessIndexFile(file.Key, file.Timestamp, entityVersions, Log);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error accessing deltas");
            sb.AppendLine($"Error accessing deltas: {ex.Message}");
        }

        return sb.ToString();
    }

    private async Task LoadVersionsFromSnapshot(string snapshotsPrefix, string deltasPrefix, Dictionary<string, ulong> entityVersions, Action<string> log)
    {
        try
        {
            // Get all delta versions to find the oldest
            var deltaFiles = await _storage.ListObjects(deltasPrefix);
            var deltaVersions = deltaFiles
                .Where(f => f.ObjectKey.EndsWith("/index.pb", StringComparison.Ordinal))
                .Select(f => ParseDeltaVersionFromKey(f.ObjectKey))
                .Where(v => v.HasValue)
                .Select(v => v!.Value)
                .OrderBy(v => v)
                .ToList();

            if (!deltaVersions.Any())
            {
                return;
            }

            ulong oldestDelta = deltaVersions.First();

            // Find the snapshot just before the oldest delta
            var snapshotFiles = await _storage.ListObjects(snapshotsPrefix);
            var snapshotFile = snapshotFiles
                .Where(f => f.ObjectKey.EndsWith("/index.pb", StringComparison.Ordinal))
                .Select(f => new
                {
                    Key = f.ObjectKey,
                    Version = ParseDeltaVersionFromKey(f.ObjectKey) // Snapshot version parsing is similar
                })
                .Where(x => x.Version.HasValue && x.Version.Value < oldestDelta)
                .OrderByDescending(x => x.Version!.Value)
                .FirstOrDefault();

            if (snapshotFile == null)
            {
                return;
            }

            log($"Loading versions from snapshot: {snapshotFile.Key}");
            var bytes = await _storage.DownloadBytes(snapshotFile.Key);
            var snapshotIndex = SnapshotIndex.Parser.ParseFrom(bytes.ToArray());

            foreach (var kvp in snapshotIndex.Entities)
            {
                string entityName = kvp.Key;
                var entity = kvp.Value;

                // Calculate max version from all version fields
                ulong maxVersion = entity.LastUpdatedVersion;

                foreach (var deletedVer in entity.LastDeletedVersion)
                {
                    if (deletedVer > maxVersion)
                        maxVersion = deletedVer;
                }

                foreach (var insertedVer in entity.LastInsertedVersion)
                {
                    if (insertedVer > maxVersion)
                        maxVersion = insertedVer;
                }

                if (maxVersion > 0)
                {
                    entityVersions[entityName] = maxVersion;
                }
            }

            log($"Loaded {entityVersions.Count} entity versions from snapshot");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading snapshot versions");
            log($"Error loading snapshot versions: {ex.Message}");
        }
    }

    private ulong? ParseDeltaVersionFromKey(string key)
    {
        // Key format: prefix/deltas/VERSION/index.pb
        var parts = key.Split('/');
        if (parts.Length >= 2)
        {
            // The version should be the second to last part (before index.pb)
            if (ulong.TryParse(parts[parts.Length - 2], out var version))
            {
                return version;
            }
        }
        return null;
    }

    private async Task ProcessIndexFile(string key, DateTimeOffset fileTimestamp, Dictionary<string, ulong> entityVersions, Action<string> log)
    {
        try
        {
            var bytes = await _storage.DownloadBytes(key);
            var deltaIndex = DeltaIndex.Parser.ParseFrom(bytes.ToArray());
            bool changed = false;

            // Get the current delta version
            ulong currentVersion = deltaIndex.DeltaVersion != 0 ? deltaIndex.DeltaVersion : deltaIndex.DeltaVersionLegacy;

            // Fix timestamp if needed
            if (deltaIndex.Timestamp == null || (deltaIndex.Timestamp.Seconds == 0 && deltaIndex.Timestamp.Nanos == 0))
            {
                deltaIndex.Timestamp = Timestamp.FromDateTime(fileTimestamp.UtcDateTime);
                changed = true;
                log($"Updated timestamp for {key} to {fileTimestamp}");
            }

            // Process changed entities
            foreach (var entity in deltaIndex.ChangedEntities)
            {
                if (entity.PreviousVersion == 0 && entityVersions.TryGetValue(entity.Name, out var lastVersion))
                {
                    entity.PreviousVersion = lastVersion;
                    changed = true;
                    log($"  Set previous_version for changed entity '{entity.Name}' to {lastVersion}");
                }
            }

            // Update tracking for changed entities
            foreach (var entity in deltaIndex.ChangedEntities)
            {
                entityVersions[entity.Name] = currentVersion;
            }

            // Process new entities
            foreach (var entity in deltaIndex.NewEntities)
            {
                if (entity.PreviousVersion == 0 && entityVersions.TryGetValue(entity.Name, out var lastVersion))
                {
                    entity.PreviousVersion = lastVersion;
                    changed = true;
                    log($"  Set previous_version for new entity '{entity.Name}' to {lastVersion}");
                }
            }

            // Update tracking for new entities
            foreach (var entity in deltaIndex.NewEntities)
            {
                entityVersions[entity.Name] = currentVersion;
            }

            // Process deleted entities (new format)
            foreach (var entity in deltaIndex.DeletedEntities)
            {
                if (entity.PreviousVersion == 0 && entityVersions.TryGetValue(entity.Name, out var lastVersion))
                {
                    entity.PreviousVersion = lastVersion;
                    changed = true;
                    log($"  Set previous_version for deleted entity '{entity.Name}' to {lastVersion}");
                }
            }

            // Update tracking for deleted entities
            foreach (var entity in deltaIndex.DeletedEntities)
            {
                entityVersions[entity.Name] = currentVersion;
            }

            // Migrate deleted_names to deleted_entities (backward compatible)
            if (deltaIndex.DeletedNames.Count > 0)
            {
                bool hasMigrations = false;
                foreach (var deletedName in deltaIndex.DeletedNames)
                {
                    // Check if already in deleted_entities
                    if (!deltaIndex.DeletedEntities.Any(e => e.Name == deletedName))
                    {
                        if (!hasMigrations)
                        {
                            log($"  Migrating deleted_names to deleted_entities");
                            hasMigrations = true;
                        }
                        var deletedEntity = new DeletedEntity
                        {
                            Name = deletedName,
                            PreviousVersion = entityVersions.TryGetValue(deletedName, out var lastVersion) ? lastVersion : 0
                        };
                        deltaIndex.DeletedEntities.Add(deletedEntity);
                        changed = true;
                        log($"    Migrated deleted entity '{deletedName}' with previous_version {deletedEntity.PreviousVersion}");
                    }
                }

                // Update tracking for all deleted names
                foreach (var deletedName in deltaIndex.DeletedNames)
                {
                    entityVersions[deletedName] = currentVersion;
                }
            }

            if (changed)
            {
                await _storage.UploadBytes(key, deltaIndex.ToByteArray());
                log($"Saved changes to {key}");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error processing {key}");
            log($"Error processing {key}: {ex.Message}");
        }
    }

    private async Task<string> RunHierarchicalIndexMigration()
    {
        var sb = new StringBuilder();
        sb.AppendLine($"## {typeof(T).Name}");
        sb.AppendLine();

        void Log(string message)
        {
            _logger.LogInformation(message);
            sb.AppendLine(message);
        }

        Log("Starting hierarchical index migration...");
        Log("This migration converts flat delta files to hierarchical structure.");

        var prefix = DatasetHelper.GetObjectNamePrefix<T>(_config.Value);
        var deltasPrefix = prefix + FolderNames.DeltasFolderNameWithSlash;

        try
        {
            // Get all delta index files
            var deltaFiles = await _storage.ListObjects(deltasPrefix);
            var versionFiles = deltaFiles
                .Where(f => f.ObjectKey.EndsWith("/index.pb", StringComparison.Ordinal))
                .Select(f => new
                {
                    Key = f.ObjectKey,
                    Version = ParseDeltaVersionFromKey(f.ObjectKey),
                    Timestamp = f.DateTime
                })
                .Where(x => x.Version.HasValue)
                .OrderBy(x => x.Version!.Value)
                .ToList();

            Log($"Found {versionFiles.Count} delta files to migrate");

            int migratedCount = 0;
            int skippedCount = 0;

            foreach (var file in versionFiles)
            {
                bool wasMigrated = await MigrateHierarchicalDeltaFile(file.Key, Log);
                if (wasMigrated)
                    migratedCount++;
                else
                    skippedCount++;
            }

            Log($"Migration complete: {migratedCount} files converted, {skippedCount} files already hierarchical");
            return sb.ToString();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Hierarchical index migration failed");
            Log($"Migration failed: {ex.Message}");
            return sb.ToString();
        }
    }

    private async Task<bool> MigrateHierarchicalDeltaFile(string deltaFilePath, Action<string> log)
    {
        try
        {
            var bytes = await _storage.DownloadBytes(deltaFilePath);
            var deltaIndex = DeltaIndex.Parser.ParseFrom(bytes.ToArray());

            // Check if already migrated to hierarchical format
            if (deltaIndex.Root != null && (deltaIndex.Root.DeletedNames.Count > 0 || 
                                             deltaIndex.Root.ChangedEntities.Count > 0 || 
                                             deltaIndex.Root.NewEntities.Count > 0 || 
                                             deltaIndex.Root.Children.Count > 0))
            {
                log($"  {deltaFilePath}: Already hierarchical, skipping");
                return false;
            }

            // If flat entities exist, convert to hierarchical
            if (deltaIndex.DeletedNames.Count > 0 || deltaIndex.ChangedEntities.Count > 0 || deltaIndex.NewEntities.Count > 0)
            {
                log($"  {deltaFilePath}: Converting to hierarchical format");

                // Create hierarchical root node
                var rootNode = new DeltaNode();

                // Distribute deleted names hierarchically
                foreach (var deletedName in deltaIndex.DeletedNames)
                {
                    var parts = deletedName.Split('/', StringSplitOptions.RemoveEmptyEntries);
                    DistributeDeletedNameToHierarchy(rootNode, parts, deletedName);
                }

                // Distribute changed entities hierarchically
                foreach (var changedEntity in deltaIndex.ChangedEntities)
                {
                    var parts = changedEntity.Name.Split('/', StringSplitOptions.RemoveEmptyEntries);
                    var leafName = parts[parts.Length - 1];
                    
                    // Create a copy of the entity with only the leaf name
                    var entityForHierarchy = changedEntity.Clone();
                    entityForHierarchy.Name = leafName;
                    
                    DistributeEntityToHierarchy(rootNode, parts, entityForHierarchy);
                }

                // Distribute new entities hierarchically
                foreach (var newEntity in deltaIndex.NewEntities)
                {
                    var parts = newEntity.Name.Split('/', StringSplitOptions.RemoveEmptyEntries);
                    var leafName = parts[parts.Length - 1];
                    
                    // Create a copy of the entity with only the leaf name
                    var entityForHierarchy = newEntity.Clone();
                    entityForHierarchy.Name = leafName;
                    
                    DistributeEntityToHierarchy(rootNode, parts, entityForHierarchy);
                }

                // Set the root node and clear flat entities
                deltaIndex.Root = rootNode;
                deltaIndex.DeletedNames.Clear();
                deltaIndex.ChangedEntities.Clear();
                deltaIndex.NewEntities.Clear();

                // Save the migrated delta file
                await _storage.UploadBytes(deltaFilePath, deltaIndex.ToByteArray());
                log($"    Successfully migrated {deltaFilePath}");
                return true;
            }

            log($"  {deltaFilePath}: No flat entities to migrate, skipping");
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"Error migrating hierarchical delta file {deltaFilePath}");
            log($"  Error migrating {deltaFilePath}: {ex.Message}");
            return false;
        }
    }

    private void DistributeEntityToHierarchy(DeltaNode node, string[] pathParts, DeltaEntity entity)
    {
        if (pathParts.Length == 0)
            return;

        // Navigate/create path to leaf
        var current = node;
        for (int i = 0; i < pathParts.Length - 1; i++)
        {
            var part = pathParts[i];
            if (!current.Children.ContainsKey(part))
            {
                current.Children[part] = new DeltaNode();
            }
            current = current.Children[part];
        }

        // Add entity to the leaf node
        var leafName = pathParts[pathParts.Length - 1];
        current.ChangedEntities[leafName] = entity;
    }

    private void DistributeDeletedNameToHierarchy(DeltaNode node, string[] pathParts, string deletedName)
    {
        if (pathParts.Length == 0)
            return;

        // Navigate/create path to leaf
        var current = node;
        for (int i = 0; i < pathParts.Length - 1; i++)
        {
            var part = pathParts[i];
            if (!current.Children.ContainsKey(part))
            {
                current.Children[part] = new DeltaNode();
            }
            current = current.Children[part];
        }

        // Add deleted name to the leaf node
        var leafName = pathParts[pathParts.Length - 1];
        current.DeletedNames.Add(leafName);
    }
}
