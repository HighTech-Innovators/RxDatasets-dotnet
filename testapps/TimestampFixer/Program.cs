using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

class Program
{
    static void Main(string[] args)
    {
        if (args.Length == 0)
        {
            Console.WriteLine("Usage: TimestampFixer <directory>");
            return;
        }

        string rootDir = args[0];
        if (!Directory.Exists(rootDir))
        {
            Console.WriteLine($"Directory not found: {rootDir}");
            return;
        }

        ProcessDirectory(rootDir);
    }

    static void ProcessDirectory(string dir)
    {
        // Check if this is a 'deltas' directory
        if (Path.GetFileName(dir).Equals("deltas", StringComparison.OrdinalIgnoreCase))
        {
            ProcessDeltasDirectory(dir);
        }

        // Recurse
        try
        {
            foreach (var subDir in Directory.GetDirectories(dir))
            {
                ProcessDirectory(subDir);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error accessing {dir}: {ex.Message}");
        }
    }

    static void ProcessDeltasDirectory(string deltasDir)
    {
        Console.WriteLine($"Processing deltas in: {deltasDir}");
        
        // Find the parent directory to look for snapshots
        var parentDir = Directory.GetParent(deltasDir)?.FullName;
        
        // Track versions per entity across all deltas
        var entityVersions = new Dictionary<string, ulong>();
        
        // Load initial versions from snapshot if available
        if (parentDir != null)
        {
            var snapshotsDir = Path.Combine(parentDir, "snapshots");
            if (Directory.Exists(snapshotsDir))
            {
                LoadVersionsFromSnapshot(snapshotsDir, deltasDir, entityVersions);
            }
        }

        try
        {
            // Get all version directories and sort them numerically
            var versionDirs = Directory.GetDirectories(deltasDir)
                .Select(dir => new
                {
                    Path = dir,
                    Version = ParseDeltaVersion(Path.GetFileName(dir))
                })
                .Where(x => x.Version.HasValue)
                .OrderBy(x => x.Version.Value)
                .ToList();

            foreach (var versionDir in versionDirs)
            {
                var indexFile = Path.Combine(versionDir.Path, "index.pb");
                if (File.Exists(indexFile))
                {
                    ProcessIndexFile(indexFile, entityVersions);
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error accessing {deltasDir}: {ex.Message}");
        }
    }

    static void LoadVersionsFromSnapshot(string snapshotsDir, string deltasDir, Dictionary<string, ulong> entityVersions)
    {
        try
        {
            // Get all delta versions to find the oldest
            var deltaVersions = Directory.GetDirectories(deltasDir)
                .Select(dir => ParseDeltaVersion(Path.GetFileName(dir)))
                .Where(v => v.HasValue)
                .Select(v => v.Value)
                .OrderBy(v => v)
                .ToList();

            if (!deltaVersions.Any())
            {
                return;
            }

            ulong oldestDelta = deltaVersions.First();
            
            // Find the snapshot just before the oldest delta
            var snapshotDirs = Directory.GetDirectories(snapshotsDir)
                .Select(dir => new
                {
                    Path = dir,
                    Version = ParseDeltaVersion(Path.GetFileName(dir))
                })
                .Where(x => x.Version.HasValue && x.Version.Value < oldestDelta)
                .OrderByDescending(x => x.Version.Value)
                .ToList();

            if (!snapshotDirs.Any())
            {
                return;
            }

            var snapshotIndexFile = Path.Combine(snapshotDirs.First().Path, "index.pb");
            if (!File.Exists(snapshotIndexFile))
            {
                return;
            }

            Console.WriteLine($"Loading versions from snapshot: {snapshotIndexFile}");
            var bytes = File.ReadAllBytes(snapshotIndexFile);
            var snapshotIndex = SnapshotIndex.Parser.ParseFrom(bytes);

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
            
            Console.WriteLine($"Loaded {entityVersions.Count} entity versions from snapshot");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error loading snapshot versions: {ex.Message}");
        }
    }

    static ulong? ParseDeltaVersion(string dirName)
    {
        if (ulong.TryParse(dirName, out var version))
        {
            return version;
        }
        return null;
    }

    static void ProcessIndexFile(string filePath, Dictionary<string, ulong> entityVersions)
    {
        try
        {
            var bytes = File.ReadAllBytes(filePath);
            var deltaIndex = DeltaIndex.Parser.ParseFrom(bytes);
            bool changed = false;

            // Get the current delta version
            ulong currentVersion = deltaIndex.DeltaVersion != 0 ? deltaIndex.DeltaVersion : deltaIndex.DeltaVersionLegacy;
            
            // Fix timestamp if needed
            if (deltaIndex.Timestamp == null || (deltaIndex.Timestamp.Seconds == 0 && deltaIndex.Timestamp.Nanos == 0))
            {
                var fileInfo = new FileInfo(filePath);
                var lastWriteTime = fileInfo.LastWriteTimeUtc;
                deltaIndex.Timestamp = Timestamp.FromDateTime(lastWriteTime);
                changed = true;
                Console.WriteLine($"Updated timestamp for {filePath} to {lastWriteTime}");
            }

            // Process changed entities
            foreach (var entity in deltaIndex.ChangedEntities)
            {
                if (entity.PreviousVersion == 0 && entityVersions.TryGetValue(entity.Name, out var lastVersion))
                {
                    entity.PreviousVersion = lastVersion;
                    changed = true;
                    Console.WriteLine($"  Set previous_version for changed entity '{entity.Name}' to {lastVersion}");
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
                    Console.WriteLine($"  Set previous_version for new entity '{entity.Name}' to {lastVersion}");
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
                    Console.WriteLine($"  Set previous_version for deleted entity '{entity.Name}' to {lastVersion}");
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
                            Console.WriteLine($"  Migrating deleted_names to deleted_entities");
                            hasMigrations = true;
                        }
                        var deletedEntity = new DeletedEntity
                        {
                            Name = deletedName,
                            PreviousVersion = entityVersions.TryGetValue(deletedName, out var lastVersion) ? lastVersion : 0
                        };
                        deltaIndex.DeletedEntities.Add(deletedEntity);
                        changed = true;
                        Console.WriteLine($"    Migrated deleted entity '{deletedName}' with previous_version {deletedEntity.PreviousVersion}");
                    }
                }
                // Keep deleted_names for backward compatibility (don't clear it)
                
                // Update tracking for all deleted names
                foreach (var deletedName in deltaIndex.DeletedNames)
                {
                    entityVersions[deletedName] = currentVersion;
                }
            }

            if (changed)
            {
                using (var output = File.OpenWrite(filePath))
                {
                    output.SetLength(0);
                    deltaIndex.WriteTo(output);
                }
                Console.WriteLine($"Saved changes to {filePath}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error processing {filePath}: {ex.Message}");
        }
    }
}
