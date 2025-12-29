# Quickstart: Hierarchical Index

## Overview
The hierarchical index optimizes memory usage for datasets with path-like IDs (e.g., `a/b/c`). It is enabled by default and handles legacy data transparently.

## Usage

### Reading Data
No changes are required for consumers. `DatasetReader` automatically detects the index format.

```csharp
var reader = new DatasetReader(storage, config);
await reader.Initialize();
// Access data as usual
```

### Writing Data
`DatasetWriter` will automatically persist the index in the new hierarchical format.

### Migration (Optional)
To explicitly migrate legacy delta files to the new format (snapshots are migrated automatically on next write):

```csharp
var migrator = new DataMigrator(storage, logger, config);
await migrator.MigrateDataset("my-dataset");
```

## Verification
To verify the memory reduction:
1. Load a large dataset with path-like IDs.
2. Check the process memory usage.
3. Compare with the previous version.

## Configuration
The separator character defaults to `/`. Currently, this is not configurable but may be exposed in `StorageConfig` in the future.
