# ProtoScope - Protobuf Inspector and YAML Converter

A CLI tool to inspect RxDatasets protobuf files and convert them to human-readable YAML format.

## Features

- **Auto-detection**: Automatically detects proto file type from filename or by attempting to parse
- **Multiple formats**: Supports all RxDatasets proto types:
  - `SnapshotIndex` (-s, --snapshot)
  - `DeltaIndex` (-d, --delta)
  - `SnapshotInfo` (-S, --snapshotinfo)
  - `DeltaInfo` (-D, --deltainfo)
  - `ProcessedVersionsInfo` (-P, --processedinfo)
- **YAML output**: Pretty-printed YAML with camelCase naming
- **Verbose mode**: Shows file metadata and detection results

## Usage

### Basic Usage (Auto-detect)
```bash
dotnet protoscope.dll path/to/snapshot/0/index.pb
dotnet protoscope.dll path/to/deltas/1/index.pb
```

### Override File Type
```bash
# Explicit snapshot
dotnet protoscope.dll some_file.pb -s

# Explicit delta
dotnet protoscope.dll some_file.pb -d

# Explicit info files
dotnet protoscope.dll snapshot_info.pb -S
dotnet protoscope.dll delta_info.pb -D
dotnet protoscope.dll processed_versions.pb -P
```

### Verbose Output
```bash
dotnet protoscope.dll path/to/file.pb -v
# Shows: file name, detected type, file size, plus YAML output
```

### Help
```bash
dotnet protoscope.dll --help
dotnet protoscope.dll -h
```

## Examples

### Inspect a Snapshot
```bash
dotnet protoscope.dll snapshot/0/index.pb
```

Output:
```yaml
snapshotVersion: 1
entities:
  entity1:
    lastUpdatedVersion: 1
    position: 0
    length: 100
    crc32: 12345
  company/dept/employee:
    lastUpdatedVersion: 1
    position: 100
    length: 50
    crc32: 54321
root:
  entities: {}
  children:
    company:
      entities: {}
      children:
        dept:
          entities:
            employee:
              lastUpdatedVersion: 1
              position: 100
              length: 50
              crc32: 54321
          children: {}
```

### Inspect a Delta
```bash
dotnet protoscope.dll deltas/1/index.pb -d
```

### Check Delta Info
```bash
dotnet protoscope.dll deltas/info.pb -D
```

Output:
```yaml
earliestDeltaVersion: 1
latestDeltaVersion: 10
```

## Auto-Detection Strategy

ProtoScope uses the following strategy to detect file types:

1. **Explicit flag**: Use -s, -d, -S, -D, or -P if provided
2. **Filename pattern**:
   - Contains "snapshot" → SnapshotIndex (or SnapshotInfo if ends with info.pb)
   - Contains "delta" → DeltaIndex (or DeltaInfo if ends with info.pb)
   - Contains "processed" or "versions" → ProcessedVersionsInfo
3. **Try parsing**: Attempts to parse with each proto type in order:
   - SnapshotIndex
   - DeltaIndex
   - SnapshotInfo
   - DeltaInfo
   - ProcessedVersionsInfo

## Use Cases

- **Debugging**: Inspect actual snapshot and delta files to verify hierarchical index structure
- **Validation**: Check file contents before/after migration
- **Exploration**: Understand data structure in human-readable format
- **Testing**: Verify proto serialization/deserialization works correctly

## Dependencies

- YamlDotNet: For YAML serialization
- RxDatasets: For proto message definitions
- Google.Protobuf: For protobuf parsing
