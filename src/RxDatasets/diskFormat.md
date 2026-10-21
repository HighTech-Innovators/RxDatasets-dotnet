# Introduction
For the storage of DTOs, the Protobuf encoding is used. Protobuf is a binary file format that is used to serialize and deserialize objects. 
Three different object types are used:
- Deltas: for each update made to the dataset, a delta is created and stored, containing the changes made to the dataset.
- Snapshots: these files contain the full state of the data at a certain point in time.
- `DeltaInfo.pb` and `SnapshotInfo.pb`: this file contains metadata about the delta and snapshot files that are available.

In total, there are five different file types stored on disk:
- `deltas/{delta_version}/data.pbs`: Storing the actual (concatinated) data of the entities that have been changed or added.
- `deltas/{delta_version}/index.pb`: Storing the metadata of entities in the delta, and their position in the data file.
- `snapshot/{snapshot_version}/data.pbs`: Storing the actual (concatenated) data of the entities that are part of the snapshot.
- `snapshot/{snapshot_version}/index.pb`: Storing the metadata of entities in the snapshot, and their position in the data file.
- `DeltaInfo.pb`: Storing the metadata about the delta files that are available.
- `SnapshotInfo.pb`: Storing the metadata about the snapshot files that are available.
- `ProcessedVersions.pb`: Storing the metadata about the services that process delta notifications.

The full .proto file containing the object definitions can be found [here](src/RxDatasets/Models/DatasetEntities.proto)

## Delta's
After each update made to the dataset, a Delta is created and stored in the `/deltas/{delta_version}/` folder in the S3 bucket. A delta consists of two files:
- `deltas/{delta_version}/index.pb`
- `deltas/{delta_version}/data.pbs`

The `index.pb` file contains the metadata of the delta, such as the version number, the names of the entities that have been deleted, the entities that have been changed, and the entities that have been added. The `data.pbs` file contains the actual data of the entities that have been changed or added.

Note that for the deleted entities, only the names are stored in the `index.pb` file since the actual data of the deleted entities is not stored in the `data.pbs` file.

For the added and changed entities, the actual data is stored in the `data.pbs` file. This file is a concatination of **delimited** protobuf streams of the entities that have been added or changed. The structure of the actual entities depends on the DTO that is being stored.

The delimiters used in the data.pbs file denote the number of bytes that the next entity will take up. This allows for reading and parsing the `data.pbs` file even without reading the `index.pb` file first if this is desired.

The `index.pb` file contains the following fields:
- `delta_version`: the version number of the delta file.
- `deleted_entity_names`: a list of the names of the entities that have been deleted.
- `updated_entities`: a list of the entities that have been updated in this delta. Each entity is of the type DeltaEntity, a proto message we defined.
- `created_entities`: a list of the entities that have been created in this delta. Each entity is of the type DeltaEntity.

The `DeltaEntity` message contains the following fields:
- `name`: the name of the entity.
- `position_in_datafile`: the position of the entity in the `data.pbs` file. **This is the position of the actual entity data in the `data.pbs` file, not the delimiter.** The delimiter bytes are not required here since we already know the length since it is stored in the index file as `number_of_bytes`.
- `number_of_bytes`: the number of bytes of the entity in the `data.pbs` file starting at the position denoted in `position_in_datafile`.
- `crc32`: the crc32 hash of the entity data, this is used to determine if the entity has been changed.

```protobuf
message DeltaIndex {
      uint64 delta_version_legacy = 1;
      repeated string deleted_names = 2;
      repeated DeltaEntity changed_entities = 3;
      repeated DeltaEntity new_entities = 4;
      uint64 delta_version = 32;
}

message DeltaEntity {
	string name = 1;
	int64 position_in_datafile = 2;
	int32 number_of_bytes = 3;
	int32 crc32 = 5;
}
```

## Snapshots
Snapshots are created at regular intervals and contain the full state of the data at that point in time, aggregating all the deltas. 
In case an entity is deleted, it won't be part of the next snapshot anymore. 
Snapshots are stored in the `/snapshot/{snapshot_version}/` folder. 
A snapshot consists of two files:
- `snapshot/{snapshot_version}/index.pb`
- `snapshot/{snapshot_version}/data.pbs`

The index contains the metadata about the snapshot, such as the version number, the entities that have been deleted, created, and updated. 
The `data.pbs` file contains the actual data of the entities that are part of the snapshot. 
The `data.pbs` file is a concatenation of **delimited** protobuf streams of the entities that are part of the snapshot. 
The structure of the actual entities depends on the DTO that is being stored.

The `index.pb` file contains the following fields:
- `snapshot_version`: the version number of the snapshot file.
- `entities`: a map / dictionary of the entities that are part of the snapshot. The key is the name of the entity and the value is of the type SnapshotEntity.

The `SnapshotEntity` message contains the following fields:
- `deleted_in_versions`: a list of versions at which the entity has been deleted.
- `created_in_versions`: a list of version at which the entity has been created.
- `last_updated_in_version`: the last version at which the entity has been updated.
- `position_in_datafile`: the position of the entity in the `data.pbs` file. **This is the position of the actual entity data in the `data.pbs` file, not the delimiter.** The delimiter bytes are not required here since we already know the length since it is stored in the index file.
- `number_of_bytes`: the length of the entity in the `data.pbs` file.
- `crc32`: the crc32 hash of the entity data, this is used to determine if the entity has been updated.

```protobuf
message SnapshotIndex {
  uint64 snapshot_version_legacy = 1;
  map<string, SnapshotEntity> entities = 2;
  uint64 snapshot_version = 32;
}

message SnapshotEntity {
	repeated uint64 deleted_in_versions = 1;
	repeated uint64 created_in_versions = 2;
	uint64 last_updated_in_version = 3;
	int64 position_in_datafile = 4;
	int32 number_of_bytes = 5;
	int32 crc32 = 7;
}
```

## Hierarchical index (tree format)
The `index.pb` of a snapshot or delta stores its entities in a tree, in the `root` field, instead of in the flat
`entities` map or `*_entities` lists shown above. Entity names repeat their leading path (`t/{tenant}/s/{store}/...`) and
their fixed literals (`items`, `values`) for every entity; the tree stores each of those once.

### How a name maps onto the tree
Split the name on `/` into n segments (see `IndexKeys`):
- The **leaf** is the whole name when n <= 3, otherwise the last 2 segments when n is even, and the last 3 when n is odd.
- The leading segments are `collection/id` pairs, one tree **node** each.
- A leaf is `C/V`, `C/V/L` or (only for n = 1) `V`. It is stored under its **template** `C/{}`, `C/{}/L` or `{}`,
  and its **variable** `V`.

| Name | Nodes | Template | Variable |
|---|---|---|---|
| `t/T/s/S/items/I/values` | `t/T`, `s/S` | `items/{}/values` | `I` |
| `t/T/s/S/esls/B/pages/P/studioimage` | `t/T`, `s/S`, `esls/B` | `pages/{}/studioimage` | `P` |
| `t/T/s/S/eccparameters` | `t/T` | `s/{}/eccparameters` | `S` |
| `t/T/tenant` | (root) | `t/{}/tenant` | `T` |
| `esltypes/E` | (root) | `esltypes/{}` | `E` |

The rule needs no knowledge of id patterns, so a dataset with several patterns needs no configuration, and the fixed
parts of every pattern are stored once per node. To rebuild a name, join the node path, the template up to its `{}`,
the variable, and the rest of the template.

### Messages
Children are grouped by collection and then by id; leaves by template and then by variable:

```protobuf
message SnapshotNode {
	map<string, SnapshotChildGroup> children = 1; // by collection
	map<string, SnapshotLeafGroup> leaves = 2; // by template
}
message SnapshotChildGroup { map<string, SnapshotNode> nodes = 1; } // by id
message SnapshotLeafGroup { map<string, SnapshotEntity> entities = 1; } // by variable

message DeltaNode {
	map<string, DeltaChildGroup> children = 1;
	map<string, DeltaLeafGroup> leaves = 2;
}
message DeltaChildGroup { map<string, DeltaNode> nodes = 1; }
message DeltaLeafGroup {
	map<string, DeltaEntity> new_entities = 1;
	map<string, DeltaEntity> changed_entities = 2;
	map<string, DeletedEntity> deleted_entities = 3;
}

// SnapshotIndex: SnapshotNode root = 34;   DeltaIndex: DeltaNode root = 34;
```

In the tree, `DeltaEntity.name` and `DeletedEntity.name` are left empty: the position in the tree is the name. Field 33
is reserved: an earlier, unreleased tree format used it.

### Compatibility and rollout
- Readers accept both formats, for snapshots and deltas, and a file holding both. Flat files are never rewritten;
  old deltas age out through cleanup, and the next snapshot is written in the tree format.
- Writers write only the tree format.
- A process from before the tree format parses a tree-format file without error, but sees it as empty.

**Every process that opens the dataset must run the tree-aware version before the first tree-format write.** That is
not only readers and writers, but also every host of a `SnapshotCreationPlugin`, `StorageCleaningService`,
`DatasetUpdateReportingPlugin` or `DatasetWatcher`. Until then, an old process silently loses data:
- An old `SnapshotCreationPlugin` builds its snapshot from an index in which the tree deltas are empty. That snapshot
  lacks every entity created since the upgrade, and has the older version of every entity changed or deleted since; readers
  that start from it see that state. Once cleaning removes the deltas behind it (with `CleanupConfig.KeepAllDeltas` set
  to `false`), the lost writes are gone for good.
- An old `DatasetUpdateReportingPlugin` reports nothing for a tree delta and still stores it as processed, so those
  changes are never reported.
- An old reader or `DatasetWatcher` misses the entities in tree deltas.

So update the processes that only read first, and the writers last, in a stop-then-start deployment rather than a
rolling one, so no old writer replica (with its snapshot plugin) runs next to a new one. Snapshot creation has no
configuration switch: it runs wherever `WithSnapshot()` is called. Cleaning does: set `CleanupConfig.IsEnabled` to
`false` (in whichever configuration section the host binds it from) for the mixed-version window, so the older
snapshots and the deltas behind a bad snapshot are kept, and that snapshot can still be removed and rebuilt from them.

### Possible later refinement
Templates and variables are opaque strings to everything except `IndexKeys`. Registered id patterns could map a name
onto the tree differently later (for example to also share a literal that the pair rule takes as a variable, like
`studio` in `t/T/s/S/esls/B/link/studio`) without changing these messages.

## Aliases
An alias is another name for an entity, under an alias name (like `by_storeesl`), computed from its data by the alias
definitions the writer is registered with. An alias value is unique within its alias name; one entity may own many.
The writer rejects a write that would give an alias two live owners, so the stored state never has two.

Aliases are stored in the index files, as diffs in deltas and as full sets in snapshots:

```protobuf
message AliasRef { string name = 1; string value = 2; } // alias name, alias value

message SnapshotEntity { ...; repeated AliasRef aliases = 8; }         // the full set the entity owns
message DeltaEntity    { ...; repeated AliasRef added_aliases = 9;
                              repeated AliasRef removed_aliases = 10; }
message DeletedEntity  { ...; repeated AliasRef removed_aliases = 5; } // the aliases the delete released

// An alias change without a data change, by the full entity name (written by an alias definitions rebuild)
message AliasOnlyChange {
	string name = 1;
	repeated AliasRef added_aliases = 2;
	repeated AliasRef removed_aliases = 3;
}

message DeltaIndex    { ...; repeated AliasOnlyChange alias_changes = 7; string alias_definitions_hash = 8; }
message SnapshotIndex { ...; string alias_definitions_hash = 3; }
```

- A delta stores each entity's net change over the whole write. A reader gets an entity's set by applying the deltas
  in order on top of the snapshot's set, and builds the reverse map (alias → owner, per alias name) from the sets, so
  no entity data is read for aliases.
- A deleted entity owns no aliases: whatever it owned is released, also when `removed_aliases` is empty.
- `alias_definitions_hash` is written on every delta, and copied into a snapshot from the delta it is made at. A
  dataset without alias definitions has an empty hash. When a writer starts with definitions whose hash differs from
  the latest one, it reads the data of all entities once, recomputes their aliases, and writes one delta with an
  `alias_changes` entry for every entity whose set changed, and the new hash. If it crashes before that delta is
  stored, the next start does it again.

## DeltaInfo
- `DeltaInfo.pb`: Storing the metadata about the delta files that are available.

It conatins the following fields:
- `earliest_delta_version`: the version number of the earliest delta file that is still available.
- `latest_delta_version`: the version number of the latest delta file that has been created.
```protobuf
message DeltaInfo {
	uint64 earliest_delta_version = 1;
	uint64 latest_delta_version = 2;
}
```

This file can be used to determine which delta files are available. Using this file is highly recommended compared to listing all files since this can be a very time-consuming/expensive operation.
## SnapshotInfo
- `SnapshotInfo.pb`: Storing the metadata about the snapshot files that are available.

It conatins the following fields:
- `snapshot_versions`: This is a list of snapshot versions that are available. For each snapshot version, the version number and the timestamp at which the snapshot was created are stored.

```protobuf
message SnapshotInfo {
	repeated snapshotVersion snapshot_versions = 1; 
	message snapshotVersion {
		uint64 version = 1;
		google.protobuf.Timestamp timestamp = 2;
	}
}
```

This file can be used to determine which snapshot files are available. Using this file is highly recommended compared to listing all files since this can be a very time-consuming/expensive operation.

## ProcessedVersions
- `ProcessedVersions.pb`: Storing the metadata about the services that process delta notifications.

It conatins the following fields:
- `processes_versions`: This is a map/dictionary that stores the delta version up to where the data has been processed by a service. The key is the name of the service and the value is a delta version number.

```protobuf
message ProcessedVersionsInfo {
	map<string, uint64> processed_versions = 1;
}

```

This file can be used to determine which files have been processed by a service. This can be used to determine which files need to be processed by a service and which files can be skipped.


# Reading the protobuf files using the index
Assuming you want to get a list of all the entities that currently exist in the dataset, you can follow these steps:
1. Read the `DeltaInfo.pb` and `SnapshotInfo.pb` file to determine which delta and snapshot files are available.
2. Read the snapshot `index.pb` file of the latest snapshot.
3. Read the remaining deltas `index.pb` starting from the latest snapshot version up to the latest delta version.
4. (Optional) Extra index data sanity validation can be done by checking if both "snapshot_version" & "snapshot_version_legacy" are not 0 and one of them must be the version you are trying to load
 
For each entity in the snapshot and delta files, you can read the actual data of the entity from the `data.pbs` file of the same delta/snapshot.
To efficiently read the data.pbs file, you can use the `position_in_file` and `number_of_bytes` fields of the entity in the `index.pb` file to determine the position and length of the entity in the `data.pbs` file, allowing you to read only the required data.
The resulting byte array is also in protobuf format and can then be deserialized into the actual entity object using the proto-generated code of the DTO.
Note that the actual entities are stored in a delimited format in the `data.pbs` file, however, since the number of bytes is already known in case you are using the index file, the `position_in_datafile` in the index points to the start of the actual entity data, ignoring the delimiter bytes.

# Reading the protobuf files without using the index

If you want to read the `data.pbs` file without using the `index.pb` file, you can still do this by reading the `data.pbs` file directly.
In this case, you probably still want to use the `DeltaInfo.pb` and `SnapshotInfo.pb` file to determine which delta and snapshot files are available. Listing all deltas and snapshots can be a very time-consuming/expensive operation, especially when there are many files.

1. Read the `DeltaInfo.pb` and `SnapshotInfo.pb` file to determine which delta and snapshot files are available.
2. Read / stream the `data.pbs` file of the latest snapshot.
3. The first bytes of the stream will contain the length delimiter (number of bytes) of the first entity, this value is encoded as a [varint](https://protobuf.dev/programming-guides/encoding/#varints), and therefore is not of a constant size.
Using this length delimiter, you can read the next n bytes of the stream, where n is the length of the entity.
For most languages, the protobuf library will have a method (usually called `parseDelimitedFrom()`) to read from a length delimited stream of protobuf messages, automatically handling the varint encoding and reading and deserializing the next n bytes.
4. Repeat step 3 until the end of the stream is reached.
5. Repeat steps 2 and 3 for the delta files, starting from the latest snapshot version up to the latest delta version.

# Last remarks
The actual structure of the entities in the `data.pbs` file depends on the DTO that is being stored.
The structure of the DTO is currently defined in the proto file and the actual entities are serialized and deserialized using the generated code from the proto file.
In theory, the structure of the entities doesn't have to use protobuf, however, by design, this is the current implementation.