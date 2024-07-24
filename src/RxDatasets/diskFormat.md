# Introduction
For the storage of DTOs, the Protobuf encoding is used. Protobuf is a binary file format that is used to serialize and deserialize objects. 
Three different object types are used:
- Deltas: for each update made to the dataset, a delta is created and stored, containing the changes made to the dataset.
- Snapshots: these files contain the full state of the data at a certain point in time.
- VersionInfo: this file contains metadata about the delta and snapshot files that are available.

In total, there are five different file types stored on disk:
- `deltas/{delta_version}/data.pbs`: Storing the actual (concatinated) data of the entities that have been changed or added.
- `deltas/{delta_version}/index.pb`: Storing the metadata of entities in the delta, and their position in the data file.
- `snapshot/{snapshot_version}/data.pbs`: Storing the actual (concatenated) data of the entities that are part of the snapshot.
- `snapshot/{snapshot_version}/index.pb`: Storing the metadata of entities in the snapshot, and their position in the data file.
- `VersionInfo.pb`: Storing the metadata about the delta and snapshot files that are available.

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
	uint64 delta_version = 1; 
	repeated string deleted_entity_names = 2; 
	repeated DeltaEntity updated_entities = 3;
	repeated DeltaEntity created_entities = 4;
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
	uint64 snapshot_version = 1; 
	map<string, SnapshotEntity> entities = 2;
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

## VersionInfo
The `VersionIndex.pb` file contains metadata about the delta and snapshots files that are available.

It conatins the following fields:
- `earliest_delta_version`: the version number of the earliest delta file that is still available.
- `latest_delta_version`: the version number of the latest delta file that has been created.
- `snapshot_versions`: This is a list of snapshot versions that are available. For each snapshot version, the version number and the timestamp at which the snapshot was created are stored.
- `processes_versions`: This is a map/dictionary that stores the delta version up to where the data has been processed by a service. The key is the name of the service and the value is a delta version number.

```protobuf
message VersionInfo {
	uint64 latest_delta_version = 1;
	repeated snapshotVersion snapshot_versions = 2; 
	uint64 earliest_delta_version = 3;
	map<string, uint64> processed_versions = 4;

	message snapshotVersion {
		uint64 version = 1;
		google.protobuf.Timestamp timestamp = 2;
	}
}
```

This file can be used to determine which delta and snapshot files are available and which files have been processed by a service. This can be used to determine which files need to be processed by a service and which files can be skipped. Using this file is highly recommended compared to listing all files since this can be a very time-consuming/expensive operation.

# Reading the protobuf files using the index
Assuming you want to get a list of all the entities that currently exist in the dataset, you can follow these steps:
1. Read the `VersionInfo.pb` file to determine which delta and snapshot files are available.
2. Read the snapshot `index.pb` file of the latest snapshot.
3. Read the remaining deltas `index.pb` starting from the latest snapshot version up to the latest delta version.

For each entity in the snapshot and delta files, you can read the actual data of the entity from the `data.pbs` file of the same delta/snapshot.
To efficiently read the data.pbs file, you can use the `position_in_file` and `number_of_bytes` fields of the entity in the `index.pb` file to determine the position and length of the entity in the `data.pbs` file, allowing you to read only the required data.
The resulting byte array is also in protobuf format and can then be deserialized into the actual entity object using the proto-generated code of the DTO.
Note that the actual entities are stored in a delimited format in the `data.pbs` file, however, since the number of bytes is already known in case you are using the index file, the `position_in_datafile` in the index points to the start of the actual entity data, ignoring the delimiter bytes.

# Reading the protobuf files without using the index

If you want to read the `data.pbs` file without using the `index.pb` file, you can still do this by reading the `data.pbs` file directly.
In this case, you probably still want to use the `VersionInfo.pb` file to determine which delta and snapshot files are available. Listing all deltas and snapshots can be a very time-consuming/expensive operation, especially when there are many files.

1. Read the `VersionInfo.pb` file to determine which delta and snapshot files are available.
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